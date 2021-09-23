#include "postgres.h"

#include <math.h>
#include "access/tupdesc.h"
#include "access/htup_details.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/datetime.h"
#include "utils/dynahash.h"
#include "utils/timestamp.h"
#include "pgsk_aggregated_stats.h"

static char *worker_name = "pgsk_worker";

static char *extension_name = "pg_stat_kcache";

static HTAB *string_to_id = NULL;

static HTAB *id_to_string = NULL;

static GlobalInfo *global_variables = NULL;

static volatile sig_atomic_t got_sigterm = false;

static void get_pgskKeysArray_by_query(char*, pgskKeysArray*, bool*);

static int get_index_of_comment_key(char*);

static int get_id_from_string(char*);

static Datum pgsk_internal_get_stats_time_interval(TimestampTz, TimestampTz, FunctionCallInfo);

void pgsk_add(void*, void*);

void pgsk_on_delete(void*, void*);

static int
get_random_int(void)
{
    int x;

    x = (int) (random() & 0xFFFF) << 16;
    x |= (int) (random() & 0xFFFF);
    return x;
}

static void
pg_stat_kcache_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

static bool
is_key_excluded(char *key) {
    int i;
    for (i = 0; i < global_variables->excluded_keys_count; ++i) {
        if (strcmp(key, global_variables->excluded_keys[i]) == 0)
            return true;
    }
    return false;
}

static int
get_index_of_comment_key(char *key) {
    int i;
    if (global_variables == NULL) {
        return comment_key_not_specified;
    }

    if (is_key_excluded(key))
        return comment_key_not_specified;

    for (i = 0; i < global_variables->keys_count; ++i) {
        if (strcmp(key, global_variables->commentKeys[i]) == 0)
            return i;
    }
    if (global_variables->keys_count == max_parameters_count) {
        elog(WARNING, "pgsk: Can not store more than %d parameters", max_parameters_count);
        return comment_key_not_specified;
    }
    strcpy(global_variables->commentKeys[global_variables->keys_count++], key);
    return global_variables->keys_count - 1;
}

static void
get_pgskKeysArray_by_query(char *query, pgskKeysArray *result, bool *is_comment_exist) {
    char query_copy[(max_parameter_length + 2) * max_parameters_count * 2];
    char *end_of_comment;
    char *start_of_comment;
    int comment_length;
    int len;
    int i;
    char end_of_key[2] = ":\0";
    int key_index = -1;
    char *query_prefix;
    int value_id;

    start_of_comment = strstr(query, "/*");
    end_of_comment = strstr(query, "*/");

    *is_comment_exist = false;
    if (start_of_comment == NULL || end_of_comment == NULL) {
        result = NULL;
        return;
    }

    start_of_comment += 3;
    comment_length = end_of_comment - start_of_comment;
    memset(&query_copy, '\0', sizeof(query_copy));
    strlcpy(query_copy, start_of_comment, comment_length + 1);
    query_copy[comment_length] = '\0';
    query_prefix = strtok(query_copy, " ");

    for (i = 0; i < max_parameters_count; ++i) {
        result->keyValues[i] = comment_value_not_specified;
    }
    while (query_prefix != NULL) {
        len = strlen(query_prefix);
        if (strcmp(query_prefix + len - 1, end_of_key) == 0 && key_index == -1) {
            /* it's key */
            query_prefix[len - 1] = '\0';
            key_index = get_index_of_comment_key(query_prefix);
        } else {
            /* it's value */
            if (key_index == comment_key_not_specified) {
                query_prefix = strtok(NULL, " ");
                continue;
            }
            value_id = get_id_from_string(query_prefix);
            if (value_id == -1) {
                return;
            }
            *is_comment_exist = true;
            result->keyValues[key_index] = value_id;
            key_index = -1;
        }

        query_prefix = strtok(NULL, " ");
    }
    if (key_index != -1)
        elog(WARNING, "pg_stat_kcache: incorrect comment");
}

static uint64_t
pgsk_find_optimal_items_count(uint64_t left_bound,
                              uint64_t right_bound,
                              uint64_t* string_to_id_htab_item,
                              uint64_t* id_to_string_htab_item,
                              uint64_t* total_size) {
    uint64_t string_to_id_htab_size;
    uint64_t id_to_string_htab_size;
    uint64_t middle;

    middle = (right_bound + left_bound) / 2;
    string_to_id_htab_size = hash_estimate_size(middle, *string_to_id_htab_item);
    id_to_string_htab_size = hash_estimate_size(middle, *id_to_string_htab_item);

    if (left_bound + 1 == right_bound) {
        return left_bound;
    }

    if (string_to_id_htab_size + id_to_string_htab_size > *total_size) {
        return pgsk_find_optimal_items_count(left_bound,
                                             middle,
                                             string_to_id_htab_item,
                                             id_to_string_htab_item,
                                             total_size);
    } else {
        return pgsk_find_optimal_items_count(middle,
                                             right_bound,
                                             string_to_id_htab_item,
                                             id_to_string_htab_item,
                                             total_size);
    }
}

static uint64_t
pgsk_find_optimal_memory_split(uint64_t left_bound,
                               uint64_t right_bound,
                               uint64_t* string_to_id_htab_item,
                               uint64_t* id_to_string_htab_item,
                               uint64_t* total_size,
                               int* global_var_const_size,
                               int* pgsk_items) {
    int pgsk_max_items_count;
    uint64_t middle;
    uint64_t pgsk_size;
    int pgtb_items;

    middle = (right_bound + left_bound) / 2;
    pgsk_size = *total_size - middle;
    pgsk_max_items_count = pgsk_size / (*string_to_id_htab_item + *id_to_string_htab_item);

    pgtb_items = pgtb_get_items_count(middle,
                         sizeof(pgskBucketItem),
                         sizeof(pgskCounters));

    *pgsk_items = pgsk_find_optimal_items_count(0,
                                               pgsk_max_items_count,
                                               string_to_id_htab_item,
                                               id_to_string_htab_item,
                                               &pgsk_size);

    if (left_bound + 1 == right_bound) {
        return left_bound;
    }

    if (pgtb_items < *pgsk_items) {
        return pgsk_find_optimal_memory_split(middle,
                                              right_bound,
                                              string_to_id_htab_item,
                                              id_to_string_htab_item,
                                              total_size,
                                              global_var_const_size,
                                              pgsk_items);
    } else {
        return pgsk_find_optimal_memory_split(left_bound,
                                              middle,
                                              string_to_id_htab_item,
                                              id_to_string_htab_item,
                                              total_size,
                                              global_var_const_size,
                                              pgsk_items);
    }
}

void
pgsk_store_aggregated_counters(pgskCounters* counters, QueryDesc* queryDesc) {
    pgskBucketItem key;
    pgskCounters additional_counters;
    bool is_comment_exist;
    char query[(max_parameter_length + 1) * max_parameters_count];

    if (global_variables == NULL) {
        return;
    }

    memset(&key.keys, 0, sizeof(pgskKeysArray));
    strlcpy(query, queryDesc->sourceText, sizeof(query));
    query[sizeof(query) - 1] = '\0';
    is_comment_exist = true;
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    get_pgskKeysArray_by_query(query, &key.keys, &is_comment_exist);
    if (!is_comment_exist) {
        LWLockRelease(&global_variables->lock);
        return;
    }
    key.database = MyDatabaseId;
    key.user = GetUserId();

    additional_counters.usage = 1;
    additional_counters.utime = counters->utime;
    additional_counters.stime = counters->stime;
    additional_counters.in_network = strlen(queryDesc->sourceText);
#if PG_VERSION_NUM >= 110000
    if (queryDesc->totaltime != NULL) {
        additional_counters.out_network = TupleDescSize(queryDesc->tupDesc) * queryDesc->totaltime->ntuples;
    }
#endif

#ifdef HAVE_GETRUSAGE
    additional_counters.minflts = counters->minflts;
    additional_counters.majflts = counters->majflts;
    additional_counters.nswaps = counters->nswaps;
    additional_counters.reads = counters->reads;
    additional_counters.writes = counters->writes;
    additional_counters.msgsnds = counters->msgsnds;
    additional_counters.msgrcvs = counters->msgrcvs;
    additional_counters.nsignals = counters->nsignals;
    additional_counters.nvcsws = counters->nvcsws;
    additional_counters.nivcsws = counters->nivcsws;
#endif
    pgtb_put(extension_name, &key, &additional_counters);
    LWLockRelease(&global_variables->lock);
}

void pgsk_add(void* value, void* anotherValue) {
    pgskCounters* counters;
    pgskCounters* another_counters;

    counters = (pgskCounters*) value;
    another_counters = (pgskCounters*) anotherValue;

    counters->usage += another_counters->usage;
    counters->utime += another_counters->utime;
    counters->stime += another_counters->stime;
    counters->in_network += another_counters->in_network;
    counters->out_network += another_counters->out_network;
    counters->minflts += another_counters->minflts;
    counters->majflts += another_counters->majflts;
    counters->nswaps += another_counters->nswaps;
    counters->reads += another_counters->reads;
    counters->writes += another_counters->writes;
    counters->msgsnds += another_counters->msgsnds;
    counters->msgrcvs += another_counters->msgrcvs;
    counters->nsignals += another_counters->nsignals;
    counters->nvcsws += another_counters->nvcsws;
    counters->nivcsws += another_counters->nivcsws;
}

void
pgsk_define_custom_shmem_vars(HASHCTL info, int _buffer_size_mb, int _stat_time_interval, char* excluded_keys) {
    bool                found_global_info;
    bool                found;
    int                 id;
    int                 items_count;
    pgskStringFromId    *stringFromId;
    uint64_t            total_shmem;
    uint64_t            pgtb_size;
    int                 global_var_const_size;
    uint64_t            string_to_id_htab_item;
    uint64_t            id_to_string_htab_item;
    char                excluded_keys_copy[max_parameters_count * max_parameter_length];
    char*               excluded_key;

    global_var_const_size = sizeof(GlobalInfo);
    string_to_id_htab_item = (sizeof(char) * max_parameter_length + sizeof(pgskIdFromString));
    id_to_string_htab_item = (sizeof(int) + sizeof(pgskStringFromId));

    total_shmem = _buffer_size_mb * 1024 * 1024;
    pgtb_size = pgsk_find_optimal_memory_split(0,
                                               total_shmem,
                                               &string_to_id_htab_item,
                                               &id_to_string_htab_item,
                                               &total_shmem,
                                               &global_var_const_size,
                                               &items_count);

    pgtb_init(extension_name,
              &pgsk_add,
              &pgsk_on_delete,
              (int)(_stat_time_interval / buckets_count + 0.5),
              pgtb_size,
              sizeof(pgskBucketItem),
              sizeof(pgskCounters));

    global_variables = ShmemInitStruct("pg_stat_kcache global_variables",
                                       sizeof(GlobalInfo),
                                       &found_global_info);

    global_variables->bucket_duration = (int)(_stat_time_interval / buckets_count + 0.5);
    global_variables->items_count = items_count;
    elog(LOG, "pgsk: Max count of unique strings: %d", global_variables->items_count);

    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(char) * max_parameter_length;
    info.entrysize = sizeof(pgskIdFromString);
    string_to_id = ShmemInitHash("pg_stat_kcache string_to_id_htab",
                                 items_count, items_count,
                                 &info,
                                 HASH_ELEM | HASH_BLOBS);

    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int);
    info.entrysize = sizeof(pgskStringFromId);

    id_to_string = ShmemInitHash("pg_stat_kcache id_to_string_htab",
                                 items_count, items_count,
                                 &info,
                                 HASH_ELEM | HASH_BLOBS);

    id = comment_value_not_specified;
    stringFromId = hash_search(id_to_string, (void *) &id, HASH_ENTER, &found);
    global_variables->currents_strings_count = 1;
    stringFromId->id = id;
    memset(stringFromId->string, '\0', sizeof(stringFromId->string));

    memset(&global_variables->excluded_keys, '\0', sizeof(global_variables->excluded_keys));
    global_variables->excluded_keys_count = 0;

    if (excluded_keys == NULL) {
        return;
    }
    /* make sure that variable not too long */
    memset(&excluded_keys_copy, '\0', sizeof(excluded_keys_copy));
    strlcpy(&excluded_keys_copy[0], excluded_keys, max_parameters_count * max_parameter_length);
    excluded_key = strtok(excluded_keys_copy, ",");
    while (excluded_key != NULL) {
        strlcpy(&global_variables->excluded_keys[global_variables->excluded_keys_count][0], excluded_key, max_parameter_length - 1);
        global_variables->excluded_keys_count += 1;

        excluded_key = strtok(NULL, " ");
    }
}

void
pgsk_register_bgworker() {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_PostmasterStart;
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
    sprintf(worker.bgw_library_name, "pg_stat_kcache");
    sprintf(worker.bgw_function_name, "pg_stat_kcache_main");
    /* Wait 10 seconds for restart after crash */
    worker.bgw_restart_time = 10;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;
    RegisterBackgroundWorker(&worker);
}

static int
get_id_from_string(char *string_pointer) {
    char string[max_parameter_length];
    pgskIdFromString *idFromString;
    pgskStringFromId *stringFromId;
    int id;
    bool found;
    if (!string_to_id || !id_to_string)
        return comment_value_not_specified;
    if (strlen(string_pointer) >= max_parameter_length) {
        elog(WARNING, "pg stat kcache: Comment value %s too long to store. Max length is %d",
             string_pointer,
             max_parameter_length);
        return comment_value_not_specified;
    }
    memset(&string, '\0', max_parameter_length);
    strcpy(string, string_pointer);
    idFromString = hash_search(string_to_id, (void *) &string, HASH_FIND, &found);
    if (!found) {
        found = true;
        /* Generate id that not used yet. */
        while (found) {
            id = get_random_int();
            stringFromId = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
        }

        if (global_variables->currents_strings_count < global_variables->items_count) {
            stringFromId = hash_search(id_to_string, (void *) &id, HASH_ENTER, &found);
            global_variables->currents_strings_count += 1;
            stringFromId->id = id;
            memset(stringFromId->string, '\0', max_parameter_length);
            strcpy(stringFromId->string, string);
            stringFromId->counter = 0;

            idFromString = hash_search(string_to_id, (void *) &string, HASH_ENTER, &found);
            memset(idFromString->string, '\0', max_parameter_length);
            strcpy(idFromString->string, string);
            idFromString->id = id;
        } else {
            if (!global_variables->max_strings_count_achieved) {
                elog(WARNING,
                     "pgsk: Can't handle request. No more memory for save strings are available. "
                     "Current max count of unique strings = %d."
                     "Decide to tune pg_stat_kcache.buffer_size", global_variables->items_count);
            }
            global_variables->max_strings_count_achieved = true;
            global_variables->strings_overflow_by += 1;
            return -1;
        }
    }
    stringFromId = hash_search(id_to_string, (void *) &idFromString->id, HASH_FIND, &found);
    stringFromId->counter++;

    return idFromString->id;
}

static void
get_string_from_id(int id, char* string) {
    pgskStringFromId *stringFromId;
    bool found;
    if (!string_to_id || !id_to_string) {
        return;
    }
    stringFromId = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
    strlcpy(string, stringFromId->string, max_parameter_length);
}

void pgsk_on_delete(void* key, void* value) {
    pgskBucketItem* item;
    pgskCounters* counters;
    pgskStringFromId *string_struct;
    int id;
    int i;
    bool found;

    if (global_variables == NULL)
        return;

    item = (pgskBucketItem*) key;
    counters = (pgskCounters*) value;

    for (i = 0; i < global_variables->keys_count; ++i) {
        id = item->keys.keyValues[i];
        if (id == comment_value_not_specified) {
            continue;
        }
        string_struct = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
        string_struct->counter -= (int)(counters->usage + 0.5);
        if (string_struct->counter == 0) {
            hash_search(id_to_string, (void *) &id, HASH_REMOVE, &found);
            hash_search(string_to_id, (void *) string_struct->string, HASH_REMOVE, &found);
            global_variables->currents_strings_count -= 1;
        }
    }
}

static void
pgsk_init() {
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    pgtb_reset_stats(extension_name);
    memset(&global_variables->commentKeys, '\0', sizeof(global_variables->commentKeys));
    global_variables->max_strings_count_achieved = false;
    global_variables->keys_count = 0;
    global_variables->strings_overflow_by = 0;
    LWLockRelease(&global_variables->lock);
}

static void
pgsk_update_info() {
    if (global_variables == NULL) {
        return;
    }
    if (global_variables->max_strings_count_achieved) {
        elog(WARNING, "pg_stat_kcache: Too many unique strings. Overflow by %d (%f%%)",
             global_variables->strings_overflow_by, (double)global_variables->strings_overflow_by / global_variables->items_count);
    }
    global_variables->strings_overflow_by = 0;
    global_variables->max_strings_count_achieved = false;
}

void
pg_stat_kcache_main(Datum main_arg) {
    TimestampTz timestamp;
    int64 wait_microsec;
    /* Register functions for SIGTERM management */
    pqsignal(SIGTERM, pg_stat_kcache_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();
    LWLockInitialize(&global_variables->lock, LWLockNewTrancheId());

    pgsk_init();
    wait_microsec = global_variables->bucket_duration * 1e6;
    while (!got_sigterm) {
        int rc;

        /* Wait necessary amount of time */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, wait_microsec / 1000, PG_WAIT_EXTENSION);
        /* Emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);
        /* Process signals */
        timestamp = GetCurrentTimestamp();
        ResetLatch(&MyProc->procLatch);
        if (got_sigterm) {
            /* Simply exit */
            elog(DEBUG1, "bgworker pg_stat_kcache signal: processed SIGTERM");
            proc_exit(0);
        }
        /* Main work happens here */
        LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
        pgsk_update_info();
        pgtb_tick(extension_name);
        LWLockRelease(&global_variables->lock);
        wait_microsec = (int64) global_variables->bucket_duration * 1e6 - (GetCurrentTimestamp() - timestamp);
        if (wait_microsec < 0)
            wait_microsec = 0;
    }

    /* No problems, so clean exit */
    proc_exit(0);
}

static char *escape_json_string(const char *str) {
    size_t str_len = strlen(str);
    size_t pstr_len = 2 * (str_len + 1);
    int i, j = 0;
    char *pstr = palloc(pstr_len);
    for (i = 0; i < str_len; i++) {
        char ch = str[i];
        switch (ch) {
            case '\\':
                pstr[j++] = '\\';
                pstr[j++] = '\\';
                break;
            case '"':
                pstr[j++] = '\\';
                pstr[j++] = '"';
                break;
            case '\n':
                pstr[j++] = '\\';
                pstr[j++] = 'n';
                break;
            case '\r':
                pstr[j++] = '\\';
                pstr[j++] = 'r';
                break;
            case '\t':
                pstr[j++] = '\\';
                pstr[j++] = 't';
                break;
            case '\b':
                pstr[j++] = '\\';
                pstr[j++] = 'b';
                break;
            case '\f':
                pstr[j++] = '\\';
                pstr[j++] = 'f';
                break;
            default:
                pstr[j++] = ch;
        }
        if (j == pstr_len) break;
    }
    pstr[j++] = '\0';
    return pstr;
}


PG_FUNCTION_INFO_V1(pgsk_get_stats);
PG_FUNCTION_INFO_V1(pgsk_get_stats_time_interval);

Datum
get_jsonb_datum_from_key(pgskKeysArray *keys) {
    StringInfoData strbuf;
    int i;
    char component[max_parameter_length];
    char *escaped_component;
    const char* const_component;
    bool some_value_stored;
    int keys_count;
    char commentKeys[max_parameters_count][max_parameter_length];

    some_value_stored = false;
    initStringInfo(&strbuf);
    appendStringInfoChar(&strbuf, '{');

    memcpy(&commentKeys, &global_variables->commentKeys, sizeof(commentKeys));
    keys_count = global_variables->keys_count;

    for (i = 0; i < keys_count; ++i) {
        if (keys->keyValues[i] == comment_value_not_specified) {
            continue;
        }
        memset(&component, '\0', sizeof(component));
        if (i > 0 && some_value_stored)
            appendStringInfoChar(&strbuf, ',');
        get_string_from_id(keys->keyValues[i], (char*)&component);
        const_component = component;
        escaped_component = escape_json_string(const_component);

        appendStringInfo(&strbuf, "\"%s\":\"%s\"", (char*)&commentKeys[i], escaped_component);
        some_value_stored = true;
        pfree(escaped_component);
    }
    appendStringInfoChar(&strbuf, '}');
    appendStringInfoChar(&strbuf, '\0');

    return DirectFunctionCall1(jsonb_in, CStringGetDatum(strbuf.data));
}

Datum
pgsk_get_stats(PG_FUNCTION_ARGS) {

    TimestampTz timestamp_left;
    TimestampTz timestamp_right;

    timestamp_right = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), global_variables->bucket_duration * 1000);
    timestamp_left = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), global_variables->bucket_duration * (- buckets_count * 10));

    return pgsk_internal_get_stats_time_interval(timestamp_left, timestamp_right, fcinfo);
}

Datum
pgsk_get_stats_time_interval(PG_FUNCTION_ARGS) {
    TimestampTz timestamp_left;
    TimestampTz timestamp_right;

    timestamp_left = PG_GETARG_TIMESTAMP(0);
    timestamp_right = PG_GETARG_TIMESTAMP(1);

    return pgsk_internal_get_stats_time_interval(timestamp_left, timestamp_right, fcinfo);
}

static Datum
pgsk_internal_get_stats_time_interval(TimestampTz timestamp_left, TimestampTz timestamp_right, FunctionCallInfo fcinfo) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    int message_id;
    pgskBucketItem key;
    pgskCounters value;
    uint64 reads, writes;
    char timestamp_left_s[max_parameter_length];
    char timestamp_right_s[max_parameter_length];

    int items_count;
    void* result_ptr;
    int length;
    int i;

    Datum values[PG_STAT_KCACHE_COLS + 3];
    bool nulls[PG_STAT_KCACHE_COLS + 3];

    /* Shmem structs not ready yet */
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_stat_kcache must be loaded via shared_preload_libraries")));
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("return type must be a row type")));
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, 0, sizeof(nulls));
    MemSet(&key, 0, sizeof(pgskBucketItem));

    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);

    items_count = global_variables->items_count;
    result_ptr = (void*) palloc(items_count * (sizeof(pgskBucketItem) + sizeof(pgskCounters)));
    memset(result_ptr, 0, items_count * (sizeof(pgskBucketItem) + sizeof(pgskCounters)));
    pgtb_get_stats_time_interval(extension_name, &timestamp_left, &timestamp_right, result_ptr, &length);

    strcpy(timestamp_left_s, timestamptz_to_str(timestamp_left));
    strcpy(timestamp_right_s, timestamptz_to_str(timestamp_right));

    elog(NOTICE, "pgsk: Show stats from '%s' to '%s'", timestamp_left_s, timestamp_right_s);

    /* put to tuplestore and clear bucket index -1 */
    for (message_id = 0; message_id < length; ++message_id) {
        memcpy(&key,
               (char*)result_ptr + (sizeof(pgskBucketItem) + sizeof(pgskCounters)) * message_id,
               sizeof(pgskBucketItem));
        memcpy(&value,
               (char*)result_ptr + (sizeof(pgskBucketItem) + sizeof(pgskCounters)) * message_id + sizeof(pgskBucketItem),
               sizeof(pgskCounters));

        MemSet(nulls, false, sizeof(nulls));
        i = 0;
        values[i++] = get_jsonb_datum_from_key(&key.keys);

        values[i++] = Int64GetDatum(value.usage);
        values[i++] = ObjectIdGetDatum(key.user);
        values[i++] = ObjectIdGetDatum(key.database);

#ifdef HAVE_GETRUSAGE
        reads = value.reads * RUSAGE_BLOCK_SIZE;
        writes = value.writes * RUSAGE_BLOCK_SIZE;
        values[i++] = Int64GetDatumFast(reads);
        values[i++] = Int64GetDatumFast(writes);
#else
        nulls[i++] = true; /* reads */
        nulls[i++] = true; /* writes */
#endif
        values[i++] = Float8GetDatumFast(value.utime);
        values[i++] = Float8GetDatumFast(value.stime);

#ifdef HAVE_GETRUSAGE
        values[i++] = Int64GetDatumFast(value.minflts);
        values[i++] = Int64GetDatumFast(value.majflts);
        values[i++] = Int64GetDatumFast(value.nswaps);
        values[i++] = Int64GetDatumFast(value.msgsnds);
        values[i++] = Int64GetDatumFast(value.msgrcvs);
        values[i++] = Int64GetDatumFast(value.nsignals);
        values[i++] = Int64GetDatumFast(value.nvcsws);
        values[i++] = Int64GetDatumFast(value.nivcsws);

#else
        nulls[i++] = true; /* minflts */
        nulls[i++] = true; /* majflts */
        nulls[i++] = true; /* nswaps */
        nulls[i++] = true; /* msgsnds */
        nulls[i++] = true; /* msgrcvs */
        nulls[i++] = true; /* nsignals */
        nulls[i++] = true; /* nvcsws */
        nulls[i++] = true; /* nivcsws */
#endif
        values[i++] = Int64GetDatumFast(value.in_network);

#if PG_VERSION_NUM >= 110000
        values[i++] = Int64GetDatumFast(value.out_network);
#else
        nulls[i++] = true;
#endif
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
    pfree(result_ptr);
    LWLockRelease(&global_variables->lock);
    /* return the tuplestore */
    tuplestore_donestoring(tupstore);
    return (Datum) 0;
}

PG_FUNCTION_INFO_V1(pgsk_reset_stats);

Datum
pgsk_reset_stats(PG_FUNCTION_ARGS) {
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_stat_kcache must be loaded via shared_preload_libraries")));

    pgsk_init();

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pgsk_exclude_key);

Datum
pgsk_exclude_key(PG_FUNCTION_ARGS) {
    char key_copy[max_parameter_length];
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_stat_kcache must be loaded via shared_preload_libraries")));

    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    if (global_variables->excluded_keys_count == max_parameters_count) {
        elog(WARNING, "pgsk: Can't exclude more than %d keys. You can drop existing keys by 'pgsk_reset_excluded_keys'",
             max_parameters_count);
    }
    else {
        memset(&key_copy, 0, sizeof(key_copy));
        strlcpy((char*)&key_copy, PG_GETARG_CSTRING(0), sizeof(key_copy) - 1);
        if (is_key_excluded(&key_copy)) {
            elog(WARNING, "pgsk: Key already excluded.");
        } else {
            strlcpy(&global_variables->excluded_keys[global_variables->excluded_keys_count][0], PG_GETARG_CSTRING(0), max_parameter_length - 1);
            global_variables->excluded_keys_count++;
        }
    }
    LWLockRelease(&global_variables->lock);

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pgsk_get_excluded_keys);

Datum
pgsk_get_excluded_keys(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    Datum values[1];
    bool nulls[1];
    int i;

    /* Shmem structs not ready yet */
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_stat_kcache must be loaded via shared_preload_libraries")));
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_SCALAR)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("return type must be a scalar type")));
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;

    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, 0, sizeof(nulls));
#if PG_VERSION_NUM >= 120000
    tupdesc = CreateTemplateTupleDesc(1);
#else
    tupdesc = CreateTemplateTupleDesc(1, false);
#endif
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "excluded_key",
                       TEXTOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(&global_variables->lock, LW_SHARED);
    for (i = 0; i < global_variables->excluded_keys_count; ++i) {
        values[0] = CStringGetTextDatum(global_variables->excluded_keys[i]);
        nulls[0] = false;
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    LWLockRelease(&global_variables->lock);

    tuplestore_donestoring(tupstore);
    return (Datum) 0;
}

PG_FUNCTION_INFO_V1(pgsk_get_buffer_stats);

Datum
pgsk_get_buffer_stats(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    Datum values[BUFFER_STATS_COUNT];
    bool nulls[BUFFER_STATS_COUNT];

    /* Shmem structs not ready yet */
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_stat_kcache must be loaded via shared_preload_libraries")));
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("return type must be a row type")));
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;

    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    MemSet(values, 0, sizeof(values));
    MemSet(nulls, 0, sizeof(nulls));
#if PG_VERSION_NUM >= 120000
    tupdesc = CreateTemplateTupleDesc(BUFFER_STATS_COUNT);
#else
    tupdesc = CreateTemplateTupleDesc(BUFFER_STATS_COUNT, false);
#endif
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "saved_strings_count",
                       INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "available_strings_count",
                       INT4OID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    LWLockAcquire(&global_variables->lock, LW_SHARED);
    values[0] = Int32GetDatum(global_variables->currents_strings_count);
    values[1] = Int32GetDatum(global_variables->items_count);

    LWLockRelease(&global_variables->lock);
    tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    tuplestore_donestoring(tupstore);
    return (Datum) 0;
}

PG_FUNCTION_INFO_V1(pgsk_reset_excluded_keys);

Datum
pgsk_reset_excluded_keys(PG_FUNCTION_ARGS) {
    if (!global_variables)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("pg_stat_kcache must be loaded via shared_preload_libraries")));

    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    memset(&global_variables->excluded_keys, '\0', sizeof(global_variables->excluded_keys));
    global_variables->excluded_keys_count = 0;
    LWLockRelease(&global_variables->lock);
    PG_RETURN_VOID();
}
