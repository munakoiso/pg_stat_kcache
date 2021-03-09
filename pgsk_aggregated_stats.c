#include "postgres.h"

#include <math.h>
#include "access/tupdesc.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"
#include "pgsk_aggregated_stats.h"

static char *worker_name = "pgsk_worker";

static HTAB *counters_htab = NULL;

static HTAB *string_to_id = NULL;

static HTAB *id_to_string = NULL;

static GlobalInfo *global_variables = NULL;

static volatile sig_atomic_t got_sigterm = false;

static void get_pgskKeysArray_by_query(char*, pgskKeysArray*, bool*);

static int get_index_of_comment_key(char*);

static int get_id_from_string(char*);

static Datum pgsk_internal_get_stats_time_interval(TimestampTz, TimestampTz, FunctionCallInfo);

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

static int
get_index_of_comment_key(char *key) {
    int i;
    if (global_variables == NULL) {
        return comment_key_not_specified;
    }

    for (i = 0; i < global_variables->keys_count; ++i) {
        if (strcmp(key, global_variables->commentKeys[i]) == 0)
            return i;
    }
    strcpy(global_variables->commentKeys[global_variables->keys_count++], key);
    return global_variables->keys_count - 1;
}

static void
get_pgskKeysArray_by_query(char *query, pgskKeysArray *result, bool *is_comment_exist) {
    char query_copy[(max_parameter_length + 1) * max_parameters_count * 2];
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

    if (start_of_comment == NULL || end_of_comment == NULL) {
        result = NULL;
        *is_comment_exist = false;
        return;
    }

    start_of_comment += 3;
    comment_length = end_of_comment - start_of_comment;
    memset(&query_copy, '\0', sizeof(query_copy));
    strlcpy(query_copy, start_of_comment, comment_length + 1);
    query_copy[comment_length] = '\0';
    query_prefix = strtok(query_copy, " ");

    for (i = 0; i < max_parameters_count; ++i) {
        result->keyValues[i] = comment_key_not_specified;
    }
    while (query_prefix != NULL) {
        len = strlen(query_prefix);
        if (strcmp(query_prefix + len - 1, end_of_key) == 0 && key_index == -1) {
            /* it's key */
            key_index = get_index_of_comment_key(query_prefix);
        } else {
            /* it's value */

            value_id = get_id_from_string(query_prefix);
            if (value_id == -1) {
                *is_comment_exist = false;
                return;
            }
            result->keyValues[key_index] = value_id;
            key_index = -1;
        }

        query_prefix = strtok(NULL, " ");
    }
    if (key_index != -1)
        elog(WARNING, "pg_stat_kcache: incorrect comment");
}

void
pgsk_store_aggregated_counters(pgskCounters* counters, QueryDesc* queryDesc) {
    bool found;
    pgskCountersHtabValue *elem;
    pgskCountersHtabKey key;
    pgskKeysArray *keysArray;
    bool is_comment_exist;
    char query[(max_parameter_length + 1) * max_parameters_count];
    int index;

    keysArray = &key.info.keys;

    if (global_variables == NULL) {
        return;
    }
    memset(keysArray, 0, sizeof(key.info.keys));
    strlcpy(query, queryDesc->sourceText, sizeof(query));
    query[sizeof(query) - 1] = '\0';
    is_comment_exist = true;
    get_pgskKeysArray_by_query(query, keysArray, &is_comment_exist);
    if (!is_comment_exist) {
        return;
    }
    memcpy(&key.info.keys, keysArray, sizeof(pgskKeysArray));
    key.info.database = MyDatabaseId;
    key.info.user = GetUserId();
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);

    key.bucket = global_variables->bucket;
    elem = hash_search(counters_htab, (void *) &key, HASH_FIND, &found);
    if (!found) {
        global_variables->required_max_strings_count += 1;
        if (global_variables->bucket_fullness[key.bucket] == global_variables->max_strings_count) {
            elog(LOG, "pgsk: Bucket %d is full. That case not solved yet, skipping...", global_variables->bucket);
            elog(LOG, "pgsk: Required max strings count %d", global_variables->required_max_strings_count);
            LWLockRelease(&global_variables->lock);
            return;
        }
        elem = hash_search(counters_htab, (void *) &key, HASH_ENTER, &found);
        index = global_variables->bucket * global_variables->max_strings_count + global_variables->bucket_fullness[key.bucket];
        memcpy(&global_variables->buckets[index], &elem->key.info, sizeof(pgskBucketItem));
        global_variables->bucket_fullness[key.bucket] += 1;
        memcpy(&elem->key, &key, sizeof(pgskCountersHtabKey));
        memset(&elem->counters, 0, sizeof(pgskCounters));
    }

    elem->counters.usage = elem->counters.usage + 1;
    elem->counters.utime += counters->utime;
    elem->counters.stime += counters->stime;
    elem->counters.in_network += strlen(queryDesc->sourceText);
#if PG_VERSION_NUM >= 110000
    elem->counters.out_network += TupleDescSize(queryDesc->tupDesc) * queryDesc->totaltime->ntuples;
#endif

#ifdef HAVE_GETRUSAGE
    elem->counters.minflts += counters->minflts;
    elem->counters.majflts += counters->majflts;
    elem->counters.nswaps += counters->nswaps;
    elem->counters.reads += counters->reads;
    elem->counters.writes += counters->writes;
    elem->counters.msgsnds += counters->msgsnds;
    elem->counters.msgrcvs += counters->msgrcvs;
    elem->counters.nsignals += counters->nsignals;
    elem->counters.nvcsws += counters->nvcsws;
    elem->counters.nivcsws += counters->nivcsws;
#endif
    LWLockRelease(&global_variables->lock);
}

void
pgsk_define_custom_shmem_vars(HASHCTL info, int _buffer_size_mb, int _stat_time_interval) {
    bool                found_global_info;
    bool                found;
    int                 id;
    int                 bucket_size;
    int                 max_strings_count;
    pgskStringFromId    *stringFromId;
    int64               buffer_size;
    int global_var_const_size;
    int counters_htab_size;
    int string_to_id_htab_size;
    int id_to_string_htab_size;

    global_var_const_size = sizeof(GlobalInfo);
    counters_htab_size = (sizeof(pgskCountersHtabKey) + sizeof(pgskCountersHtabValue)) * (actual_buckets_count + 1);
    string_to_id_htab_size = (sizeof(char) * max_parameter_length + sizeof(pgskIdFromString)) * max_parameters_count;
    id_to_string_htab_size = (sizeof(int) + sizeof(pgskStringFromId)) * max_parameters_count;

    buffer_size = _buffer_size_mb * 1e6;
    bucket_size = (buffer_size - global_var_const_size) /
                                    (counters_htab_size + string_to_id_htab_size + id_to_string_htab_size);


    global_variables = ShmemInitStruct("pg_stat_kcache global_variables",
                                       sizeof(GlobalInfo) + sizeof(pgskBucketItem) * actual_buckets_count * bucket_size,
                                       &found_global_info);

    global_variables->max_strings_count = bucket_size;
    global_variables->bucket_duration = _stat_time_interval / buckets_count;
    elog(LOG, "pgsk: Max count of unique strings: %d", global_variables->max_strings_count);

    max_strings_count = global_variables->max_strings_count;

    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(pgskCountersHtabKey);
    info.entrysize = sizeof(pgskCountersHtabValue);
    counters_htab = ShmemInitHash("pg_stat_kcache counters_htab",
                                  bucket_size * (actual_buckets_count + 1), bucket_size * (actual_buckets_count + 1),
                                  &info,
                                  HASH_ELEM | HASH_BLOBS);

    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(char) * max_parameter_length;
    info.entrysize = sizeof(pgskIdFromString);
    string_to_id = ShmemInitHash("pg_stat_kcache string_to_id_htab",
                                 max_strings_count, max_strings_count,
                                 &info,
                                 HASH_ELEM | HASH_BLOBS);

    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int);
    info.entrysize = sizeof(pgskStringFromId);

    id_to_string = ShmemInitHash("pg_stat_kcache id_to_string_htab",
                                 max_strings_count, max_strings_count,
                                 &info,
                                 HASH_ELEM | HASH_BLOBS);

    id = comment_key_not_specified;
    stringFromId = hash_search(id_to_string, (void *) &id, HASH_ENTER, &found);
    global_variables->currents_strings_count = 1;
    stringFromId->id = id;
    memset(stringFromId->string, '\0', sizeof(stringFromId->string));
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
        return comment_key_not_specified;
    memset(&string, '\0', max_parameter_length);
    strcpy(string, string_pointer);
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    idFromString = hash_search(string_to_id, (void *) &string, HASH_FIND, &found);
    if (!found) {
        found = true;
        /* Generate id that not used yet. */
        while (found) {
            id = get_random_int();
            stringFromId = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
        }

        if (global_variables->currents_strings_count < global_variables->max_strings_count) {
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
            elog(WARNING,
                 "pgsk: Can't handle request. No more memory for save strings are available. "
                 "Current max count of unique strings = %d."
                 "Decide to tune pg_stat_kcache.buffer_size", global_variables->max_strings_count);
            LWLockRelease(&global_variables->lock);
            return -1;
        }
    }
    stringFromId = hash_search(id_to_string, (void *) &idFromString->id, HASH_FIND, &found);
    stringFromId->counter++;

    LWLockRelease(&global_variables->lock);
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
    strcpy(string, stringFromId->string);
}

static void
pop_key(int bucket, int key_in_bucket) {
    bool found;
    int i;
    int id;
    /* pgskIdFromString* id_pointer; */
    char str_key[max_parameter_length];
    pgskStringFromId *string_struct;
    pgskCountersHtabValue *elem;
    pgskCountersHtabKey key;
    int index;

    if (global_variables == NULL)
        return;
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);

    if (key_in_bucket >= global_variables->bucket_fullness[bucket]) {
        LWLockRelease(&global_variables->lock);
        return;
    }
    index = bucket * global_variables->max_strings_count + key_in_bucket;
    memcpy(&key.info, &global_variables->buckets[index], sizeof(key.info));
    elem = hash_search(counters_htab, (void *) &key, HASH_FIND, &found);
    if (found) {
        elem->counters.usage -= 1;
        for (i = 0; i < global_variables->keys_count; ++i) {
            index = bucket * global_variables->max_strings_count + key_in_bucket;
            id = global_variables->buckets[index].keys.keyValues[i];
            string_struct = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
            string_struct->counter -= 1;
            if (string_struct->counter == 0 && id != comment_key_not_specified) {
                hash_search(id_to_string, (void *) &id, HASH_REMOVE, &found);
                hash_search(string_to_id, (void *) &str_key, HASH_REMOVE, &found);
                global_variables->currents_strings_count -= 1;
            }
        }

        if (fabs(elem->counters.usage) < 1e-5) {
            elem = hash_search(counters_htab, (void *) &key, HASH_REMOVE, &found);
        }
    }
    LWLockRelease(&global_variables->lock);
}

static void
pgsk_init(bool explicit_reset) {
    int bucket;
    int key_in_bucket;

    /* Wait for all locks (in case of manual reset some locks can be acquired) */
    if (!explicit_reset) {
        LWLockInitialize(&global_variables->lock, LWLockNewTrancheId());
    }
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    memset(&global_variables->commentKeys, '\0', sizeof(global_variables->commentKeys));
    global_variables->keys_count = 0;
    global_variables->bucket = 0;
    global_variables->required_max_strings_count = 0;
    if (!explicit_reset) {
        memset(&global_variables->buckets, 0, sizeof(pgskBucketItem) * actual_buckets_count * global_variables->max_strings_count);
        memset(&global_variables->bucket_fullness, 0, sizeof(global_variables->bucket_fullness));
    }
    LWLockRelease(&global_variables->lock);
    for (bucket = 0; bucket < actual_buckets_count; ++bucket) {
        for (key_in_bucket = 0; key_in_bucket < global_variables->max_strings_count; ++key_in_bucket) {
            pop_key(bucket, key_in_bucket);
        }
    }

    if (explicit_reset) {
        LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);

        memset(&global_variables->buckets, 0, sizeof(pgskBucketItem) * actual_buckets_count * global_variables->max_strings_count);
        memset(&global_variables->bucket_fullness, 0, sizeof(global_variables->bucket_fullness));

        LWLockRelease(&global_variables->lock);
    }
    global_variables->init_timestamp = GetCurrentTimestamp();
}

static void
pgsk_update_info() {
    int next_bucket;
    int key_in_bucket;
    int64 stat_interval_ms;
    if (global_variables == NULL) {
        return;
    }
    LWLockAcquire(&global_variables->lock, LW_SHARED);
    next_bucket = (global_variables->bucket + 1) % actual_buckets_count;
    LWLockRelease(&global_variables->lock);

    for (key_in_bucket = 0; key_in_bucket < global_variables->max_strings_count; ++key_in_bucket) {
        pop_key(next_bucket, key_in_bucket);
    }
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    stat_interval_ms = ((int64)global_variables->bucket_duration) * actual_buckets_count * 1e3;
    global_variables->bucket_fullness[next_bucket] = 0;
    global_variables->bucket = next_bucket;
    global_variables->required_max_strings_count = 0;
    global_variables->last_update_timestamp = GetCurrentTimestamp();
    if (global_variables->init_timestamp + stat_interval_ms < global_variables->last_update_timestamp)
        global_variables->init_timestamp = global_variables->last_update_timestamp - stat_interval_ms;
    LWLockRelease(&global_variables->lock);
}

void
pg_stat_kcache_main(Datum main_arg) {
    /* Register functions for SIGTERM management */
    pqsignal(SIGTERM, pg_stat_kcache_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    pgsk_init(false);
    while (!got_sigterm) {
        int rc;
        /* Wait necessary amount of time */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, global_variables->bucket_duration * 1000, PG_WAIT_EXTENSION);

        ResetLatch(&MyProc->procLatch);
        /* Emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);
        /* Process signals */

        if (got_sigterm) {
            /* Simply exit */
            elog(DEBUG1, "bgworker pg_stat_kcache signal: processed SIGTERM");
            proc_exit(0);
        }
        /* Main work happens here */
        pgsk_update_info();
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

    some_value_stored = false;
    initStringInfo(&strbuf);
    appendStringInfoChar(&strbuf, '{');
    for (i = 0; i < global_variables->keys_count; ++i) {
        if (keys->keyValues[i] == comment_key_not_specified) {
            continue;
        }
        memset(&component, '\0', sizeof(component));
        if (i > 0 && some_value_stored)
            appendStringInfoChar(&strbuf, ',');
        get_string_from_id(keys->keyValues[i], (char*)&component);
        const_component = component;
        escaped_component = escape_json_string(const_component);
        appendStringInfo(&strbuf, "\"%s\":\"%s\"", (char*)&global_variables->commentKeys[i], escaped_component);
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
    timestamp_left = global_variables->init_timestamp;

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

static TimestampTz
_pgsk_normalize_ts(TimestampTz ts) {
    long sec_diff;
    int msec_diff;
    TimestampTz now;
    now = GetCurrentTimestamp();
    if (global_variables->init_timestamp > ts)
        ts = global_variables->init_timestamp;
    TimestampDifference(ts, global_variables->last_update_timestamp, &sec_diff, &msec_diff);
    /* if ts < oldest stat we have */
    if (sec_diff > buckets_count * global_variables->bucket_duration) {
        ts = TimestampTzPlusMilliseconds(global_variables->last_update_timestamp, - (int64) buckets_count * global_variables->bucket_duration * 1e3);
    }
    if (ts > now) {
        ts = now;
    }

    return ts;
}

static Datum
pgsk_internal_get_stats_time_interval(TimestampTz timestamp_left, TimestampTz timestamp_right, FunctionCallInfo fcinfo) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    int bucket_index;
    int bucket_index_0;
    int delta_bucket;
    int key_index;
    int i;
    int bucket_fullness;
    pgskCountersHtabKey key;
    pgskCountersHtabValue *tmp;
    pgskCountersHtabValue *elem;
    bool found;
    char key_name[max_parameter_length];
    uint64 reads, writes;
    int index;
    TimestampTz norm_left_ts;
    TimestampTz norm_right_ts;
    int bucket_left;
    int bucket_right;
    int bucket_interval;
    int64 bucket_time_since_init;
    char timestamp_left_s[max_parameter_length];
    char timestamp_right_s[max_parameter_length];
    long sec_diff;
    int msec_diff;
    int current_bucket;

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
    MemSet(&key, 0, sizeof(pgskCountersHtabKey));
    MemSet(key_name, 0, sizeof(key_name));

    /* aggregate in bucket_ind = -1 all stats */
    LWLockAcquire(&global_variables->lock, LW_SHARED);

    strcpy(timestamp_left_s, timestamptz_to_str(timestamp_left));
    strcpy(timestamp_right_s, timestamptz_to_str(timestamp_right));

    norm_left_ts = _pgsk_normalize_ts(timestamp_left);
    TimestampDifference(global_variables->init_timestamp, norm_left_ts, &sec_diff, &msec_diff);
    bucket_time_since_init = sec_diff / global_variables->bucket_duration;
    norm_left_ts = TimestampTzPlusMilliseconds(global_variables->init_timestamp, bucket_time_since_init * global_variables->bucket_duration * 1e3);
    bucket_left = bucket_time_since_init % actual_buckets_count;

    norm_right_ts = _pgsk_normalize_ts(timestamp_right);
    TimestampDifference(global_variables->init_timestamp, norm_right_ts, &sec_diff, &msec_diff);
    bucket_time_since_init = sec_diff / global_variables->bucket_duration + 1;
    norm_right_ts = TimestampTzPlusMilliseconds(global_variables->init_timestamp, bucket_time_since_init * global_variables->bucket_duration * 1e3);

    if (norm_right_ts > GetCurrentTimestamp())
        norm_right_ts = GetCurrentTimestamp();

    bucket_right = bucket_time_since_init % actual_buckets_count;
    if (bucket_right > global_variables->bucket ||
            (global_variables->bucket == actual_buckets_count - 1 && bucket_right == 0))
        bucket_right = global_variables->bucket;

    strcpy(timestamp_left_s, timestamptz_to_str(norm_left_ts));
    strcpy(timestamp_right_s, timestamptz_to_str(norm_right_ts));
    elog(NOTICE, "pgsk: Show stats from '%s' to '%s'", timestamp_left_s, timestamp_right_s);

    bucket_interval = bucket_right - bucket_left + 1;
    bucket_index_0 = bucket_left;
    bucket_fullness = global_variables->bucket_fullness[global_variables->bucket];
    current_bucket = global_variables->bucket;
    LWLockRelease(&global_variables->lock);

    for (delta_bucket = 0; delta_bucket < bucket_interval; ++delta_bucket) {
        bucket_index = (bucket_index_0 + delta_bucket) % actual_buckets_count;
        for (key_index = 0; key_index < global_variables->max_strings_count; ++key_index) {
            LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
            if ((bucket_index == current_bucket && key_index == bucket_fullness) ||
                (bucket_index < current_bucket && key_index == global_variables->bucket_fullness[bucket_index])) {
                LWLockRelease(&global_variables->lock);
                break;
            }

            index = bucket_index * global_variables->max_strings_count + key_index;
            memcpy(&key.info, &global_variables->buckets[index], sizeof(pgskBucketItem));
            key.bucket = bucket_index;

            elem = hash_search(counters_htab, (void *) &key, HASH_FIND, &found);

            key.bucket = -1;

            tmp = hash_search(counters_htab, (void *) &key, HASH_FIND, &found);
            if (!found) {
                tmp = hash_search(counters_htab, (void *) &key, HASH_ENTER, &found);
                memcpy(tmp, elem, sizeof(pgskCountersHtabValue));
                tmp->key.bucket = -1;
            } else {
                tmp->counters.usage += elem->counters.usage;
                tmp->counters.utime += elem->counters.utime;
                tmp->counters.stime += elem->counters.stime;
                tmp->counters.out_network += elem->counters.out_network;
                tmp->counters.in_network += elem->counters.in_network;
#ifdef HAVE_GETRUSAGE
                tmp->counters.minflts += elem->counters.minflts;
                tmp->counters.majflts += elem->counters.majflts;
                tmp->counters.nswaps += elem->counters.nswaps;
                tmp->counters.reads += elem->counters.reads;
                tmp->counters.writes += elem->counters.writes;
                tmp->counters.msgsnds += elem->counters.msgsnds;
                tmp->counters.msgrcvs += elem->counters.msgrcvs;
                tmp->counters.nsignals += elem->counters.nsignals;
                tmp->counters.nvcsws += elem->counters.nvcsws;
                tmp->counters.nivcsws += elem->counters.nivcsws;
#endif

            }
            LWLockRelease(&global_variables->lock);
        }
    }

    /* put to tuplestore and clear bucket index -1 */
    for (delta_bucket = 0; delta_bucket < bucket_interval; ++delta_bucket) {
        bucket_index = (bucket_index_0 + delta_bucket) % actual_buckets_count;
        for (key_index = 0; key_index < global_variables->max_strings_count; ++key_index) {
            LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
            if ((bucket_index == current_bucket && key_index == bucket_fullness) ||
                (bucket_index < current_bucket && key_index == global_variables->bucket_fullness[bucket_index])) {
                LWLockRelease(&global_variables->lock);
                break;
            }
            index = bucket_index * global_variables->max_strings_count + key_index;
            memcpy(&key.info, &global_variables->buckets[index], sizeof(pgskBucketItem));
            key.bucket = -1;

            elem = hash_search(counters_htab, (void *) &key, HASH_FIND, &found);
            if (found) {
                i = 0;
                MemSet(nulls, false, sizeof(nulls));

                values[i++] = get_jsonb_datum_from_key(&key.info.keys);
                /* put to tuplestore, then remove hash */
                values[i++] = Int64GetDatum(elem->counters.usage);
                values[i++] = ObjectIdGetDatum(elem->key.info.user);
                values[i++] = ObjectIdGetDatum(elem->key.info.database);

#ifdef HAVE_GETRUSAGE
                reads = elem->counters.reads * RUSAGE_BLOCK_SIZE;
                writes = elem->counters.writes * RUSAGE_BLOCK_SIZE;
                values[i++] = Int64GetDatumFast(reads);
                values[i++] = Int64GetDatumFast(writes);
#else
                nulls[i++] = true; /* reads */
                nulls[i++] = true; /* writes */
#endif

                values[i++] = Float8GetDatumFast(elem->counters.utime);
                values[i++] = Float8GetDatumFast(elem->counters.stime);

#ifdef HAVE_GETRUSAGE
                values[i++] = Int64GetDatumFast(elem->counters.minflts);
                values[i++] = Int64GetDatumFast(elem->counters.majflts);
                values[i++] = Int64GetDatumFast(elem->counters.nswaps);
                values[i++] = Int64GetDatumFast(elem->counters.msgsnds);
                values[i++] = Int64GetDatumFast(elem->counters.msgrcvs);
                values[i++] = Int64GetDatumFast(elem->counters.nsignals);
                values[i++] = Int64GetDatumFast(elem->counters.nvcsws);
                values[i++] = Int64GetDatumFast(elem->counters.nivcsws);

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
                values[i++] = Int64GetDatumFast(elem->counters.in_network);

#if PG_VERSION_NUM >= 110000
                values[i++] = Int64GetDatumFast(elem->counters.out_network);
#else
                nulls[i++] = true;
#endif
                tuplestore_putvalues(tupstore, tupdesc, values, nulls);
            }
            elem = hash_search(counters_htab, (void *) &key, HASH_REMOVE, &found);
            LWLockRelease(&global_variables->lock);

        }
    }

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

    pgsk_init(true);

    PG_RETURN_VOID();
}
