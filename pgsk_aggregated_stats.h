#include <math.h>
#include "access/tupdesc.h"
#include "utils/jsonb.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/datetime.h"

#include "pg_stat_kcache_constants.h"

static char *worker_name = "pgsk_worker";

static HTAB *counters_htab = NULL;

static HTAB *string_to_id = NULL;

static HTAB *id_to_string = NULL;

/*
 * Current getrusage counters.
 *
 * For platform without getrusage support, we rely on postgres implementation
 * defined in rusagestub.h, which only supports user and system time.
*/
typedef struct pgskCounters
{
    double			usage;		/* usage factor */
    /* These fields are always used */
    float8			utime;		/* CPU user time */
    float8			stime;		/* CPU system time */
    int64 in_network;
    int64 out_network;
#ifdef HAVE_GETRUSAGE
    /* These fields are only used for platform with HAVE_GETRUSAGE defined */
	int64			minflts;	/* page reclaims (soft page faults) */
	int64			majflts;	/* page faults (hard page faults) */
	int64			nswaps;		/* page faults (hard page faults) */
	int64			reads;		/* Physical block reads */
	int64			writes;		/* Physical block writes */
	int64			msgsnds;	/* IPC messages sent */
	int64			msgrcvs;	/* IPC messages received */
	int64			nsignals;	/* signals received */
	int64			nvcsws;		/* voluntary context witches */
	int64			nivcsws;	/* unvoluntary context witches */
#endif
} pgskCounters;

typedef struct pgskStringFromId {
    int id;
    char string[max_parameter_length];
    int counter;
} pgskStringFromId;

typedef struct pgskIdFromString {
    char string[max_parameter_length];
    int id;
} pgskIdFromString;

typedef struct pgskKeysArray {
    int keyValues[max_parameters_count];
} pgskKeysArray;

typedef struct pgskBucketItem {
    pgskKeysArray keys;
    Oid database;
    Oid user;
} pgskBucketItem;

void pg_stat_kcache_main(Datum)pg_attribute_noreturn();
Datum get_jsonb_datum_from_key(pgskKeysArray *);

typedef struct pgskCountersHtabKey {
    pgskBucketItem info;
    int bucket;
} pgskCountersHtabKey;

typedef struct pgskCountersHtabValue {
    pgskCountersHtabKey key;
    pgskCounters counters; /* statistics for this query */
} pgskCountersHtabValue;

typedef struct global_info {
    int bucket;
    char commentKeys[max_parameters_count][max_parameter_length];
    int bucket_fullness[actual_buckets_count];
    int keys_count;
    int currents_strings_count;
    LWLock lock;
    pgskBucketItem buckets[FLEXIBLE_ARRAY_MEMBER];
} GlobalInfo;

static GlobalInfo *global_variables = NULL;

static volatile sig_atomic_t got_sigterm = false;

static void get_pgskKeysArray_by_query(char*, pgskKeysArray*, bool*);

static int get_index_of_comment_key(char*);

static int get_id_from_string(char*);

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
            // it's key
            key_index = get_index_of_comment_key(query_prefix);
        } else {
            // it's value

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

static void
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
        if (global_variables->bucket_fullness[key.bucket] == bucket_size) {
            elog(LOG, "pgsk: Bucket %d is full. That case not solved yet, skipping...", global_variables->bucket);
            return;
        }
        elem = hash_search(counters_htab, (void *) &key, HASH_ENTER, &found);
        index = global_variables->bucket * bucket_size + global_variables->bucket_fullness[key.bucket];
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

static void
define_custom_shmem_vars(HASHCTL info) {
    bool                found_global_info;
    bool                found;
    int                 id;
    pgskStringFromId    *stringFromId;

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

    global_variables = ShmemInitStruct("pg_stat_kcache global_variables",
                                       sizeof(GlobalInfo) + sizeof(pgskBucketItem) * actual_buckets_count * bucket_size,
                                       &found_global_info);

    id = comment_key_not_specified;
    stringFromId = hash_search(id_to_string, (void *) &id, HASH_ENTER, &found);
    global_variables->currents_strings_count = 1;
    stringFromId->id = id;
    memset(stringFromId->string, '\0', sizeof(stringFromId->string));
}

static void
calculate_max_strings_count() {
    int counters_htab_size;
    int string_to_id_htab_size;
    int id_to_string_htab_size;
    int global_var_const_size;

    global_var_const_size = sizeof(GlobalInfo);
    counters_htab_size = (sizeof(pgskCountersHtabKey) + sizeof(pgskCountersHtabValue)) * (actual_buckets_count + 1);
    string_to_id_htab_size = (sizeof(char) * max_parameter_length + sizeof(pgskIdFromString)) * max_parameters_count;
    id_to_string_htab_size = (sizeof(int) + sizeof(pgskStringFromId)) * max_parameters_count;

    buffer_size = buffer_size_mb * 1e6;
    bucket_size = (buffer_size - global_var_const_size) /
                  (counters_htab_size + string_to_id_htab_size + id_to_string_htab_size);
    max_strings_count = bucket_size;
    bucket_duration = stat_time_interval / buckets_count;
    elog(LOG, "pgsk: Max count of unique strings: %d", max_strings_count);
}

static void
register_bgworker() {
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
        // Generate id that not used yet.
        while (found) {
            id = get_random_int();
            stringFromId = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
        }

        if (global_variables->currents_strings_count < max_strings_count) {
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
                 "Decide to tune pg_stat_kcache.buffer_size");
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
    // pgskIdFromString* id_pointer;
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
    index = bucket * bucket_size + key_in_bucket;
    memcpy(&key.info, &global_variables->buckets[index], sizeof(key.info));
    elem = hash_search(counters_htab, (void *) &key, HASH_FIND, &found);
    if (found) {
        elem->counters.usage -= 1;
        for (i = 0; i < global_variables->keys_count; ++i) {
            index = bucket * bucket_size + key_in_bucket;
            id = global_variables->buckets[index].keys.keyValues[i];
            string_struct = hash_search(id_to_string, (void *) &id, HASH_FIND, &found);
            string_struct->counter -= 1;
            if (string_struct->counter == 0 && id != comment_key_not_specified) {
                hash_search(id_to_string, (void *) &id, HASH_REMOVE, &found);
                hash_search(string_to_id, (void *) &str_key, HASH_REMOVE, &found);
                global_variables->currents_strings_count -= 1;
            }
        }

        if (fabs(elem->counters.usage) < EPS) {
            elem = hash_search(counters_htab, (void *) &key, HASH_REMOVE, &found);
        }
    }
    LWLockRelease(&global_variables->lock);
}

static void
pgsk_init(bool explicit_reset) {
    int bucket;
    int key_in_bucket;

    // Wait for all locks (in case of manual reset some locks can be acquired)
    if (!explicit_reset) {
        LWLockInitialize(&global_variables->lock, LWLockNewTrancheId());
    }
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    memset(&global_variables->commentKeys, '\0', sizeof(global_variables->commentKeys));
    global_variables->keys_count = 0;
    global_variables->bucket = 0;
    if (!explicit_reset) {
        memset(&global_variables->buckets, 0, sizeof(pgskBucketItem) * actual_buckets_count * bucket_size);
        memset(&global_variables->bucket_fullness, 0, sizeof(global_variables->bucket_fullness));
    }
    LWLockRelease(&global_variables->lock);
    for (bucket = 0; bucket < actual_buckets_count; ++bucket) {
        for (key_in_bucket = 0; key_in_bucket < bucket_size; ++key_in_bucket) {
            pop_key(bucket, key_in_bucket);
        }
    }

    if (explicit_reset) {
        LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);

        memset(&global_variables->buckets, 0, sizeof(pgskBucketItem) * actual_buckets_count * bucket_size);
        memset(&global_variables->bucket_fullness, 0, sizeof(global_variables->bucket_fullness));

        LWLockRelease(&global_variables->lock);
    }
}

static void
pgsk_update_info() {
    int next_bucket;
    int key_in_bucket;
    if (global_variables == NULL) {
        return;
    }
    LWLockAcquire(&global_variables->lock, LW_SHARED);
    next_bucket = (global_variables->bucket + 1) % actual_buckets_count;
    LWLockRelease(&global_variables->lock);

    for (key_in_bucket = 0; key_in_bucket < bucket_size; ++key_in_bucket) {
        pop_key(next_bucket, key_in_bucket);
    }
    LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
    global_variables->bucket_fullness[next_bucket] = 0;
    global_variables->bucket = next_bucket;
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
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, bucket_duration * 1000, PG_WAIT_EXTENSION);

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

    // aggregate in bucket_ind = -1 all stats
    LWLockAcquire(&global_variables->lock, LW_SHARED);
    bucket_index_0 = (global_variables->bucket - buckets_count + 1 + actual_buckets_count) % actual_buckets_count;
    bucket_fullness = global_variables->bucket_fullness[global_variables->bucket];
    LWLockRelease(&global_variables->lock);

    for (delta_bucket = 0; delta_bucket < buckets_count; ++delta_bucket) {
        bucket_index = (bucket_index_0 + delta_bucket) % actual_buckets_count;
        for (key_index = 0; key_index < bucket_size; ++key_index) {
            LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
            if ((delta_bucket == buckets_count - 1 && key_index == bucket_fullness) ||
                (delta_bucket < buckets_count - 1 && key_index == global_variables->bucket_fullness[bucket_index])) {
                LWLockRelease(&global_variables->lock);
                break;
            }

            index = bucket_index * bucket_size + key_index;
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

    // put to tuplestore and clear bucket index -1
    for (delta_bucket = 0; delta_bucket < buckets_count; ++delta_bucket) {
        bucket_index = (bucket_index_0 + delta_bucket) % actual_buckets_count;
        for (key_index = 0; key_index < bucket_size; ++key_index) {
            LWLockAcquire(&global_variables->lock, LW_EXCLUSIVE);
            if ((delta_bucket == buckets_count - 1 && key_index == bucket_fullness) ||
                (delta_bucket < buckets_count - 1 && key_index == global_variables->bucket_fullness[bucket_index])) {
                LWLockRelease(&global_variables->lock);
                break;
            }
            index = bucket_index * bucket_size + key_index;
            memcpy(&key.info, &global_variables->buckets[index], sizeof(pgskBucketItem));
            key.bucket = -1;

            elem = hash_search(counters_htab, (void *) &key, HASH_FIND, &found);
            if (found) {
                i = 0;
                MemSet(nulls, false, sizeof(nulls));

                values[i++] = get_jsonb_datum_from_key(&key.info.keys);
                // put to tuplestore, then remove hash
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
