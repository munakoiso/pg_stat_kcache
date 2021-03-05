#ifndef PGSK_AGGREGATED_STATS
#define PGSK_AGGREGATED_STATS

#include "postgres.h"

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/jsonb.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "storage/ipc.h"
#include "utils/builtins.h"

#include "pg_stat_kcache_constants.h"

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
    int64           in_network;
    int64           out_network;
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
    int max_strings_count;
    int bucket_duration;
    LWLock lock;
    TimestampTz init_timestamp;
    TimestampTz last_update_timestamp;
    pgskBucketItem buckets[FLEXIBLE_ARRAY_MEMBER];
} GlobalInfo;

void pgsk_register_bgworker(void);
void pgsk_define_custom_shmem_vars(HASHCTL, int, int);
void pgsk_store_aggregated_counters(pgskCounters*, QueryDesc*);

#endif
