/*----------------
 * pg_stat_ucache extension
 *
 * Gather statistics about physical disk access and CPU consumption
 * done by backends per UID.
 *
 * UID number is collected by parsing query comment in a form
 * of </><*> <uid:><number> <*></>. Comment may include additional
 * information, which is ignored by parser.
 *
 * If no UID is specified, then statement will be ignored.
 */

#include "postgres.h"

#include <unistd.h>

#include "access/hash.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "pg_stat_ucache.h"

#if PG_VERSION_NUM >= 90300
#define PGSU_DUMP_FILE "pg_stat/pg_stat_ucache.stat"
#else
#define PGSU_DUMP_FILE "global/pg_stat_ucache.stat"
#endif

#define PG_STAT_UCACHE_COLS 8
/* Size of a block for getrusage() */
#define RUSAGE_BLOCK_SIZE 512

static const uint32 PGSU_FILE_HEADER = 0x0dea6e0f;

typedef struct
{
	int64  uid;
	Oid    userid;
	Oid    dbid;
	uint32 queryid;
} pgsuHashKey;

typedef struct
{
	pgsuHashKey key;    /* hash key of entry - MUST BE FIRST */
	int64       calls;  /* # of times executed */
	int64       reads;  /* number of physical reads per database and per statement kind */
	int64       writes; /* number of physical writes per database and per statement kind */
	float8      utime;  /* CPU user time per database and per statement kind */
	float8      stime;  /* CPU system time per database and per statement kind */
	double      usage;  /* usage factor */
	slock_t     lock;   /* protects the counters only */
} pgsuEntry;

typedef struct
{
	LWLockId lock;
	double   cur_median_usage;
} pgsuSharedState;

/* Max number of queries to track */
static int pgsu_max = 0;

/* Links to shared memory state */
static pgsuSharedState *pgsu = NULL;
static HTAB *pgsu_hash = NULL;

static uint32
pgsu_hash_fn(const void *key, Size keysize)
{
	const pgsuHashKey *k = (const pgsuHashKey *) key;

	return hash_uint32(k->uid) ^
	    hash_uint32((uint32) k->userid) ^
	    hash_uint32((uint32) k->dbid) ^
	    hash_uint32((uint32) k->queryid);
}

static int
pgsu_match_fn(const void *key1, const void *key2, Size keysize)
{
	const pgsuHashKey *k1 = (const pgsuHashKey *) key1;
	const pgsuHashKey *k2 = (const pgsuHashKey *) key2;

	if (k1->uid == k2->uid &&
	    k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->queryid == k2->queryid)
		return 0;
	else
		return 1;
}

static int
pgsu_entry_cmp(const void *lhs, const void *rhs)
{
	double l_usage = (*(pgsuEntry *const *) lhs)->usage;
	double r_usage = (*(pgsuEntry *const *) rhs)->usage;

	if (l_usage < r_usage)
		return -1;
	else if (l_usage > r_usage)
		return +1;
	return 0;
}

/*
 * Deallocate least used entries.
 * Caller must hold an exclusive lock on pgsu->lock.
 */
static void
pgsu_entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgsuEntry **entries;
	pgsuEntry  *entry;
	int			nvictims;
	int			i;

	entries = palloc(hash_get_num_entries(pgsu_hash) * sizeof(pgsuEntry *));

	i = 0;
	hash_seq_init(&hash_seq, pgsu_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
		if (entry->calls == 0)
			entry->usage *= 0.50;
		else
			entry->usage *= 0.99;
	}

	qsort(entries, i, sizeof(pgsuEntry *), pgsu_entry_cmp);

	if (i > 0)
	{
		pgsu->cur_median_usage = entries[i / 2]->usage;
	}

	nvictims = Max(10, (i * 5) / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(pgsu_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}

/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on pgsu->lock
 */
static pgsuEntry *pgsu_entry_alloc(pgsuHashKey *key, bool sticky)
{
	pgsuEntry *entry;
	bool       found;

	while (hash_get_num_entries(pgsu_hash) >= pgsu_max)
		pgsu_entry_dealloc();

	entry = (pgsuEntry *) hash_search(pgsu_hash, key, HASH_ENTER, &found);

	if (!found)
	{
		/* set the appropriate initial usage count */
		entry->usage = sticky ? pgsu->cur_median_usage : 1.0;
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->lock);
	}

	entry->calls = 0;
	entry->reads = 0;
	entry->writes = 0;
	entry->utime = (0.0);
	entry->stime = (0.0);
	entry->usage = (0.0);
	return entry;
}

static void pgsu_entry_reset(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgsuEntry  *entry;

	LWLockAcquire(pgsu->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgsu_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgsu_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgsu->lock);
}

static void pgsu_recover(void)
{
	FILE      *file;
	int        i;
	uint32     header;
	int32      num;
	pgsuEntry *buffer = NULL;

	/* Load stat file, don't care about locking */
	file = AllocateFile(PGSU_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
			return;			/* ignore not-found error */
		goto error;
	}

	/* check is header is valid */
	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != PGSU_FILE_HEADER)
		goto error;

	/* get number of entries */
	if (fread(&num, sizeof(int32), 1, file) != 1)
		goto error;

	for (i = 0; i < num ; i++)
	{
		pgsuEntry  temp;
		pgsuEntry *entry;

		if (fread(&temp, sizeof(pgsuEntry), 1, file) != 1)
			goto error;

		if (temp.calls == 0)
			continue;

		entry = pgsu_entry_alloc(&temp.key, false);

		/* copy in the actual stats */
		entry->calls  = temp.calls;
		entry->reads  = temp.reads;
		entry->writes = temp.writes;
		entry->utime  = temp.utime;
		entry->stime  = temp.stime;
		entry->usage  = temp.usage;
		/* don't initialize spinlock, already done */
	}

	FreeFile(file);

	/*
	 * Remove the file so it's not included in backups/replication slaves,
	 * etc. A new file will be written on next shutdown.
	 */
	unlink(PGSU_DUMP_FILE);

	return;

error:
	ereport(LOG,
	        (errcode_for_file_access(),
	         errmsg("could not read pg_stat_kcache file \"%s\": %m",
	                PGSU_DUMP_FILE)));
	if (buffer)
		pfree(buffer);
	if (file)
		FreeFile(file);
	/* delete bogus file, don't care of errors in this case */
	unlink(PGSU_DUMP_FILE);
}

static void pgsu_dump(void)
{
	FILE	        *file;
	HASH_SEQ_STATUS  hash_seq;
	int32	         num_entries;
	pgsuEntry       *entry;

	file = AllocateFile(PGSU_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGSU_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;

	num_entries = hash_get_num_entries(pgsu_hash);

	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
		goto error;

	hash_seq_init(&hash_seq, pgsu_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (fwrite(entry, sizeof(pgsuEntry), 1, file) != 1)
		{
			/* note: we assume hash_seq_term won't change errno */
			hash_seq_term(&hash_seq);
			goto error;
		}
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file inplace
	 */
	if (rename(PGSU_DUMP_FILE ".tmp", PGSU_DUMP_FILE) != 0)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename pg_stat_kcache file \"%s\": %m",
						PGSU_DUMP_FILE ".tmp")));

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read pg_stat_kcache file \"%s\": %m",
					PGSU_DUMP_FILE)));

	if (file)
		FreeFile(file);
	unlink(PGSU_DUMP_FILE);
}

void pgsu_init(void)
{
	Size size;

	DefineCustomIntVariable("pg_stat_ucache.max",
	  "Sets the maximum number of uids tracked by pg_stat_kcache (with uid extension).",
							NULL,
							&pgsu_max,
							10000,
							100,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	EmitWarningsOnPlaceholders("pg_stat_ucache");

	size = MAXALIGN(sizeof(pgsuSharedState));
	size = add_size(size, hash_estimate_size(pgsu_max, sizeof(pgsuEntry)));
	RequestAddinShmemSpace(size);
#if PG_VERSION_NUM >= 90500
	RequestNamedLWLockTranche("pg_stat_ucache", 1);
#else
	RequestAddinLWLocks(1);
#endif

	elog(INFO, "pg_stat_ucache: loaded");
	elog(INFO, "pg_stat_ucache: pg_stat_ucache.max = %d", pgsu_max);
}

void pgsu_shmem_startup(void)
{
	HASHCTL info;
	bool found;

	/* reset in case this is a restart within the postmaster */
	pgsu = NULL;

	/* create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* global access lock */
	pgsu = ShmemInitStruct("pg_stat_ucache",
	                       sizeof(pgsuSharedState), &found);

	/* first time */
	if (!found) {
#if PG_VERSION_NUM >= 90500
		pgsu->lock = &(GetNamedLWLockTranche("pg_stat_ucache"))->lock;
#else
		pgsu->lock = LWLockAssign();
#endif
	}

	/* allocate stats shared memory hash */
	memset(&info, 0, sizeof(info));
	info.keysize   = sizeof(pgsuHashKey);
	info.entrysize = sizeof(pgsuEntry);
	info.hash      = pgsu_hash_fn;
	info.match     = pgsu_match_fn;

	pgsu_hash = ShmemInitHash("pg_stat_ucache hash",
	                          pgsu_max, pgsu_max,
	                          &info,
	                          HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	/*
	if (!IsUnderPostmaster)
		on_shmem_exit(pgsu_shmem_shutdown, (Datum) 0);
	*/

	if (! found)
		pgsu_recover();
}

void pgsu_shmem_shutdown(void)
{
	if (!pgsu)
		return;
	pgsu_dump();
}

static int64 pgsu_uidof(const char *query)
{
	int64 uid = 0;
	int is_digit = 0;
	int in_comment = 0;
	int in_uid = 0;
	int i = 0;
	for (; query[i]; i++)
	{
		/* example: sql: SomeName, uid: 45167630 */
		is_digit = 0;
		switch (query[i]) {
		case '*':
			if (i > 0 && query[i - 1] == '/')
				in_comment = 1;
			break;
		case '/':
			if (in_comment && i > 0 && query[i - 1] == '*')
				in_comment = 0;
			break;
		case ':':
			if (in_comment) {
				if (i >= 5 &&
				    query[i - 1] == 'd' &&
				    query[i - 2] == 'i' &&
				    query[i - 3] == 'u') {
					in_uid = 1;
					if (query[i + 1] == ' ')
						i++;
					continue;
				}
			}
			break;
		case '0': case '1': case '2':
		case '3': case '4': case '5':
		case '6': case '7': case '8':
		case '9':
			if (! in_uid)
				continue;
			uid = (uid * 10) + query[i] - '0';
			is_digit = 1;
			break;
		}
		if (in_uid && !is_digit)
			return uid;
	}
	return -1;
}

void pgsu_store(QueryDesc *query, int64 reads, int64 writes,
                double utime,
                double stime)
{
	pgsuHashKey         key;
	volatile pgsuEntry *e;
	pgsuEntry          *entry;
	int64               uid;

	if (!pgsu || !pgsu_hash)
		return;

	/* Collect UID from query comment */
	uid = pgsu_uidof(query->sourceText);
	if (uid == -1)
		return;

	/* Set up key for hashtable search */
	key.uid     = uid;
	key.userid  = GetUserId();
	key.dbid    = MyDatabaseId;
	key.queryid = query->plannedstmt->queryId;

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgsu->lock, LW_SHARED);

	entry = (pgsuEntry *) hash_search(pgsu_hash, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(pgsu->lock);
		LWLockAcquire(pgsu->lock, LW_EXCLUSIVE);

		/* OK to create a new hashtable entry */
		entry = pgsu_entry_alloc(&key, false);
	}

	/*
	 * Grab the spinlock while updating the counters (see comment about
	 * locking rules at the head of the file)
	 */
	e = (volatile pgsuEntry *) entry;

	SpinLockAcquire(&e->lock);

	/* "Unstick" entry if it was previously sticky */
	if (e->calls == 0)
		e->usage = 1.0;
	e->calls  += 1;
	e->reads  += reads;
	e->writes += writes;
	e->utime  += utime;
	e->stime  += stime;

	SpinLockRelease(&e->lock);

	LWLockRelease(pgsu->lock);
}

PG_FUNCTION_INFO_V1(pg_stat_ucache_reset);
PG_FUNCTION_INFO_V1(pg_stat_ucache);

Datum
pg_stat_ucache_reset(PG_FUNCTION_ARGS)
{
	if (!pgsu)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_stat_kcache must be loaded via shared_preload_libraries")));

	pgsu_entry_reset();
	PG_RETURN_VOID();
}

Datum
pg_stat_ucache(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	TupleDesc		tupdesc;
	Tuplestorestate	*tupstore;
	HASH_SEQ_STATUS hash_seq;
	pgsuEntry		*entry;


	if (!pgsu)
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
				 errmsg("materialize mode required, but it is not " \
							"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(pgsu->lock, LW_SHARED);

	hash_seq_init(&hash_seq, pgsu_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[PG_STAT_UCACHE_COLS];
		bool		nulls[PG_STAT_UCACHE_COLS];
		int64		reads, writes;
		int			i = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		SpinLockAcquire(&entry->lock);

		if ( entry->calls == 0)
		{
			SpinLockRelease(&entry->lock);
			continue;
		}
		reads = entry->reads * RUSAGE_BLOCK_SIZE;
		writes = entry->writes * RUSAGE_BLOCK_SIZE;
		values[i++] = Int64GetDatum(entry->key.queryid);
		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);
		values[i++] = Int32GetDatum(entry->key.uid);
		values[i++] = Int64GetDatumFast(reads);
		values[i++] = Int64GetDatumFast(writes);
		values[i++] = Float8GetDatumFast(entry->utime);
		values[i++] = Float8GetDatumFast(entry->stime);
		SpinLockRelease(&entry->lock);

		Assert(i == PG_STAT_UCACHE_COLS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(pgsu->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
