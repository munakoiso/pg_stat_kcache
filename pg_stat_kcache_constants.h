#define max_buffer_size_mb 5000
#define min_buffer_size_mb 1
#define max_time_interval 360000
#define min_time_interval 600
#define buckets_count 100
#define actual_buckets_count 105
#define max_parameter_length 256
#define max_parameters_count 5
#define comment_key_not_specified 0

#define RUSAGE_BLOCK_SIZE	512			/* Size of a block for getrusage() */
#define PG_STAT_KCACHE_COLS_V2_0	7
#define PG_STAT_KCACHE_COLS_V2_1	17
#define PG_STAT_KCACHE_COLS			17	/* maximum of above */

#define USAGE_INCREASE			(1.0)
#define USAGE_DECREASE_FACTOR	(0.99)	/* decreased every pgsk_entry_dealloc */
#define USAGE_DEALLOC_PERCENT	5		/* free this % of entries at once */
#define USAGE_INIT				(1.0)	/* including initial planning */
