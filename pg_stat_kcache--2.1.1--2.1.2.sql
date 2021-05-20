CREATE FUNCTION pgsk_get_stats(
    OUT comment_keys    jsonb,
    OUT query_count     integer,
    OUT userid          oid,
    OUT dbid            oid,
    OUT reads           bigint,             /* total reads, in bytes */
    OUT writes          bigint,             /* total writes, in bytes */
    OUT user_time       double precision,   /* total user CPU time used */
    OUT system_time     double precision,   /* total system CPU time used */
    OUT minflts         bigint,             /* total page reclaims (soft page faults) */
    OUT majflts         bigint,             /* total page faults (hard page faults) */
    OUT nswaps          bigint,             /* total swaps */
    OUT msgsnds         bigint,             /* total IPC messages sent */
    OUT msgrcvs         bigint,             /* total IPC messages received */
    OUT nsignals        bigint,             /* total signals received */
    OUT nvcsws          bigint,             /* total voluntary context switches */
    OUT nivcsws         bigint,             /* total involuntary context switches */
    OUT in_network      bigint,
    OUT out_network     bigint
)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pgsk_get_stats'
    LANGUAGE C STRICT;

CREATE FUNCTION pgsk_get_stats_time_interval(
    start_ts            timestamptz,
    stop_ts             timestamptz,
    OUT comment_keys    jsonb,
    OUT query_count     integer,
    OUT userid          oid,
    OUT dbid            oid,
    OUT reads           bigint,             /* total reads, in bytes */
    OUT writes          bigint,             /* total writes, in bytes */
    OUT user_time       double precision,   /* total user CPU time used */
    OUT system_time     double precision,   /* total system CPU time used */
    OUT minflts         bigint,             /* total page reclaims (soft page faults) */
    OUT majflts         bigint,             /* total page faults (hard page faults) */
    OUT nswaps          bigint,             /* total swaps */
    OUT msgsnds         bigint,             /* total IPC messages sent */
    OUT msgrcvs         bigint,             /* total IPC messages received */
    OUT nsignals        bigint,             /* total signals received */
    OUT nvcsws          bigint,             /* total voluntary context switches */
    OUT nivcsws         bigint,             /* total involuntary context switches */
    OUT in_network      bigint,
    OUT out_network     bigint
)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pgsk_get_stats_time_interval'
    LANGUAGE C STRICT;

CREATE FUNCTION pgsk_reset_stats()
    RETURNS void
AS 'MODULE_PATHNAME', 'pgsk_reset_stats'
    LANGUAGE C STRICT;

CREATE FUNCTION pgsk_exclude_key(
    exclude_string  cstring
)
    RETURNS void
AS 'MODULE_PATHNAME', 'pgsk_exclude_key'
    LANGUAGE C STRICT;

CREATE FUNCTION pgsk_get_excluded_keys() RETURNS SETOF text
AS 'MODULE_PATHNAME', 'pgsk_get_excluded_keys'
    LANGUAGE C STRICT;

CREATE FUNCTION pgsk_reset_excluded_keys()
    RETURNS void
AS 'MODULE_PATHNAME', 'pgsk_reset_excluded_keys'
    LANGUAGE C STRICT;
