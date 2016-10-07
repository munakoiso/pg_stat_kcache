-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2014: Dalibo

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_stat_kernel" to load this file. \quit

SET client_encoding = 'UTF8';

CREATE FUNCTION pg_stat_kcache(OUT queryid bigint, OUT userid oid, OUT dbid oid, OUT reads bigint,
    OUT writes bigint, OUT user_time double precision, OUT system_time double precision)
    RETURNS SETOF record
    LANGUAGE c COST 1000
    AS '$libdir/pg_stat_kcache', 'pg_stat_kcache';

CREATE FUNCTION pg_stat_kcache_reset()
    RETURNS void
    LANGUAGE c COST 1000
    AS '$libdir/pg_stat_kcache', 'pg_stat_kcache_reset';

CREATE VIEW pg_stat_kcache_detail AS
SELECT s.query, d.datname, r.rolname,
       k.reads AS reads,
       k.reads/(current_setting('block_size')::integer) AS reads_blks,
       k.writes AS writes,
       k.writes/(current_setting('block_size')::integer) AS writes_blks,
       k.user_time,
       k.system_time
  FROM pg_stat_kcache() k
  JOIN pg_stat_statements s
    ON k.queryid = s.queryid AND k.dbid = s.dbid AND k.userid = s.userid
  JOIN pg_database d
    ON  d.oid = s.dbid
  JOIN pg_roles r
    ON r.oid = s.userid;

CREATE VIEW pg_stat_kcache AS
SELECT datname,
       SUM(reads) AS reads,
       SUM(reads_blks) AS reads_blks,
       SUM(writes) AS writes,
       SUM(writes_blks) AS writes_blks,
       SUM(user_time) AS user_time,
       SUM(system_time) AS system_time
  FROM pg_stat_kcache_detail
 GROUP BY datname;

GRANT SELECT ON pg_stat_kcache_detail TO public;
GRANT SELECT ON pg_stat_kcache TO public;
GRANT ALL ON FUNCTION pg_stat_kcache() TO public;
REVOKE ALL ON FUNCTION pg_stat_kcache_reset() FROM public;

CREATE FUNCTION pg_stat_ucache(OUT queryid bigint, OUT userid oid, OUT dbid oid, OUT uid bigint,
    OUT reads bigint, OUT writes bigint, OUT user_time double precision,
    OUT system_time double precision)
    RETURNS SETOF record
    LANGUAGE c COST 1000
    AS '$libdir/pg_stat_kcache', 'pg_stat_ucache';

CREATE FUNCTION pg_stat_ucache_reset()
    RETURNS void
    LANGUAGE c COST 1000
    AS '$libdir/pg_stat_kcache', 'pg_stat_ucache_reset';

CREATE VIEW pg_stat_ucache_detail AS
SELECT s.query, d.datname, r.rolname,
       k.uid,
       k.reads AS reads,
       k.reads/(current_setting('block_size')::integer) AS reads_blks,
       k.writes AS writes,
       k.writes/(current_setting('block_size')::integer) AS writes_blks,
       k.user_time,
       k.system_time
  FROM pg_stat_ucache() k
  JOIN pg_stat_statements s
    ON k.queryid = s.queryid AND k.dbid = s.dbid AND k.userid = s.userid
  JOIN pg_database d
    ON  d.oid = s.dbid
  JOIN pg_roles r
    ON r.oid = s.userid;

CREATE VIEW pg_stat_ucache AS
SELECT datname,
       SUM(reads) AS reads,
       SUM(reads_blks) AS reads_blks,
       SUM(writes) AS writes,
       SUM(writes_blks) AS writes_blks,
       SUM(user_time) AS user_time,
       SUM(system_time) AS system_time
  FROM pg_stat_ucache_detail
 GROUP BY datname;

GRANT SELECT ON pg_stat_ucache_detail TO public;
GRANT SELECT ON pg_stat_ucache TO public;
GRANT ALL ON FUNCTION pg_stat_ucache() TO public;
REVOKE ALL ON FUNCTION pg_stat_ucache_reset() FROM public;