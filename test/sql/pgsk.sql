CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION pg_stat_kcache;

-- dummy query
SELECT 1 AS dummy;

SELECT count(*) FROM pg_stat_kcache WHERE datname = current_database();

SELECT count(*) FROM pg_stat_kcache_detail WHERE datname = current_database() AND (query = 'SELECT $1 AS dummy' OR query = 'SELECT ? AS dummy;');

SELECT reads, reads_blks, writes, writes_blks FROM pg_stat_kcache_detail WHERE datname = current_database() AND (query = 'SELECT $1 AS dummy' OR query = 'SELECT ? AS dummy;');

-- dummy table
CREATE TABLE test AS SELECT i FROM generate_series(1, 1000) i;

-- dummy query again
SELECT count(*) FROM test;

SELECT user_time + system_time > 0 AS cpu_time_ok FROM pg_stat_kcache_detail WHERE datname = current_database() AND query LIKE 'SELECT count(*) FROM test%';
