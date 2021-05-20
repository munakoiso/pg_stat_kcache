#ifndef PG_STAT_UCACHE_H_
#define PG_STAT_UCACHE_H_

void pgsu_shmem_startup(void);
void pgsu_shmem_shutdown(void);

void pgsu_init(void);
void pgsu_store(QueryDesc *query, int64 reads, int64 writes,
                double utime,
                double stime);

#endif
