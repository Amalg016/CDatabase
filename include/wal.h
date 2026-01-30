#ifndef WAL_H
#define WAL_H

#include <stdint.h>
#include <stddef.h>

void wal_open(const char *filename);
void wal_close();

void wal_log_write(
    uint32_t page_num,
    uint32_t offset,
    const void *data,
    uint32_t size
);

void wal_replay();

#endif
