// wal.c
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>

#include "../include/pager.h"   // pager_get_page()

/*
 WAL RECORD FORMAT (PHYSICAL LOGGING)
 -----------------------------------
 [uint32 page_num]
 [uint32 offset]
 [uint32 size]
 [bytes  data[size]]
*/

static int wal_fd = -1;

/* Open WAL file */
void wal_open(const char *filename) {
    wal_fd = open(filename, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (wal_fd < 0) {
        perror("wal_open");
        exit(1);
    }
}

/* Close WAL file */
void wal_close() {
    if (wal_fd >= 0) {
        close(wal_fd);
        wal_fd = -1;
    }
}

/* Write-ahead log write (MUST fsync) */
void wal_log_write(
    uint32_t page_num,
    uint32_t offset,
    const void *data,
    uint32_t size
) {
    if (wal_fd < 0) {
        fprintf(stderr, "WAL not opened\n");
        exit(1);
    }

    write(wal_fd, &page_num, sizeof(page_num));
    write(wal_fd, &offset, sizeof(offset));
    write(wal_fd, &size, sizeof(size));
    write(wal_fd, data, size);

    printf("%s", "WAL used here\n");
    /* Durability guarantee */
    fsync(wal_fd);
}

/* Replay WAL on startup */
void wal_replay() {
    if (wal_fd < 0)
        return;

    lseek(wal_fd, 0, SEEK_SET);

    while (1) {
        uint32_t page_num, offset, size;

        ssize_t r = read(wal_fd, &page_num, sizeof(page_num));
        if (r <= 0)
            break;

        read(wal_fd, &offset, sizeof(offset));
        read(wal_fd, &size, sizeof(size));

        void *data = malloc(size);
        read(wal_fd, data, size);

        void *page = pager_get_page(page_num);
        memcpy((char *)page + offset, data, size);

        free(data);
    }

    /* Move back to append mode */
    lseek(wal_fd, 0, SEEK_END);
}
