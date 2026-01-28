#include "db.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>

#define MAX_INDEX 1024
#define MAX_KEY   64

struct index_entry {
    char key[MAX_KEY];
    off_t offset;
};

static int db_fd = -1;
static struct index_entry db_index[MAX_INDEX];
static size_t index_size = 0;


static void load_index() {
    off_t offset = 0;

    while (1) {
        uint32_t key_len, val_len;
        ssize_t r;

        r = pread(db_fd, &key_len, sizeof(key_len), offset);
        if (r <= 0) break;

        pread(db_fd, &val_len, sizeof(val_len), offset + 4);

        char key[MAX_KEY];
        pread(db_fd, key, key_len, offset + 8);
        key[key_len] = '\0';

        strncpy(db_index[index_size].key, key, MAX_KEY);
        db_index[index_size].offset = offset;
        index_size++;

        offset += 8 + key_len + val_len;
    }
}

int db_open(const char *filename) {
    db_fd = open(filename, O_RDWR | O_CREAT, 0644);
    if (db_fd < 0) return -1;

    load_index();
    return 0;
}

void db_close() {
    if (db_fd >= 0) close(db_fd);
}

int db_set(const char *key, const char *value) {
    uint32_t key_len = strlen(key);
    uint32_t val_len = strlen(value);

    off_t offset = lseek(db_fd, 0, SEEK_END);

    write(db_fd, &key_len, 4);
    write(db_fd, &val_len, 4);
    write(db_fd, key, key_len);
    write(db_fd, value, val_len);

    strncpy(db_index[index_size].key, key, MAX_KEY);
    db_index[index_size].offset = offset;
    index_size++;

    fsync(db_fd);
    return 0;
}


char *db_get(const char *key) {
    for (ssize_t i = index_size - 1; i >= 0; i--) {
        if (strcmp(db_index[i].key, key) == 0) {
            off_t offset = db_index[i].offset;

            uint32_t key_len, val_len;
            pread(db_fd, &key_len, 4, offset);
            pread(db_fd, &val_len, 4, offset + 4);

            char *value = malloc(val_len + 1);
            pread(db_fd, value, val_len, offset + 8 + key_len);
            value[val_len] = '\0';

            return value;
        }
    }
    return NULL;
}
