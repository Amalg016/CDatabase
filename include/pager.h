#ifndef PAGER_H
#define PAGER_H

#include "db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

struct Pager {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
};

Pager* pager_open(const char* filename);

void* pager_get_page(Pager* pager, uint32_t page_num); 

void pager_flush(Pager* pager, uint32_t page_num);

void pager_close(Pager* pager); 

uint32_t pager_allocate_page(Pager* pager);

#endif // PAGER_H
