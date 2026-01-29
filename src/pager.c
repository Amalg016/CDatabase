#include "../include/pager.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#define PAGE_SIZE 4096
#define MAX_PAGES 1024

static int fd;
static void *pages[MAX_PAGES];
static uint32_t num_pages;

int pager_open(const char *filename) {
    fd = open(filename, O_RDWR | O_CREAT, 0644);
    off_t size = lseek(fd, 0, SEEK_END);
    num_pages = size / PAGE_SIZE;
    return fd >= 0 ? 0 : -1;
}

void *pager_get_page(uint32_t page_num) {
     if (page_num >= MAX_PAGES) {
        fprintf(stderr, "Pager error: page %u out of bounds\n", page_num);
        exit(1);
    }
    if (!pages[page_num]) {
        pages[page_num] = calloc(1, PAGE_SIZE);
        pread(fd, pages[page_num], PAGE_SIZE, page_num * PAGE_SIZE);
    }
    return pages[page_num];
}

uint32_t pager_new_page() {
    if (num_pages >= MAX_PAGES) {
        fprintf(stderr, "Pager full\n");
        exit(1);
    }
    return num_pages++;
}

void pager_close() {
    for (uint32_t i = 0; i < num_pages; i++) {
        if (pages[i]) {
            pwrite(fd, pages[i], PAGE_SIZE, i * PAGE_SIZE);
            free(pages[i]);
        }
    }
    close(fd);
}
