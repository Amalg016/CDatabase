#ifndef PAGER_H
#define PAGER_H

#include <stdint.h>

int pager_open(const char *filename);
void pager_close();

void *pager_get_page(uint32_t page_num);
uint32_t pager_new_page();

#endif
