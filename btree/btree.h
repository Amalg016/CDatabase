#ifndef BTREE_H
#define BTREE_H

#include <stdint.h>

void btree_init();
void btree_insert(uint32_t key, uint32_t value_offset);
int  btree_search(uint32_t key, uint32_t *value_offset);

#endif
