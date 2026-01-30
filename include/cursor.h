#ifndef CURSOR_H
#define CURSOR_H

#include "db.h"
#include "table.h"
#include "btree.h"
#include <stdlib.h>

struct Cursor {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
};
Cursor* table_start(Table* table);
Cursor* table_find(Table* table, uint32_t key);
void* cursor_value(Cursor* cursor);
uint32_t cursor_key(Cursor* cursor);
void cursor_advance(Cursor* cursor);
void leaf_node_insert(Cursor* cursor, uint32_t key, void* value);
void create_new_root(Table* table, uint32_t right_child_page_num);
void cursor_free(Cursor* cursor);

Cursor* create_cursor(Table* table, uint32_t page_num, uint32_t cell_num);

#endif
