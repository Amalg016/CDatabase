#ifndef TABLE_H
#define TABLE_H

#include "db.h"
#include "pager.h"
#include "btree.h"
#include <stdlib.h>
#include <string.h>

struct Table {
    Pager* pager;
    uint32_t root_page_num;
    Schema* schema;
};

Table* table_create(const char* filename, Schema* schema);
void table_close(Table* table);
void serialize_row(Schema* schema, void** values, void* destination);

void** deserialize_row(Schema* schema, void* source);

// Print a row
void print_row(Schema* schema, void** values); 

uint32_t calculate_row_size(Schema* schema); 

Schema* schema_create(const char* table_name, uint32_t num_columns); 

void schema_add_column(Schema* schema, uint32_t index, const char* name, ColumnType type, uint32_t size);

#endif // TABLE_H
