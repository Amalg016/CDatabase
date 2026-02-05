#ifndef TABLE_H
#define TABLE_H

#include "db.h"
#include "pager.h"
#include "btree.h"
#include <stdlib.h>
#include <string.h>

struct Table {
    Pager* pager;
    Schema* schema;
};


void serialize_row(Schema* schema, void** values, void* destination);

void** deserialize_row(Schema* schema, void* source);

// Print a row
void print_row(Schema* schema, void** values); 


#endif // TABLE_H
