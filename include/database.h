#ifndef DATABASE_H
#define DATABASE_H

#include "db.h"
#include "table.h"
#include "pager.h"
#include <string.h>
#include <stdlib.h>

struct Database {
    Pager* pager;
    Catalog* catalog;
};

// Initialize a new database
Database* db_open(const char* filename);

// Close database
void db_close(Database* db); 

// Create a new table in the database
Schema* db_create_table(Database* db, const char* table_name, uint32_t num_columns);

// Get a table by name
Schema* db_get_table(Database* db, const char* table_name); 

// List all tables
void db_list_tables(Database* db) ;

// Add column to schema
void schema_add_column(Schema* schema, uint32_t index, const char* name, ColumnType type, uint32_t size, bool is_pk); 

Table* table_open(Database* db, const char* table_name);
void table_close(Table* table);

#endif // DATABASE_H
