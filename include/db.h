#ifndef DB_MULTI_H
#define DB_MULTI_H

#include <stdint.h>
#include <stdbool.h>

// Page size (4KB)
#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 400
#define MAX_TABLES 10

// B+tree node configuration
#define ORDER 4
#define MAX_KEYS (ORDER - 1)
#define MIN_KEYS ((ORDER + 1) / 2 - 1)

// Column types
typedef enum {
    COL_TYPE_INT,
    COL_TYPE_TEXT
} ColumnType;

// Column definition
typedef struct {
    char name[32];
    ColumnType type;
    uint32_t size;
    bool is_pk;  // Is this column the primary key?
} Column;

#define MAX_COLUMNS_PER_TABLE 10

// Table schema
typedef struct {
    char name[32];
    uint32_t num_columns;
    Column columns[MAX_COLUMNS_PER_TABLE];  // Inline storage instead of pointer
    uint32_t row_size;
    uint32_t root_page_num;  // Root page for this table's B+tree
    bool in_use;
    int32_t pk_column;  // Index of PRIMARY KEY column (-1 if none)
    uint32_t next_rowid;  // Auto-increment if no PK
} Schema;

// Database catalog (stored in page 0)
typedef struct {
    uint32_t num_tables;
    uint32_t next_free_page;
    Schema tables[MAX_TABLES];
} Catalog;

// Node types
typedef enum {
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

// Forward declarations
typedef struct Table Table;
typedef struct Pager Pager;
typedef struct Cursor Cursor;
typedef struct Database Database;
#endif // DB_MULTI_H
