#ifndef DB_H
#define DB_H

#include <stdint.h>
#include <stdbool.h>

// Page size (4KB)
#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100

// B+tree node configuration
#define ORDER 4  // Order of B+tree (max children per node)
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
    uint32_t size;  // For TEXT type
} Column;

// Table schema
typedef struct {
    char name[32];
    uint32_t num_columns;
    Column* columns;
    uint32_t row_size;
} Schema;

// Node types
typedef enum {
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

// B+tree node header
typedef struct {
    NodeType type;
    bool is_root;
    uint32_t parent;
    uint32_t num_keys;
} NodeHeader;

// Forward declarations
typedef struct Table Table;
typedef struct Pager Pager;
typedef struct Cursor Cursor;

#endif // DB_H
