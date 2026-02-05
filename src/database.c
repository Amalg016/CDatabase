#include "../include/database.h"

// Initialize a new database
Database *db_open(const char *filename) {
  Pager *pager = pager_open(filename);
  Database *db = malloc(sizeof(Database));
  db->pager = pager;

  if (pager->num_pages == 0) {
    // New database - initialize catalog in page 0
    void *catalog_page = pager_get_page(pager, 0);
    db->catalog = (Catalog *)catalog_page;
    db->catalog->num_tables = 0;
    db->catalog->next_free_page = 1; // Page 0 is for catalog

    for (uint32_t i = 0; i < MAX_TABLES; i++) {
      db->catalog->tables[i].in_use = false;
    }
  } else {
    // Existing database - load catalog from page 0
    void *catalog_page = pager_get_page(pager, 0);
    db->catalog = (Catalog *)catalog_page;
  }

  return db;
}

// Close database
void db_close(Database *db) {
  pager_close(db->pager);
  free(db);
}

// Create a new table in the database
Schema *db_create_table(Database *db, const char *table_name,
                        uint32_t num_columns) {
  // Check if table already exists
  for (uint32_t i = 0; i < MAX_TABLES; i++) {
    if (db->catalog->tables[i].in_use &&
        strcmp(db->catalog->tables[i].name, table_name) == 0) {
      printf("Table '%s' already exists\n", table_name);
      return NULL;
    }
  }

  // Find free slot
  uint32_t slot = MAX_TABLES;
  for (uint32_t i = 0; i < MAX_TABLES; i++) {
    if (!db->catalog->tables[i].in_use) {
      slot = i;
      break;
    }
  }

  if (slot == MAX_TABLES) {
    printf("Maximum number of tables reached\n");
    return NULL;
  }

  // Initialize schema
  Schema *schema = &db->catalog->tables[slot];
  strncpy(schema->name, table_name, 31);
  schema->name[31] = '\0';
  schema->num_columns = num_columns;
  schema->in_use = true;
  schema->row_size = 0;
  schema->pk_column = -1; // No PK yet
  schema->next_rowid = 1; // Start auto-increment at 1

  // Allocate a root page for this table
  schema->root_page_num = db->catalog->next_free_page++;

  if (num_columns > MAX_COLUMNS_PER_TABLE) {
    printf("Too many columns (max %d)\n", MAX_COLUMNS_PER_TABLE);
    schema->in_use = false;
    return NULL;
  }

  db->catalog->num_tables++;

  return schema;
}

// Get a table by name
Schema *db_get_table(Database *db, const char *table_name) {
  for (uint32_t i = 0; i < MAX_TABLES; i++) {
    if (db->catalog->tables[i].in_use &&
        strcmp(db->catalog->tables[i].name, table_name) == 0) {
      return &db->catalog->tables[i];
    }
  }
  return NULL;
}

// List all tables
void db_list_tables(Database *db) {
  printf("Tables in database:\n");
  for (uint32_t i = 0; i < MAX_TABLES; i++) {
    if (db->catalog->tables[i].in_use) {
      printf("  - %s (%d columns)\n", db->catalog->tables[i].name,
             db->catalog->tables[i].num_columns);
    }
  }
}

// Add column to schema
void schema_add_column(Schema *schema, uint32_t index, const char *name,
                       ColumnType type, uint32_t size, bool is_pk) {
  if (index >= schema->num_columns) {
    printf("Column index out of bounds\n");
    return;
  }

  // Validate PRIMARY KEY constraints
  if (is_pk) {
    // Check if PK already exists
    if (schema->pk_column != -1) {
      printf("Error: Table can only have one PRIMARY KEY\n");
      printf("Column '%s' is already the PRIMARY KEY\n",
             schema->columns[schema->pk_column].name);
      return;
    }

    // PK must be INT
    if (type != COL_TYPE_INT) {
      printf("Error: PRIMARY KEY must be INT type\n");
      return;
    }

    schema->pk_column = index;
  }

  Column *col = &schema->columns[index];
  strncpy(col->name, name, 31);
  col->name[31] = '\0';
  col->type = type;
  col->size = (type == COL_TYPE_TEXT) ? size : sizeof(int32_t);
  col->is_pk = is_pk;

  // Recalculate row size
  schema->row_size = 0;
  for (uint32_t i = 0; i < schema->num_columns; i++) {
    Column *c = &schema->columns[i];
    if (c->type == COL_TYPE_INT) {
      schema->row_size += sizeof(int32_t);
    } else if (c->type == COL_TYPE_TEXT) {
      schema->row_size += c->size;
    }
  }
}

// Open a table for use
Table *table_open(Database *db, const char *table_name) {
  Schema *schema = db_get_table(db, table_name);
  if (!schema) {
    return NULL;
  }

  Table *table = malloc(sizeof(Table));
  table->pager = db->pager;
  table->schema = schema;

  return table;
}

// Close a table (doesn't close the pager, just frees the table struct)
void table_close(Table *table) {
  // Note: Don't close pager here - it's owned by Database
  free(table);
}
