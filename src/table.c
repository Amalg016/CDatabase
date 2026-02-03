#include "../include/table.h"
#include "../include/btree.h"

Table *table_create(const char *filename, Schema *schema) {
  Pager *pager = pager_open(filename);

  Table *table = (Table *)malloc(sizeof(Table));
  table->pager = pager;
  table->schema = schema;

  if (pager->num_pages == 0) {
    // New database file. Initialize page 0 as leaf node.
    void *root_node = pager_get_page(pager, 0);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }

  table->root_page_num = 0;

  return table;
}

// Close table and free resources
void table_close(Table *table) {
  pager_close(table->pager);
  if (table->schema->columns) {
    free(table->schema->columns);
  }
  free(table->schema);
  free(table);
}

// Serialize a row into a buffer based on schema
void serialize_row(Schema *schema, void **values, void *destination) {
  uint32_t offset = 0;
  for (uint32_t i = 0; i < schema->num_columns; i++) {
    Column *col = &schema->columns[i];
    if (col->type == COL_TYPE_INT) {
      memcpy(destination + offset, values[i], sizeof(int32_t));
      offset += sizeof(int32_t);
    } else if (col->type == COL_TYPE_TEXT) {
      memcpy(destination + offset, values[i], col->size);
      offset += col->size;
    }
  }
}

// Deserialize a row from buffer based on schema
void **deserialize_row(Schema *schema, void *source) {
  void **values = malloc(sizeof(void *) * schema->num_columns);
  uint32_t offset = 0;

  for (uint32_t i = 0; i < schema->num_columns; i++) {
    Column *col = &schema->columns[i];
    if (col->type == COL_TYPE_INT) {
      values[i] = malloc(sizeof(int32_t));
      memcpy(values[i], source + offset, sizeof(int32_t));
      offset += sizeof(int32_t);
    } else if (col->type == COL_TYPE_TEXT) {
      values[i] = malloc(col->size);
      memcpy(values[i], source + offset, col->size);
      offset += col->size;
    }
  }

  return values;
}

// Print a row
void print_row(Schema *schema, void **values) {
  for (uint32_t i = 0; i < schema->num_columns; i++) {
    Column *col = &schema->columns[i];
    printf("%s: ", col->name);
    if (col->type == COL_TYPE_INT) {
      printf("%d", *(int32_t *)values[i]);
    } else if (col->type == COL_TYPE_TEXT) {
      printf("%s", (char *)values[i]);
    }
    if (i < schema->num_columns - 1) {
      printf(", ");
    }
  }
  printf("\n");
}

// Calculate row size from schema
uint32_t calculate_row_size(Schema *schema) {
  uint32_t size = 0;
  for (uint32_t i = 0; i < schema->num_columns; i++) {
    Column *col = &schema->columns[i];
    if (col->type == COL_TYPE_INT) {
      size += sizeof(int32_t);
    } else if (col->type == COL_TYPE_TEXT) {
      size += col->size;
    }
  }
  return size;
}

// Create a schema
Schema *schema_create(const char *table_name, uint32_t num_columns) {
  Schema *schema = (Schema *)malloc(sizeof(Schema));
  strncpy(schema->name, table_name, 31);
  schema->name[31] = '\0';
  schema->num_columns = num_columns;
  schema->columns = (Column *)malloc(sizeof(Column) * num_columns);
  schema->row_size = 0;
  return schema;
}

// Add a column to schema
void schema_add_column(Schema *schema, uint32_t index, const char *name,
                       ColumnType type, uint32_t size) {
  if (index >= schema->num_columns) {
    printf("Column index out of bounds\n");
    return;
  }

  Column *col = &schema->columns[index];
  strncpy(col->name, name, 31);
  col->name[31] = '\0';
  col->type = type;
  col->size = (type == COL_TYPE_TEXT) ? size : sizeof(int32_t);

  schema->row_size = calculate_row_size(schema);
}
