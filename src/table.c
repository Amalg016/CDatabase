#include "../include/table.h"

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
