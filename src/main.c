#define _GNU_SOURCE
#include "../include/btree.h"
#include "../include/cursor.h"
#include "../include/database.h"
#include "../include/pager.h"
#include "../include/table.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  char *buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

InputBuffer *new_input_buffer() {
  InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;
  return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer *input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer *input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_NO_TABLE_SELECTED
} PrepareResult;

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
  STATEMENT_CREATE_TABLE,
  STATEMENT_USE_TABLE
} StatementType;

typedef struct {
  StatementType type;
  char table_name[32];
  uint32_t key;
  void **values;
  // For CREATE TABLE
  uint32_t num_columns;
  Column *columns;
} Statement;

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_TABLE_FULL,
  EXECUTE_TABLE_NOT_FOUND
} ExecuteResult;

// Global current table and database
Database *current_db = NULL;
Table *current_table = NULL;

MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    if (current_db) {
      db_close(current_db);
    }
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".tables") == 0) {
    if (current_db) {
      db_list_tables(current_db);
    } else {
      printf("No database open\n");
    }
    return META_COMMAND_SUCCESS;
  } else if (strncmp(input_buffer->buffer, ".btree", 6) == 0) {
    if (current_table) {
      printf("Tree for table '%s':\n", current_table->schema->name);
      print_tree(current_table->pager, current_table->schema->root_page_num, 0);
    } else {
      printf("No table selected. Use 'use <table_name>' first.\n");
    }
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_create_table(InputBuffer *input_buffer,
                                   Statement *statement) {
  statement->type = STATEMENT_CREATE_TABLE;

  char *keyword = strtok(input_buffer->buffer, " "); // "create"
  keyword = strtok(NULL, " ");                       // "table"
  if (!keyword || strcmp(keyword, "table") != 0) {
    return PREPARE_SYNTAX_ERROR;
  }

  char *table_name = strtok(NULL, " ");
  if (!table_name) {
    return PREPARE_SYNTAX_ERROR;
  }
  strncpy(statement->table_name, table_name, 31);
  statement->table_name[31] = '\0';

  // For simplicity: "create table <name> <num_cols>"
  // Then we'll ask for column definitions
  char *num_cols_str = strtok(NULL, " ");
  if (!num_cols_str) {
    return PREPARE_SYNTAX_ERROR;
  }

  statement->num_columns = atoi(num_cols_str);
  if (statement->num_columns == 0 || statement->num_columns > 10) {
    printf("Number of columns must be between 1 and 10\n");
    return PREPARE_SYNTAX_ERROR;
  }

  return PREPARE_SUCCESS;
}

PrepareResult prepare_use_table(InputBuffer *input_buffer,
                                Statement *statement) {
  statement->type = STATEMENT_USE_TABLE;

  char *keyword = strtok(input_buffer->buffer, " "); // "use"
  char *table_name = strtok(NULL, " ");

  if (!table_name) {
    return PREPARE_SYNTAX_ERROR;
  }

  strncpy(statement->table_name, table_name, 31);
  statement->table_name[31] = '\0';

  return PREPARE_SUCCESS;
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
  if (!current_table) {
    return PREPARE_NO_TABLE_SELECTED;
  }

  statement->type = STATEMENT_INSERT;

  strtok(input_buffer->buffer, " "); // Skip "insert" keyword

  statement->values =
      malloc(sizeof(void *) * current_table->schema->num_columns);

  for (uint32_t i = 0; i < current_table->schema->num_columns; i++) {
    Column *col = &current_table->schema->columns[i];
    char *value_string = strtok(NULL, " ");

    if (value_string == NULL) {
      printf("Error: Not enough values. Expected %d columns\n",
             current_table->schema->num_columns);
      // Free already allocated values
      for (uint32_t j = 0; j < i; j++) {
        free(statement->values[j]);
      }
      free(statement->values);
      return PREPARE_SYNTAX_ERROR;
    }

    if (col->type == COL_TYPE_INT) {
      statement->values[i] = malloc(sizeof(int32_t));
      *(int32_t *)statement->values[i] = atoi(value_string);
    } else if (col->type == COL_TYPE_TEXT) {
      statement->values[i] = malloc(col->size);
      strncpy((char *)statement->values[i], value_string, col->size - 1);
      ((char *)statement->values[i])[col->size - 1] = '\0';
    }
  }

  return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "create table", 12) == 0) {
    return prepare_create_table(input_buffer, statement);
  }
  if (strncmp(input_buffer->buffer, "use", 3) == 0) {
    return prepare_use_table(input_buffer, statement);
  }
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    if (!current_table) {
      return PREPARE_NO_TABLE_SELECTED;
    }
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_create_table(Statement *statement) {
  Schema *schema = db_create_table(current_db, statement->table_name,
                                   statement->num_columns);
  if (!schema) {
    return EXECUTE_TABLE_FULL;
  }

  printf("Enter column definitions (name type [size] [PRIMARY KEY]):\n");
  printf("Types: int, text\n");
  printf("Example: id int PRIMARY KEY\n");
  printf("Example: name text 50\n");

  char line[256];
  for (uint32_t i = 0; i < statement->num_columns; i++) {
    printf("Column %d: ", i + 1);
    if (fgets(line, sizeof(line), stdin)) {
      char name[32];
      char type_str[16];
      char pk_str[16] = "";
      int size = 0;

      // Try to parse with optional PRIMARY KEY
      int scanned =
          sscanf(line, "%31s %15s %d %15s", name, type_str, &size, pk_str);
      if (scanned < 2) {
        // Try without size
        scanned = sscanf(line, "%31s %15s %15s", name, type_str, pk_str);
        if (scanned < 2) {
          printf("Invalid column definition\n");
          schema->in_use = false;
          return EXECUTE_TABLE_FULL;
        }
      }

      // Check for PRIMARY KEY keywords
      bool is_pk = false;
      if (strstr(line, "PRIMARY") != NULL || strstr(line, "PK") != NULL) {
        is_pk = true;
      }

      ColumnType type;
      if (strcmp(type_str, "int") == 0) {
        type = COL_TYPE_INT;
        size = 0;
      } else if (strcmp(type_str, "text") == 0) {
        type = COL_TYPE_TEXT;
        if (size == 0)
          size = 32; // default text size
      } else {
        printf("Unknown type: %s\n", type_str);
        schema->in_use = false;
        return EXECUTE_TABLE_FULL;
      }

      schema_add_column(schema, i, name, type, size, is_pk);

      // Check if schema_add_column failed (e.g., duplicate PK)
      if (schema->pk_column == -1 && is_pk && type == COL_TYPE_INT) {
        // Should have been set but wasn't - error occurred
        schema->in_use = false;
        return EXECUTE_TABLE_FULL;
      }
    }
  }

  // Initialize root node for this table
  void *root_node = pager_get_page(current_db->pager, schema->root_page_num);
  initialize_leaf_node(root_node);
  set_node_root(root_node, true);

  printf("Table '%s' created successfully\n", schema->name);
  if (schema->pk_column != -1) {
    printf("PRIMARY KEY: %s (fast lookups enabled)\n",
           schema->columns[schema->pk_column].name);
  } else {
    printf("No PRIMARY KEY (using auto-increment ROWID, slower lookups)\n");
  }

  // Auto-select the new table
  if (current_table) {
    table_close(current_table);
  }
  current_table = table_open(current_db, schema->name);
  printf("Current table: %s\n", current_table->schema->name);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_use_table(Statement *statement) {
  Schema *schema = db_get_table(current_db, statement->table_name);
  if (!schema) {
    printf("Table '%s' not found\n", statement->table_name);
    return EXECUTE_TABLE_NOT_FOUND;
  }

  // Close current table if any
  if (current_table) {
    table_close(current_table);
  }

  // Open the new table
  current_table = table_open(current_db, statement->table_name);
  printf("Using table: %s\n", current_table->schema->name);

  // Show schema
  printf("Columns: ");
  for (uint32_t i = 0; i < current_table->schema->num_columns; i++) {
    Column *col = &current_table->schema->columns[i];
    printf("%s(%s)", col->name, col->type == COL_TYPE_INT ? "int" : "text");
    if (col->is_pk) {
      printf(" [PK]");
    }
    if (i < current_table->schema->num_columns - 1)
      printf(", ");
  }
  printf("\n");

  if (current_table->schema->pk_column != -1) {
    printf(
        "PRIMARY KEY: %s (B+tree indexed for fast lookups)\n",
        current_table->schema->columns[current_table->schema->pk_column].name);
  } else {
    printf("No PRIMARY KEY (using auto-increment ROWID)\n");
  }

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert(Statement *statement) {
  void *node = pager_get_page(current_table->pager,
                              current_table->schema->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  void *row_data = malloc(current_table->schema->row_size);
  serialize_row(current_table->schema, statement->values, row_data);

  // Determine the B+tree key
  uint32_t btree_key;

  if (current_table->schema->pk_column != -1) {
    // Use PRIMARY KEY column value as B+tree key
    int32_t pk_value =
        *(int32_t *)statement->values[current_table->schema->pk_column];
    if (pk_value <= 0) {
      printf("Error: PRIMARY KEY must be positive integer\n");
      free(row_data);
      return EXECUTE_TABLE_FULL;
    }
    btree_key = (uint32_t)pk_value;
  } else {
    // No PRIMARY KEY - use auto-increment ROWID
    btree_key = current_table->schema->next_rowid++;
    printf("Note: No PK, assigned ROWID=%u (slower lookups)\n", btree_key);
  }

  Cursor *cursor = table_find(current_table, btree_key);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = cursor_key(cursor);
    if (key_at_index == btree_key) {
      free(row_data);
      cursor_free(cursor);
      if (current_table->schema->pk_column != -1) {
        printf("Error: Duplicate PRIMARY KEY value %u\n", btree_key);
      } else {
        printf("Error: Duplicate ROWID\n");
      }
      return EXECUTE_TABLE_FULL;
    }
  }

  leaf_node_insert(cursor, btree_key, row_data,
                   current_table->schema->row_size);

  free(row_data);
  cursor_free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement) {
  Cursor *cursor = table_start(current_table);

  while (!cursor->end_of_table) {
    void *row_data = cursor_value(cursor);
    void **values = deserialize_row(current_table->schema, row_data);

    printf("Key: %d | ", cursor_key(cursor));
    print_row(current_table->schema, values);

    for (uint32_t i = 0; i < current_table->schema->num_columns; i++) {
      free(values[i]);
    }
    free(values);

    cursor_advance(cursor);
  }

  cursor_free(cursor);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement) {
  switch (statement->type) {
  case STATEMENT_CREATE_TABLE:
    return execute_create_table(statement);
  case STATEMENT_USE_TABLE:
    return execute_use_table(statement);
  case STATEMENT_INSERT:
    return execute_insert(statement);
  case STATEMENT_SELECT:
    return execute_select(statement);
  default:
    return EXECUTE_SUCCESS;
  }
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char *filename = argv[1];
  current_db = db_open(filename);

  InputBuffer *input_buffer = new_input_buffer();

  printf("Multi-Table B+Tree Database with PRIMARY KEY support\n");
  printf("Commands:\n");
  printf("  create table <name> <num_columns> - Create a new table\n");
  printf("  use <table_name> - Select a table to work with\n");
  printf("  insert <values...> - Insert record (provide all column values)\n");
  printf("  select - Display all records from current table\n");
  printf("  .tables - List all tables\n");
  printf("  .btree - Show B+tree for current table\n");
  printf("  .exit - Exit\n\n");

  if (current_db->catalog->num_tables > 0) {
    printf("Existing tables found:\n");
    db_list_tables(current_db);
    printf("\n");
  }

  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer)) {
      case META_COMMAND_SUCCESS:
        continue;
      case META_COMMAND_UNRECOGNIZED_COMMAND:
        printf("Unrecognized command '%s'\n", input_buffer->buffer);
        continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_NO_TABLE_SELECTED:
      printf("No table selected. Use 'use <table_name>' first.\n");
      continue;
    case PREPARE_SYNTAX_ERROR:
      printf("Syntax error.\n");
      continue;
    case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
      continue;
    }

    switch (execute_statement(&statement)) {
    case EXECUTE_SUCCESS:
      printf("Executed.\n");
      break;
    case EXECUTE_TABLE_FULL:
      printf("Error: Duplicate key or table full.\n");
      break;
    case EXECUTE_TABLE_NOT_FOUND:
      printf("Error: Table not found.\n");
      break;
    }

    if (statement.type == STATEMENT_INSERT && statement.values) {
      for (uint32_t i = 0; i < current_table->schema->num_columns; i++) {
        free(statement.values[i]);
      }
      free(statement.values);
    }
  }
}
