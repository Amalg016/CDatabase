#define _GNU_SOURCE
#include "../include/btree.h"
#include "../include/cursor.h"
#include "../include/database.h"
#include "../include/pager.h"
#include "../include/parser.h"
#include "../include/table.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Global database
Database *current_db = NULL;

// Meta command types
typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

// Execute result types
typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_TABLE_FULL,
  EXECUTE_TABLE_NOT_FOUND
} ExecuteResult;

void print_prompt() { printf("db > "); }

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
    // .btree <table_name>
    char *table_name = strchr(input_buffer->buffer, ' ');
    if (table_name) {
      table_name++; // Skip the space
      Table *table = table_open(current_db, table_name);
      if (table) {
        printf("Tree for table '%s':\n", table->schema->name);
        print_tree(table->pager, table->schema->root_page_num, 0);
        table_close(table);
      } else {
        printf("Table '%s' not found\n", table_name);
      }
    } else {
      printf("Usage: .btree <table_name>\n");
    }
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
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
          size = 32;
      } else {
        printf("Unknown type: %s\n", type_str);
        schema->in_use = false;
        return EXECUTE_TABLE_FULL;
      }

      schema_add_column(schema, i, name, type, size, is_pk);

      if (schema->pk_column == -1 && is_pk && type == COL_TYPE_INT) {
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

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert(Statement *statement) {
  // Open the table
  Table *table = table_open(current_db, statement->table_name);
  if (!table) {
    printf("Table '%s' not found\n", statement->table_name);
    return EXECUTE_TABLE_NOT_FOUND;
  }

  void *node = pager_get_page(table->pager, table->schema->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  void *row_data = malloc(table->schema->row_size);
  serialize_row(table->schema, statement->values, row_data);

  // Determine the B+tree key
  uint32_t btree_key;

  if (table->schema->pk_column != -1) {
    // Use PRIMARY KEY column value as B+tree key
    int32_t pk_value = *(int32_t *)statement->values[table->schema->pk_column];
    if (pk_value <= 0) {
      printf("Error: PRIMARY KEY must be positive integer\n");
      free(row_data);
      table_close(table);
      return EXECUTE_TABLE_FULL;
    }
    btree_key = (uint32_t)pk_value;
  } else {
    // No PRIMARY KEY - use auto-increment ROWID
    btree_key = table->schema->next_rowid++;
    printf("Note: No PK, assigned ROWID=%u\n", btree_key);
  }

  Cursor *cursor = table_find(table, btree_key);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = cursor_key(cursor);
    if (key_at_index == btree_key) {
      free(row_data);
      cursor_free(cursor);
      table_close(table);
      if (table->schema->pk_column != -1) {
        printf("Error: Duplicate PRIMARY KEY value %u\n", btree_key);
      } else {
        printf("Error: Duplicate ROWID\n");
      }
      return EXECUTE_TABLE_FULL;
    }
  }

  leaf_node_insert(cursor, btree_key, row_data, table->schema->row_size);

  free(row_data);
  cursor_free(cursor);
  table_close(table);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement) {
  // Open the table
  Table *table = table_open(current_db, statement->table_name);
  if (!table) {
    printf("Table '%s' not found\n", statement->table_name);
    return EXECUTE_TABLE_NOT_FOUND;
  }

  // Check if we can optimize with B+tree range scan
  // Only works if WHERE clause is on the PRIMARY KEY column
  bool can_optimize = false;
  bool is_pk_filter = false;

  if (statement->where_op != OP_NONE && table->schema->pk_column != -1) {
    // Check if WHERE column is the PK
    Column *pk_col = &table->schema->columns[table->schema->pk_column];
    if (strcasecmp(statement->where_column, pk_col->name) == 0) {
      is_pk_filter = true;
      // Can optimize for =, >, >=, BETWEEN on PK
      if (statement->where_op == OP_EQUAL ||
          statement->where_op == OP_GREATER ||
          statement->where_op == OP_GREATER_EQUAL ||
          statement->where_op == OP_BETWEEN) {
        can_optimize = true;
      }
    }
  }

  Cursor *cursor;
  uint32_t end_key = UINT32_MAX; // For BETWEEN upper bound

  if (can_optimize) {
    // OPTIMIZED PATH: Use B+tree to start at the right position
    switch (statement->where_op) {
    case OP_EQUAL:
      // For =, find exact key
      cursor = table_find(table, statement->where_value);
      end_key = statement->where_value; // Stop after this key
      break;

    case OP_GREATER:
      // For >, start at next key after value
      cursor = table_find_greater_or_equal(table, statement->where_value + 1);
      break;

    case OP_GREATER_EQUAL:
      // For >=, start at value
      cursor = table_find_greater_or_equal(table, statement->where_value);
      break;

    case OP_BETWEEN:
      // For BETWEEN, start at lower bound
      cursor = table_find_greater_or_equal(table, statement->where_value);
      end_key = statement->where_value2; // Stop after upper bound
      break;

    default:
      // Fallback (shouldn't happen)
      cursor = table_start(table);
      can_optimize = false;
    }
  } else {
    // UNOPTIMIZED PATH: Full table scan
    cursor = table_start(table);
  }

  uint32_t rows_matched = 0;

  while (!cursor->end_of_table) {
    uint32_t current_key = cursor_key(cursor);

    // For optimized queries, check if we've passed the end range
    if (can_optimize && current_key > end_key) {
      break; // Early termination - don't scan rest of table!
    }

    void *row_data = cursor_value(cursor);
    void **values = deserialize_row(table->schema, row_data);

    // Apply WHERE clause filter
    bool row_matches = true;
    if (statement->where_op != OP_NONE) {
      // Find the WHERE column value
      int32_t col_value = 0;
      bool col_found = false;

      for (uint32_t i = 0; i < table->schema->num_columns; i++) {
        Column *col = &table->schema->columns[i];
        if (strcasecmp(statement->where_column, col->name) == 0) {
          col_value = *(int32_t *)values[i];
          col_found = true;
          break;
        }
      }

      if (col_found) {
        if (is_pk_filter) {
          // For PK filters, we've already optimized the scan
          // Just need to handle the operator correctly
          switch (statement->where_op) {
          case OP_EQUAL:
            row_matches = (current_key == (uint32_t)statement->where_value);
            break;
          case OP_GREATER:
            row_matches = (current_key > (uint32_t)statement->where_value);
            break;
          case OP_LESS:
            row_matches = (current_key < (uint32_t)statement->where_value);
            break;
          case OP_GREATER_EQUAL:
            row_matches = (current_key >= (uint32_t)statement->where_value);
            break;
          case OP_LESS_EQUAL:
            row_matches = (current_key <= (uint32_t)statement->where_value);
            break;
          case OP_BETWEEN:
            row_matches = (current_key >= (uint32_t)statement->where_value &&
                           current_key <= (uint32_t)statement->where_value2);
            break;
          default:
            row_matches = true;
          }
        } else {
          // Non-PK filter: full table scan with filtering
          switch (statement->where_op) {
          case OP_EQUAL:
            row_matches = (col_value == statement->where_value);
            break;
          case OP_GREATER:
            row_matches = (col_value > statement->where_value);
            break;
          case OP_LESS:
            row_matches = (col_value < statement->where_value);
            break;
          case OP_GREATER_EQUAL:
            row_matches = (col_value >= statement->where_value);
            break;
          case OP_LESS_EQUAL:
            row_matches = (col_value <= statement->where_value);
            break;
          case OP_BETWEEN:
            row_matches = (col_value >= statement->where_value &&
                           col_value <= statement->where_value2);
            break;
          default:
            row_matches = true;
          }
        }
      }
    }

    // Print row if it matches WHERE condition
    if (row_matches) {
      rows_matched++;
      printf("Key: %d | ", current_key);

      if (statement->select_columns == NULL) {
        // SELECT * - print all columns
        print_row(table->schema, values);
      } else {
        // SELECT specific columns
        for (uint32_t i = 0; i < statement->num_select_columns; i++) {
          // Find the column in schema
          for (uint32_t j = 0; j < table->schema->num_columns; j++) {
            Column *col = &table->schema->columns[j];
            if (strcasecmp(statement->select_columns[i], col->name) == 0) {
              // Print this column
              printf("%s: ", col->name);
              if (col->type == COL_TYPE_INT) {
                printf("%d", *(int32_t *)values[j]);
              } else if (col->type == COL_TYPE_TEXT) {
                printf("%s", (char *)values[j]);
              }
              if (i < statement->num_select_columns - 1) {
                printf(", ");
              }
              break;
            }
          }
        }
        printf("\n");
      }
    }

    for (uint32_t i = 0; i < table->schema->num_columns; i++) {
      free(values[i]);
    }
    free(values);

    cursor_advance(cursor);
  }

  if (statement->where_op != OP_NONE) {
    printf("(%u rows matched)\n", rows_matched);
    if (can_optimize) {
      printf("[Optimized: B+tree range scan]\n");
    } else {
      printf("[Full table scan]\n");
    }
  }

  cursor_free(cursor);
  table_close(table);
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement) {
  switch (statement->type) {
  case STATEMENT_CREATE_TABLE:
    return execute_create_table(statement);
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

  printf("Multi-Table B+Tree Database with SQL-like syntax\n");
  printf("Commands:\n");
  printf("  CREATE TABLE <n> <num_columns> - Create a new table\n");
  printf("  INSERT INTO <table> VALUES <val1> <val2> ... - Insert record\n");
  printf("  INSERT <table> <val1> <val2> ... - Insert (short form)\n");
  printf("  SELECT * FROM <table> - Display all records\n");
  printf("  SELECT <col1> <col2> FROM <table> - Display specific columns\n");
  printf("  SELECT * FROM <table> WHERE <col> <op> <val> - Filter results\n");
  printf("    Operators: =, >, <, >=, <=, BETWEEN x AND y\n");
  printf("  .tables - List all tables\n");
  printf("  .btree <table> - Show B+tree structure\n");
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
    switch (prepare_statement(input_buffer, &statement, current_db)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_SYNTAX_ERROR:
      printf("Syntax error.\n");
      continue;
    case PREPARE_UNRECOGNIZED_STATEMENT:
      printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
      continue;
    case PREPARE_TABLE_NOT_FOUND:
      printf("Error: Table not found.\n");
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

    // Cleanup statement
    free_statement(&statement, current_db);
  }
}
