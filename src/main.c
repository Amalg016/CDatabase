#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "../include/table.h"
#include "../include/btree.h"
#include "../include/pager.h"
#include "../include/cursor.h"
#include <stdio.h>


typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void print_prompt() {
    printf("db > ");
}

void read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), 
                                  &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
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
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
    uint32_t key;
    void** values;
} Statement;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        table_close(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
        printf("INTERNAL_NODE_MAX_CELLS: %d\n", INTERNAL_NODE_MAX_CELLS);
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement, 
                             Schema* schema) {
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(input_buffer->buffer, " ");
    char* key_string = strtok(NULL, " ");

    if (key_string == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int key = atoi(key_string);
    if (key < 0) {
        return PREPARE_SYNTAX_ERROR;
    }
    statement->key = key;

    // Parse values for each column
    statement->values = malloc(sizeof(void*) * schema->num_columns);
    
    for (uint32_t i = 0; i < schema->num_columns; i++) {
        Column* col = &schema->columns[i];
        char* value_string = strtok(NULL, " ");
        
        if (value_string == NULL) {
            return PREPARE_SYNTAX_ERROR;
        }

        if (col->type == COL_TYPE_INT) {
            statement->values[i] = malloc(sizeof(int32_t));
            *(int32_t*)statement->values[i] = atoi(value_string);
        } else if (col->type == COL_TYPE_TEXT) {
            statement->values[i] = malloc(col->size);
            strncpy((char*)statement->values[i], value_string, col->size - 1);
            ((char*)statement->values[i])[col->size - 1] = '\0';
        }
    }

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, 
                                Statement* statement, Schema* schema) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement, schema);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
    void* node = pager_get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    // Serialize row
    void* row_data = malloc(table->schema->row_size);
    serialize_row(table->schema, statement->values, row_data);

    // Find position and insert
    Cursor* cursor = table_find(table, statement->key);

    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = cursor_key(cursor);
        if (key_at_index == statement->key) {
            free(row_data);
            cursor_free(cursor);
            return EXECUTE_TABLE_FULL; // Duplicate key
        }
    }

    leaf_node_insert(cursor, statement->key, row_data);

    free(row_data);
    cursor_free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Cursor* cursor = table_start(table);

    while (!cursor->end_of_table) {
        void* row_data = cursor_value(cursor);
        void** values = deserialize_row(table->schema, row_data);
        
        printf("Key: %d | ", cursor_key(cursor));
        print_row(table->schema, values);

        // Free deserialized values
        for (uint32_t i = 0; i < table->schema->num_columns; i++) {
            free(values[i]);
        }
        free(values);

        cursor_advance(cursor);
    }

    cursor_free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table);
        default:
            return EXECUTE_SUCCESS;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];

    // Create a sample schema: id (int), name (text), age (int)
    Schema* schema = schema_create("users", 3);
    schema_add_column(schema, 0, "id", COL_TYPE_INT, 0);
    schema_add_column(schema, 1, "name", COL_TYPE_TEXT, 32);
    schema_add_column(schema, 2, "age", COL_TYPE_INT, 0);

    Table* table = table_create(filename, schema);

    InputBuffer* input_buffer = new_input_buffer();
    
    printf("B+Tree Database - Ready\n");
    printf("Schema: id (int), name (text), age (int)\n");
    printf("Commands:\n");
    printf("  insert <key> <id> <name> <age> - Insert a record\n");
    printf("  select - Display all records (range scan)\n");
    printf("  .btree - Show B+tree structure\n");
    printf("  .exit - Exit\n\n");

    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement, schema)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'.\n",
                       input_buffer->buffer);
                continue;
        }

        switch (execute_statement(&statement, table)) {
            case EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Error: Duplicate key or table full.\n");
                break;
        }

        // Free statement values
        if (statement.type == STATEMENT_INSERT && statement.values) {
            for (uint32_t i = 0; i < schema->num_columns; i++) {
                free(statement.values[i]);
            }
            free(statement.values);
        }
    }
}
