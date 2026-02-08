#ifndef PARSER_H
#define PARSER_H

#include "db.h"
#include "database.h"
#include <string.h>
#include <stdlib.h>
#include <strings.h>

// Input buffer structure
typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

// Statement types
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_CREATE_TABLE,
} StatementType;

// WHERE clause operator
typedef enum {
    OP_NONE,           // No WHERE clause
    OP_EQUAL,          // = 
    OP_GREATER,        // >
    OP_LESS,           // <
    OP_GREATER_EQUAL,  // >=
    OP_LESS_EQUAL,     // <=
    OP_BETWEEN,        // BETWEEN x AND y
} WhereOperator;

// Statement structure
typedef struct {
    StatementType type;
    char table_name[32];
    uint32_t key;
    void** values;
    // For CREATE TABLE
    uint32_t num_columns;
    Column* columns;
    // For SELECT
    char** select_columns;  // NULL means SELECT *
    uint32_t num_select_columns;
    // For WHERE clause
    WhereOperator where_op;
    char where_column[32];
    int32_t where_value;      // For =, >, <, >=, <=
    int32_t where_value2;     // For BETWEEN
} Statement;

// Prepare result
typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_TABLE_NOT_FOUND
} PrepareResult;

// Input buffer functions
static inline InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer =(InputBuffer*) malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

static inline void read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), 
                                  &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

static inline void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

// Parse CREATE TABLE statement
static inline PrepareResult prepare_create_table(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_CREATE_TABLE;
    
    char* keyword = strtok(input_buffer->buffer, " ");  // "create"
    keyword = strtok(NULL, " ");  // "table"
    if (!keyword || strcasecmp(keyword, "table") != 0) {
        return PREPARE_SYNTAX_ERROR;
    }
    
    char* table_name = strtok(NULL, " ");
    if (!table_name) {
        return PREPARE_SYNTAX_ERROR;
    }
    strncpy(statement->table_name, table_name, 31);
    statement->table_name[31] = '\0';
    
    char* num_cols_str = strtok(NULL, " ");
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

// Parse INSERT statement
static inline PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement, Database* db) {
    statement->type = STATEMENT_INSERT;
    
    char* token = strtok(input_buffer->buffer, " ");  // "insert"
    token = strtok(NULL, " ");  // "into" or table_name
    
    if (!token) {
        printf("Syntax: INSERT INTO <table> VALUES <val1> <val2> ...\n");
        printf("    or: INSERT <table> <val1> <val2> ...\n");
        return PREPARE_SYNTAX_ERROR;
    }
    
    // Check if next token is "into"
    if (strcasecmp(token, "into") == 0) {
        token = strtok(NULL, " ");  // Get table name
        if (!token) {
            return PREPARE_SYNTAX_ERROR;
        }
    }
    
    // token is now the table name
    strncpy(statement->table_name, token, 31);
    statement->table_name[31] = '\0';
    
    // Get the schema for this table
    Schema* schema = db_get_table(db, statement->table_name);
    if (!schema) {
        printf("Table '%s' not found\n", statement->table_name);
        return PREPARE_TABLE_NOT_FOUND;
    }
    
    // Skip "values" keyword if present
    token = strtok(NULL, " ");
    if (token && strcasecmp(token, "values") == 0) {
        token = strtok(NULL, " ");  // Get first value
    }
    
    // Parse values
    statement->values = (void**)malloc(sizeof(void*) * schema->num_columns);
    
    for (uint32_t i = 0; i < schema->num_columns; i++) {
        if (!token) {
            printf("Error: Not enough values. Expected %d columns\n", schema->num_columns);
            for (uint32_t j = 0; j < i; j++) {
                free(statement->values[j]);
            }
            free(statement->values);
            return PREPARE_SYNTAX_ERROR;
        }
        
        Column* col = &schema->columns[i];
        if (col->type == COL_TYPE_INT) {
            statement->values[i] = malloc(sizeof(int32_t));
            *(int32_t*)statement->values[i] = atoi(token);
        } else if (col->type == COL_TYPE_TEXT) {
            statement->values[i] = malloc(col->size);
            strncpy((char*)statement->values[i], token, col->size - 1);
            ((char*)statement->values[i])[col->size - 1] = '\0';
        }
        
        token = strtok(NULL, " ");
    }

    return PREPARE_SUCCESS;
}

// Parse SELECT statement
static inline PrepareResult prepare_select(InputBuffer* input_buffer, Statement* statement, Database* db) {
    statement->type = STATEMENT_SELECT;
    statement->select_columns = NULL;
    statement->num_select_columns = 0;
    
    // Make a copy of the buffer since strtok modifies it
    char buffer_copy[1024];
    strncpy(buffer_copy, input_buffer->buffer, 1023);
    buffer_copy[1023] = '\0';
    
    char* token = strtok(buffer_copy, " ");  // "select"
    token = strtok(NULL, " ");  // "*" or first column name
    
    if (!token) {
        printf("Syntax: SELECT * FROM <table>\n");
        printf("    or: SELECT <col1> <col2> ... FROM <table>\n");
        return PREPARE_SYNTAX_ERROR;
    }
    
    // Check if SELECT *
    if (strcmp(token, "*") == 0) {
        statement->select_columns = NULL;  // NULL means all columns
        token = strtok(NULL, " ");
    } else {
        // Parse column list - count how many columns before "from"
        uint32_t col_count = 0;
        char* count_token = token;
        while (count_token && strcasecmp(count_token, "from") != 0) {
            col_count++;
            count_token = strtok(NULL, " ");
        }
        
        if (col_count == 0) {
            printf("Error: No columns specified\n");
            return PREPARE_SYNTAX_ERROR;
        }
        
        // Allocate space for column names
        statement->select_columns = (char**)malloc(sizeof(char*) * col_count);
        statement->num_select_columns = col_count;
        
        // Parse again to get the columns
        strncpy(buffer_copy, input_buffer->buffer, 1023);
        buffer_copy[1023] = '\0';
        token = strtok(buffer_copy, " ");  // "select"
        token = strtok(NULL, " ");  // first column
        
        for (uint32_t i = 0; i < col_count; i++) {
            statement->select_columns[i] =(char*) malloc( 32);
            strncpy(statement->select_columns[i], token, 31);
            statement->select_columns[i][31] = '\0';
            token = strtok(NULL, " ");
        }
    }
    
    // Skip "from" keyword if present
    if (token && strcasecmp(token, "from") == 0) {
        token = strtok(NULL, " ");
    }
    
    if (!token) {
        printf("Error: Table name required\n");
        if (statement->select_columns) {
            for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                free(statement->select_columns[i]);
            }
            free(statement->select_columns);
        }
        return PREPARE_SYNTAX_ERROR;
    }
    
    // token is now the table name
    strncpy(statement->table_name, token, 31);
    statement->table_name[31] = '\0';
    
    // Verify table exists
    Schema* schema = db_get_table(db, statement->table_name);
    if (!schema) {
        printf("Table '%s' not found\n", statement->table_name);
        if (statement->select_columns) {
            for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                free(statement->select_columns[i]);
            }
            free(statement->select_columns);
        }
        return PREPARE_TABLE_NOT_FOUND;
    }
    
    // Verify all column names exist in schema
    if (statement->select_columns) {
        for (uint32_t i = 0; i < statement->num_select_columns; i++) {
            bool found = false;
            for (uint32_t j = 0; j < schema->num_columns; j++) {
                if (strcasecmp(statement->select_columns[i], schema->columns[j].name) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                printf("Error: Column '%s' not found in table '%s'\n", 
                       statement->select_columns[i], statement->table_name);
                for (uint32_t k = 0; k < statement->num_select_columns; k++) {
                    free(statement->select_columns[k]);
                }
                free(statement->select_columns);
                return PREPARE_SYNTAX_ERROR;
            }
        }
    }
    
    // Parse WHERE clause (optional)
    // Syntax: WHERE column op value
    // or: WHERE column BETWEEN value1 AND value2
    statement->where_op = OP_NONE;
    token = strtok(NULL, " ");
    
    if (token && strcasecmp(token, "where") == 0) {
        // Get column name
        token = strtok(NULL, " ");
        if (!token) {
            printf("Error: Expected column name after WHERE\n");
            if (statement->select_columns) {
                for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                    free(statement->select_columns[i]);
                }
                free(statement->select_columns);
            }
            return PREPARE_SYNTAX_ERROR;
        }
        
        strncpy(statement->where_column, token, 31);
        statement->where_column[31] = '\0';
        
        // Verify column exists
        bool col_found = false;
        for (uint32_t i = 0; i < schema->num_columns; i++) {
            if (strcasecmp(statement->where_column, schema->columns[i].name) == 0) {
                col_found = true;
                // Only support INT columns in WHERE for now
                if (schema->columns[i].type != COL_TYPE_INT) {
                    printf("Error: WHERE clause only supports INT columns\n");
                    if (statement->select_columns) {
                        for (uint32_t j = 0; j < statement->num_select_columns; j++) {
                            free(statement->select_columns[j]);
                        }
                        free(statement->select_columns);
                    }
                    return PREPARE_SYNTAX_ERROR;
                }
                break;
            }
        }
        if (!col_found) {
            printf("Error: Column '%s' not found in WHERE clause\n", statement->where_column);
            if (statement->select_columns) {
                for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                    free(statement->select_columns[i]);
                }
                free(statement->select_columns);
            }
            return PREPARE_SYNTAX_ERROR;
        }
        
        // Get operator
        token = strtok(NULL, " ");
        if (!token) {
            printf("Error: Expected operator after column name\n");
            if (statement->select_columns) {
                for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                    free(statement->select_columns[i]);
                }
                free(statement->select_columns);
            }
            return PREPARE_SYNTAX_ERROR;
        }
        
        if (strcmp(token, "=") == 0) {
            statement->where_op = OP_EQUAL;
        } else if (strcmp(token, ">") == 0) {
            statement->where_op = OP_GREATER;
        } else if (strcmp(token, "<") == 0) {
            statement->where_op = OP_LESS;
        } else if (strcmp(token, ">=") == 0) {
            statement->where_op = OP_GREATER_EQUAL;
        } else if (strcmp(token, "<=") == 0) {
            statement->where_op = OP_LESS_EQUAL;
        } else if (strcasecmp(token, "between") == 0) {
            statement->where_op = OP_BETWEEN;
        } else {
            printf("Error: Unknown operator '%s'. Supported: =, >, <, >=, <=, BETWEEN\n", token);
            if (statement->select_columns) {
                for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                    free(statement->select_columns[i]);
                }
                free(statement->select_columns);
            }
            return PREPARE_SYNTAX_ERROR;
        }
        
        // Get value(s)
        token = strtok(NULL, " ");
        if (!token) {
            printf("Error: Expected value after operator\n");
            if (statement->select_columns) {
                for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                    free(statement->select_columns[i]);
                }
                free(statement->select_columns);
            }
            return PREPARE_SYNTAX_ERROR;
        }
        
        statement->where_value = atoi(token);
        
        if (statement->where_op == OP_BETWEEN) {
            // Expect AND
            token = strtok(NULL, " ");
            if (!token || strcasecmp(token, "and") != 0) {
                printf("Error: Expected AND in BETWEEN clause\n");
                if (statement->select_columns) {
                    for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                        free(statement->select_columns[i]);
                    }
                    free(statement->select_columns);
                }
                return PREPARE_SYNTAX_ERROR;
            }
            
            // Get second value
            token = strtok(NULL, " ");
            if (!token) {
                printf("Error: Expected second value in BETWEEN clause\n");
                if (statement->select_columns) {
                    for (uint32_t i = 0; i < statement->num_select_columns; i++) {
                        free(statement->select_columns[i]);
                    }
                    free(statement->select_columns);
                }
                return PREPARE_SYNTAX_ERROR;
            }
            
            statement->where_value2 = atoi(token);
        }
    }
    
    return PREPARE_SUCCESS;
}

// Main prepare statement function
static inline PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement, Database* db) {
    if (strncasecmp(input_buffer->buffer, "create table", 12) == 0) {
        return prepare_create_table(input_buffer, statement);
    }
    if (strncasecmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement, db);
    }
    if (strncasecmp(input_buffer->buffer, "select", 6) == 0) {
        return prepare_select(input_buffer, statement, db);
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

// Free statement resources
static inline void free_statement(Statement* statement, Database* db) {
    if (statement->type == STATEMENT_INSERT && statement->values) {
        Schema* schema = db_get_table(db, statement->table_name);
        if (schema) {
            for (uint32_t i = 0; i < schema->num_columns; i++) {
                free(statement->values[i]);
            }
        }
        free(statement->values);
    }
    
    if (statement->type == STATEMENT_SELECT && statement->select_columns) {
        for (uint32_t i = 0; i < statement->num_select_columns; i++) {
            free(statement->select_columns[i]);
        }
        free(statement->select_columns);
    }
}

#endif // PARSER_H
