#include "../include/table.h"
#include "../include/cursor.h"
#include "../include/btree.h"
#include <stdio.h>

void print_prompt() {
    printf("db > ");
}

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row* source, void* destination) {
    memcpy(destination, source, sizeof(Row));
}

void deserialize_row(void* source, Row* destination) {
    memcpy(destination, source, sizeof(Row));
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = db_open(filename);

    while (1) {
        print_prompt();
        
        char input[256];
        if (fgets(input, sizeof(input), stdin) == NULL) {
            printf("Error reading input\n");
            continue;
        }
        
        // Remove newline
        input[strcspn(input, "\n")] = 0;
        
        if (strcmp(input, ".exit") == 0) {
            db_close(table);
            printf("Database closed.\n");
            break;
        } else if (strncmp(input, "insert", 6) == 0) {
            Row row;
            if (sscanf(input, "insert %d %s %s", &row.id, row.username, row.email) != 3) {
                printf("Syntax: insert <id> <username> <email>\n");
                continue;
            }
            
            Cursor* cursor = table_find(table, row.id);
            if (cursor->cell_num < *leaf_node_num_cells(get_page(table->pager, cursor->page_num)) &&
                *leaf_node_key(get_page(table->pager, cursor->page_num), cursor->cell_num) == row.id) {
                printf("Error: Key %d already exists.\n", row.id);
                free(cursor);
                continue;
            }
            
            leaf_node_insert(cursor, row.id, &row);
            free(cursor);
            printf("Inserted row with id %d\n", row.id);
            
        } else if (strncmp(input, "select", 6) == 0) {
            if (strcmp(input, "select") == 0) {
                // Select all
                Cursor* cursor = table_start(table);
                while (!cursor->end_of_table) {
                    Row* row = (Row*)cursor_value(cursor);
                    print_row(row);
                    cursor_advance(cursor);
                }
                free(cursor);
            } else {
                // Select with range
                uint32_t start_key, end_key;
                if (sscanf(input, "select %d %d", &start_key, &end_key) == 2) {
                    // Optimized range scan using RangeCursor
                    RangeCursor* rcursor = create_range_cursor(table, start_key, end_key);
                    printf("Range scan from %d to %d:\n", start_key, end_key);
                    
                    while (range_cursor_has_next(rcursor)) {
                        Row* row = range_cursor_next(rcursor);
                        if (row) {
                            print_row(row);
                        }
                    }
                    
                    free_range_cursor(rcursor);
                } else {
                    // Select single
                    uint32_t key;
                    if (sscanf(input, "select %d", &key) == 1) {
                        Cursor* cursor = table_find(table, key);
                        if (cursor->cell_num < *leaf_node_num_cells(get_page(table->pager, cursor->page_num)) &&
                            *leaf_node_key(get_page(table->pager, cursor->page_num), cursor->cell_num) == key) {
                            Row* row = (Row*)cursor_value(cursor);
                            print_row(row);
                        } else {
                            printf("Key %d not found.\n", key);
                        }
                        free(cursor);
                    } else {
                        printf("Syntax: select [<key>] or select <start_key> <end_key>\n");
                    }
                }
            }
        } else if (strcmp(input, ".btree") == 0) {
            printf("Tree:\n");
            print_tree(table->pager, table->root_page_num, 0);
        } else if (strcmp(input, ".help") == 0) {
            printf("Commands:\n");
            printf("  insert <id> <username> <email>  - Insert a new record\n");
            printf("  select                         - Select all records\n");
            printf("  select <id>                    - Select record by id\n");
            printf("  select <start> <end>          - Range select (optimized)\n");
            printf("  .btree                        - Show B+Tree structure\n");
            printf("  .exit                         - Exit database\n");
            printf("  .help                         - Show this help\n");
        } else {
            printf("Unrecognized command: '%s'. Type .help for commands\n", input);
        }
    }

    return 0;
}
