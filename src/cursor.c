#include "../include/cursor.h"
#include "../include/btree.h"

Cursor* create_cursor(Table* table, uint32_t page_num, uint32_t cell_num) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;
    cursor->cell_num = cell_num;
    
    void* node = get_page(table->pager, page_num);
    if (get_node_type(node) == NODE_LEAF) {
        uint32_t num_cells = *leaf_node_num_cells(node);
        cursor->end_of_table = (cell_num >= num_cells);
    } else {
        cursor->end_of_table = true;
    }
    
    return cursor;
}

RangeCursor* create_range_cursor(Table* table, uint32_t start_key, uint32_t end_key) {
    RangeCursor* rcursor = malloc(sizeof(RangeCursor));
    
    // Find start cursor
    Cursor* start_cursor = table_find(table, start_key);
    if (start_cursor == NULL) {
        // If start_key doesn't exist, start from the beginning
        start_cursor = table_start(table);
    }
    
    // Find end cursor
    Cursor* end_cursor = table_find(table, end_key);
    if (end_cursor == NULL) {
        // If end_key doesn't exist, end at the last element
        end_cursor = table_start(table);
        while (!end_cursor->end_of_table) {
            cursor_advance(end_cursor);
        }
    }
    
    rcursor->start_cursor = start_cursor;
    rcursor->end_cursor = end_cursor;
    rcursor->has_range = true;
    
    return rcursor;
}

void free_cursor(Cursor* cursor) {
    free(cursor);
}

void free_range_cursor(RangeCursor* rcursor) {
    if (rcursor->start_cursor) free_cursor(rcursor->start_cursor);
    if (rcursor->end_cursor) free_cursor(rcursor->end_cursor);
    free(rcursor);
}

bool range_cursor_has_next(RangeCursor* rcursor) {
    if (!rcursor->has_range || rcursor->start_cursor->end_of_table) {
        return false;
    }
    
    // Check if we've reached or passed the end cursor
    if (rcursor->start_cursor->page_num == rcursor->end_cursor->page_num &&
        rcursor->start_cursor->cell_num >= rcursor->end_cursor->cell_num) {
        return false;
    }
    
    return true;
}

Row* range_cursor_next(RangeCursor* rcursor) {
    if (!range_cursor_has_next(rcursor)) {
        return NULL;
    }
    
    void* value = cursor_value(rcursor->start_cursor);
    cursor_advance(rcursor->start_cursor);
    
    return (Row*)value;
}
