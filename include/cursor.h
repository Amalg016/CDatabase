#ifndef CURSOR_H
#define CURSOR_H

#include "table.h"

// Range cursor for optimized range scans
typedef struct {
    Cursor* start_cursor;
    Cursor* end_cursor;
    bool has_range;
} RangeCursor;

Cursor* create_cursor(Table* table, uint32_t page_num, uint32_t cell_num);
RangeCursor* create_range_cursor(Table* table, uint32_t start_key, uint32_t end_key);
void free_cursor(Cursor* cursor);
void free_range_cursor(RangeCursor* range_cursor);
bool range_cursor_has_next(RangeCursor* rcursor);
Row* range_cursor_next(RangeCursor* rcursor);

#endif
