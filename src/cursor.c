#include "../include/cursor.h"

Cursor *table_start(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->schema->root_page_num;
  cursor->cell_num = 0;

  void *root_node = pager_get_page(table->pager, table->schema->root_page_num);

  while (get_node_type(root_node) == NODE_INTERNAL) {
    cursor->page_num = *internal_node_child(root_node, 0);
    root_node = pager_get_page(table->pager, cursor->page_num);
  }

  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

Cursor *table_find(Table *table, uint32_t key) {
  void *root_node = pager_get_page(table->pager, table->schema->root_page_num);

  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;

  if (get_node_type(root_node) == NODE_LEAF) {
    cursor->page_num = table->schema->root_page_num;
    cursor->cell_num = leaf_node_find(root_node, key);
    cursor->end_of_table = false;
    return cursor;
  }

  uint32_t page_num = table->schema->root_page_num;
  while (true) {
    void *node = pager_get_page(table->pager, page_num);
    if (get_node_type(node) == NODE_LEAF) {
      cursor->page_num = page_num;
      cursor->cell_num = leaf_node_find(node, key);
      cursor->end_of_table = false;
      return cursor;
    }

    uint32_t child_index = internal_node_find_child(node, key);
    page_num = *internal_node_child(node, child_index);
  }
}

void *cursor_value(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *page = pager_get_page(cursor->table->pager, page_num);
  return leaf_node_value(page, cursor->cell_num);
}

uint32_t cursor_key(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *page = pager_get_page(cursor->table->pager, page_num);
  return *leaf_node_key(page, cursor->cell_num);
}

void cursor_advance(Cursor *cursor) {
  uint32_t page_num = cursor->page_num;
  void *node = pager_get_page(cursor->table->pager, page_num);

  cursor->cell_num += 1;
  if (cursor->cell_num >= *leaf_node_num_cells(node)) {
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
}

void cursor_free(Cursor *cursor) { free(cursor); }

void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, void *value,
                                uint32_t row_size);

void leaf_node_insert(Cursor *cursor, uint32_t key, void *value,
                      uint32_t row_size) {
  void *node = pager_get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    leaf_node_split_and_insert(cursor, key, value, row_size);
    return;
  }

  if (cursor->cell_num < num_cells) {
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  memcpy(leaf_node_value(node, cursor->cell_num), value, row_size);
}
void create_new_root(Table *table, uint32_t root_page_num,
                     uint32_t right_child_page_num) {
  void *root = pager_get_page(table->pager, root_page_num);
  void *right_child = pager_get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = pager_allocate_page(table->pager);
  void *left_child = pager_get_page(table->pager, left_child_page_num);

  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;
  uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = root_page_num;
  *node_parent(right_child) = root_page_num;
}

void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, void *value,
                                uint32_t row_size) {
  void *old_node = pager_get_page(cursor->table->pager, cursor->page_num);
  uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);
  uint32_t new_page_num = pager_allocate_page(cursor->table->pager);
  void *new_node = pager_get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  uint32_t split_index = (LEAF_NODE_MAX_CELLS + 1) / 2;

  for (uint32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    void *destination_node;
    uint32_t index_within_node;

    if (i >= split_index) {
      destination_node = new_node;
      index_within_node = i - split_index;
    } else {
      destination_node = old_node;
      index_within_node = i;
    }

    void *destination = leaf_node_cell(destination_node, index_within_node);

    if (i == cursor->cell_num) {
      *(uint32_t *)(destination) = key;
      memcpy(destination + LEAF_NODE_KEY_SIZE, value, row_size);
    } else if (i > cursor->cell_num) {
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }

    if (i == 0)
      break;
  }

  *(leaf_node_num_cells(old_node)) = split_index;
  *(leaf_node_num_cells(new_node)) = (LEAF_NODE_MAX_CELLS + 1) - split_index;

  if (is_node_root(old_node)) {
    return create_new_root(cursor->table, cursor->page_num, new_page_num);
  } else {
    uint32_t parent_page_num = *node_parent(old_node);
    uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
    void *parent = pager_get_page(cursor->table->pager, parent_page_num);

    update_internal_node_key(parent, old_max, new_max);
    internal_node_insert(cursor->table, parent_page_num, new_page_num);
  }
}

void internal_node_insert(Table *table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
  void *parent = pager_get_page(table->pager, parent_page_num);
  void *child = pager_get_page(table->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(table->pager, child);
  uint32_t index = internal_node_find_child(parent, child_max_key);

  uint32_t original_num_keys = *internal_node_num_keys(parent);
  *internal_node_num_keys(parent) = original_num_keys + 1;

  if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
    printf("Need to implement splitting internal node\n");
    exit(EXIT_FAILURE);
  }

  uint32_t right_child_page_num = *internal_node_right_child(parent);
  void *right_child = pager_get_page(table->pager, right_child_page_num);

  if (child_max_key > get_node_max_key(table->pager, right_child)) {
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) =
        get_node_max_key(table->pager, right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    for (uint32_t i = original_num_keys; i > index; i--) {
      void *destination = internal_node_cell(parent, i);
      void *source = internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
}
