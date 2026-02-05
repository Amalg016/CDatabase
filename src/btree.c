#include "../include/btree.h"

// Get node type
NodeType get_node_type(void *node) {
  uint8_t value = *((uint8_t *)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

void set_node_type(void *node, NodeType type) {
  uint8_t value = type;
  *((uint8_t *)(node + NODE_TYPE_OFFSET)) = value;
}

// Root node operations
bool is_node_root(void *node) {
  uint8_t value = *((uint8_t *)(node + IS_ROOT_OFFSET));
  return (bool)value;
}

void set_node_root(void *node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t *)(node + IS_ROOT_OFFSET)) = value;
}

// Parent pointer operations
uint32_t *node_parent(void *node) {
  return (uint32_t *)(node + PARENT_POINTER_OFFSET);
}

// Leaf node operations
uint32_t *leaf_node_num_cells(void *node) {
  return (uint32_t *)(node + LEAF_NODE_NUM_CELLS_OFFSET);
}

uint32_t *leaf_node_next_leaf(void *node) {
  return (uint32_t *)(node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

void *leaf_node_cell(void *node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t *leaf_node_key(void *node, uint32_t cell_num) {
  return (uint32_t *)leaf_node_cell(node, cell_num);
}

void *leaf_node_value(void *node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void *node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0; // 0 represents no sibling
}

// Internal node operations
uint32_t *internal_node_num_keys(void *node) {
  return (uint32_t *)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t *internal_node_right_child(void *node) {
  return (uint32_t *)(node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t *internal_node_cell(void *node, uint32_t cell_num) {
  return (uint32_t *)(node + INTERNAL_NODE_HEADER_SIZE +
                      cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t *internal_node_child(void *node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    return internal_node_right_child(node);
  } else {
    return internal_node_cell(node, child_num);
  }
}

uint32_t *internal_node_key(void *node, uint32_t key_num) {
  return (uint32_t *)((void *)internal_node_cell(node, key_num) +
                      INTERNAL_NODE_CHILD_SIZE);
}

void initialize_internal_node(void *node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
}

// Find the position where a key should be inserted in a leaf node
uint32_t leaf_node_find(void *node, uint32_t key) {
  uint32_t num_cells = *leaf_node_num_cells(node);
  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;

  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key == key_at_index) {
      return index;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

// Find child in internal node
uint32_t internal_node_find_child(void *node, uint32_t key) {
  uint32_t num_keys = *internal_node_num_keys(node);

  uint32_t min_index = 0;
  uint32_t max_index = num_keys;

  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *internal_node_key(node, index);
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

// Get the maximum key in a node (recursive)
uint32_t get_node_max_key(Pager *pager, void *node) {
  if (get_node_type(node) == NODE_LEAF) {
    uint32_t num_cells = *leaf_node_num_cells(node);
    return *leaf_node_key(node, num_cells - 1);
  }
  void *right_child = pager_get_page(pager, *internal_node_right_child(node));
  return get_node_max_key(pager, right_child);
}

void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level) {
  void *node = pager_get_page(pager, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
  case NODE_LEAF:
    num_keys = *leaf_node_num_cells(node);
    for (uint32_t i = 0; i < indentation_level; i++) {
      printf("  ");
    }
    printf("- leaf (size %d)\n", num_keys);
    for (uint32_t i = 0; i < num_keys; i++) {
      for (uint32_t j = 0; j < indentation_level + 1; j++) {
        printf("  ");
      }
      printf("- %d\n", *leaf_node_key(node, i));
    }
    break;
  case NODE_INTERNAL:
    num_keys = *internal_node_num_keys(node);
    for (uint32_t i = 0; i < indentation_level; i++) {
      printf("  ");
    }
    printf("- internal (size %d)\n", num_keys);
    for (uint32_t i = 0; i < num_keys; i++) {
      child = *internal_node_child(node, i);
      print_tree(pager, child, indentation_level + 1);

      for (uint32_t j = 0; j < indentation_level + 1; j++) {
        printf("  ");
      }
      printf("- key %d\n", *internal_node_key(node, i));
    }
    child = *internal_node_right_child(node);
    print_tree(pager, child, indentation_level + 1);
    break;
  }
}
