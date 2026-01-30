#include "../include/btree.h"

NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t* leaf_node_num_cells(void* node) {
    return (uint32_t*)(node + LEAF_NODE_NUM_CELLS_OFFSET);
}

uint32_t* leaf_node_next_leaf(void* node) {
    return (uint32_t*)(node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return (uint32_t*)leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t* internal_node_num_keys(void* node) {
    return (uint32_t*)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t* internal_node_right_child(void* node) {
    return (uint32_t*)(node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
    return (uint32_t*)(node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
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

uint32_t* internal_node_key(void* node, uint32_t key_num) {
    return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}

void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    memcpy(leaf_node_value(node, cursor->cell_num), value, sizeof(Row));
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    // Create new leaf node
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);

    // Copy half of cells to new node
    uint32_t num_cells = *leaf_node_num_cells(old_node);
    uint32_t split_point = num_cells / 2;

    // Update next leaf pointers
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;

    // Move cells to new node
    for (uint32_t i = split_point; i < num_cells; i++) {
        uint32_t key_to_move = *leaf_node_key(old_node, i);
        void* value_to_move = leaf_node_value(old_node, i);
        *leaf_node_num_cells(new_node) += 1;
        *leaf_node_key(new_node, i - split_point) = key_to_move;
        memcpy(leaf_node_value(new_node, i - split_point), value_to_move, sizeof(Row));
    }

    // Update old node count
    *leaf_node_num_cells(old_node) = split_point;

    // Insert new cell in correct node
    uint32_t index_within_node = cursor->cell_num;
    void* destination_node;
    if (index_within_node >= split_point) {
        destination_node = new_node;
        cursor->page_num = new_page_num;
        cursor->cell_num = index_within_node - split_point;
    } else {
        destination_node = old_node;
    }

    uint32_t num_cells_dest = *leaf_node_num_cells(destination_node);
    if (cursor->cell_num < num_cells_dest) {
        for (uint32_t i = num_cells_dest; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(destination_node, i), 
                   leaf_node_cell(destination_node, i - 1), 
                   LEAF_NODE_CELL_SIZE);
        }
    }

    *leaf_node_num_cells(destination_node) += 1;
    *leaf_node_key(destination_node, cursor->cell_num) = key;
    memcpy(leaf_node_value(destination_node, cursor->cell_num), value, sizeof(Row));

    // Update parent or create new root
    if (is_node_root(old_node)) {
        create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = *((uint32_t*)(old_node + PARENT_POINTER_OFFSET));
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
    }
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;
    cursor->end_of_table = false;

    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            cursor->cell_num = index;
            return cursor;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor->cell_num = min_index;
    return cursor;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void* child = get_page(table->pager, child_num);
    
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
    }
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
    uint32_t num_keys = *internal_node_num_keys(node);
    
    // Binary search
    uint32_t min_index = 0;
    uint32_t max_index = num_keys;  // there is one more child than key
    
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

void create_new_root(Table* table, uint32_t right_child_page_num) {
    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child = get_page(table->pager, left_child_page_num);
    
    // Copy old root to left child
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);
    
    // Initialize root as internal node
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    
    // Set parent pointers
    *((uint32_t*)(left_child + PARENT_POINTER_OFFSET)) = table->root_page_num;
    *((uint32_t*)(right_child + PARENT_POINTER_OFFSET)) = table->root_page_num;
}

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
    void* parent = get_page(table->pager, parent_page_num);
    void* child = get_page(table->pager, child_page_num);
    uint32_t child_max_key = get_node_max_key(table->pager, child);
    uint32_t index = internal_node_find_child(parent, child_max_key);
    
    uint32_t original_num_keys = *internal_node_num_keys(parent);
    *internal_node_num_keys(parent) = original_num_keys + 1;
    
    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
        printf("Need to implement splitting internal node\n");
        exit(EXIT_FAILURE);
    }
    
    uint32_t right_child_page_num = *internal_node_right_child(parent);
    void* right_child = get_page(table->pager, right_child_page_num);
    
    // If child is being inserted to the right of all keys, update right child
    if (child_max_key > get_node_max_key(table->pager, right_child)) {
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) = get_node_max_key(table->pager, right_child);
        *internal_node_right_child(parent) = child_page_num;
    } else {
        // Make room for new cell
        for (uint32_t i = original_num_keys; i > index; i--) {
            void* destination = internal_node_cell(parent, i);
            void* source = internal_node_cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
    }
}

uint32_t get_node_max_key(Pager* pager, void* node) {
    if (get_node_type(node) == NODE_LEAF) {
        uint32_t num_cells = *leaf_node_num_cells(node);
        if (num_cells == 0) return 0;
        return *leaf_node_key(node, num_cells - 1);
    } else {
        void* right_child = get_page(pager, *internal_node_right_child(node));
        return get_node_max_key(pager, right_child);
    }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
    void* node = get_page(pager, page_num);
    uint32_t num_keys;
    
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            num_keys = *internal_node_num_keys(node);
            for (uint32_t i = 0; i < indentation_level; i++) {
                printf("  ");
            }
            printf("- internal (size %d)\n", num_keys);
            for (uint32_t i = 0; i < num_keys; i++) {
                uint32_t child_page_num = *internal_node_child(node, i);
                print_tree(pager, child_page_num, indentation_level + 1);
                
                for (uint32_t j = 0; j < indentation_level + 1; j++) {
                    printf("  ");
                }
                printf("- key %d\n", *internal_node_key(node, i));
            }
            uint32_t right_child_page_num = *internal_node_right_child(node);
            print_tree(pager, right_child_page_num, indentation_level + 1);
            break;
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
    }
}
