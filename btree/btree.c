#include "btree.h"
#include "pager.h"
#include <string.h>

#define MAX_KEYS 32

struct btree_page {
    uint8_t is_leaf;
    uint16_t num_keys;
    uint32_t keys[MAX_KEYS];
    uint32_t pointers[MAX_KEYS + 1];
};

static uint32_t root_page;

struct split_result {
    int did_split;
    uint32_t promoted_key;
    uint32_t right_page;
};

void btree_init() {
    root_page = pager_new_page();
    struct btree_page *root = pager_get_page(root_page);
    root->is_leaf = 1;
    root->num_keys = 0;
}

int btree_search(uint32_t key, uint32_t *value) {
    uint32_t page = root_page;

    while (1) {
        struct btree_page *node = pager_get_page(page);
        int i = 0;

        while (i < node->num_keys && key > node->keys[i])
            i++;

        if (node->is_leaf) {
            if (i < node->num_keys && node->keys[i] == key) {
                *value = node->pointers[i];
                return 1;
            }
            return 0;
        } else {
            page = node->pointers[i];
        }
    }
}

static void insert_into_node(
    struct btree_page *node,
    uint32_t key,
    uint32_t pointer
) {
    int i = node->num_keys - 1;
    while (i >= 0 && node->keys[i] > key) {
        node->keys[i + 1] = node->keys[i];
        node->pointers[i + 1] = node->pointers[i];
        i--;
    }
    node->keys[i + 1] = key;
    node->pointers[i + 1] = pointer;
    node->num_keys++;
}

static void insert_into_internal(
    struct btree_page *node,
    uint32_t key,
    uint32_t right_child
) {
    int i = node->num_keys;

    while (i > 0 && node->keys[i - 1] > key) {
        node->keys[i] = node->keys[i - 1];
        node->pointers[i + 1] = node->pointers[i];
        i--;
    }

    node->keys[i] = key;
    node->pointers[i + 1] = right_child;
    node->num_keys++;
}

static struct split_result split_node(uint32_t page_num) {
    struct btree_page *node = pager_get_page(page_num);
    uint32_t new_page = pager_new_page();
    struct btree_page *right = pager_get_page(new_page);

    right->is_leaf = node->is_leaf;

    int mid = node->num_keys / 2;

    struct split_result res = {
        .did_split = 1,
        .promoted_key = node->keys[mid],
        .right_page = new_page
    };

    right->num_keys = node->num_keys - mid - 1;

    memcpy(right->keys,
           &node->keys[mid + 1],
           right->num_keys * sizeof(uint32_t));

    memcpy(right->pointers,
           &node->pointers[mid + 1],
           (right->num_keys + 1) * sizeof(uint32_t));

    node->num_keys = mid;

    return res;
}


static struct split_result insert_recursive(
    uint32_t page_num,
    uint32_t key,
    uint32_t value
) {
    struct btree_page *node = pager_get_page(page_num);

    /* 1️⃣ Leaf */
    if (node->is_leaf) {
        insert_into_internal(node, key, value);

        if (node->num_keys < MAX_KEYS) {
            return (struct split_result){0};
        }
        return split_node(page_num);
    }

    /* 2️⃣ Internal node */
    int i = 0;
    while (i < node->num_keys && key > node->keys[i])
        i++;

    struct split_result child =
        insert_recursive(node->pointers[i], key, value);

    if (!child.did_split)
        return (struct split_result){0};

    insert_into_internal(
        node,
        child.promoted_key,
        child.right_page
    );

    if (node->num_keys < MAX_KEYS)
        return (struct split_result){0};

    return split_node(page_num);
}

void btree_insert(uint32_t key, uint32_t value) {
    struct split_result res =
        insert_recursive(root_page, key, value);

    if (res.did_split) {
        uint32_t new_root = pager_new_page();
        struct btree_page *root = pager_get_page(new_root);

        root->is_leaf = 0;
        root->num_keys = 1;
        root->keys[0] = res.promoted_key;
        root->pointers[0] = root_page;
        root->pointers[1] = res.right_page;

        root_page = new_root;
    }
}
