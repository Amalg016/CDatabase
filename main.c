#include "btree/btree.h"
#include "btree/pager.h"
#include "db.h"
#include <stdio.h>
#include <string.h>

void stress_testing() {
    pager_open("btree.db");
    btree_init();

    for (int i = 1; i <= 100000; i++) {
        btree_insert(i, i * 10);
    }

    uint32_t value;
    for (int i = 1; i <= 100000; i++) {
        if (!btree_search(i, &value))
            printf("Missing %d\n", i);
    }

    pager_close();
}

int main() {

    db_open("mydb.data");
    char cmd[256], key[64], val[128];

    while (1) {
        printf("kvdb> ");
        scanf("%s", cmd);

        if (!strcmp(cmd, "SET")) {
            scanf("%s %s", key, val);
            db_set(key, val);
        } else if (!strcmp(cmd, "GET")) {
            scanf("%s", key);
            char *v = db_get(key);
            if (v) {
                printf("%s\n", v);
                free(v);
            } else {
                printf("NULL\n");
            }
        } else if (!strcmp(cmd, "EXIT")) {
            break;
        }
    }
    db_close();
    return 0;
}
