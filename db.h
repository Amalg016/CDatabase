#ifndef DB_H
#define DB_H

#include <stddef.h>
#include <stdlib.h>  // for malloc/free

int db_open(const char *filename);
void db_close();

int db_set(const char *key, const char *value);
char *db_get(const char *key);
int db_del(const char *key);

#endif
