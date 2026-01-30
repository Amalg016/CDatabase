CC = gcc
CFLAGS = -Wall -Wextra -g -O2
TARGET = bplus_db
SOURCES = src/main.c src/pager.c src/btree.c src/table.c src/cursor.c
OBJECTS = $(SOURCES:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET) test.db

run: $(TARGET)
	./$(TARGET) test.db


