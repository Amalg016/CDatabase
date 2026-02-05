CC = gcc
CFLAGS = -Wall -Wextra -g -O2 -Isrc
TARGET = bplus_db
SOURCES = src/main.c src/pager.c src/btree.c src/table.c src/cursor.c src/database.c
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


test: $(TARGET)
	@echo "=== Testing B+Tree Database ==="
	@echo "Creating database with sample data..."
	@rm -f test.db 2>/dev/null || true
	@./$(TARGET) test.db create < test_input.txt

.PHONY: all clean run test

