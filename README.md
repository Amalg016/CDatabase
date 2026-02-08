# B+Tree Database

A compact, educational database engine written in C, demonstrating key concepts of relational databases using a disk-backed B+Tree data structure.

---
## Features

- **Multi-Table Support**: Create and manage multiple tables with named columns.
- **B+Tree Indexing**: Fast lookups, inserts, and range queries using a balanced B+Tree structure.
- **Disk-based Persistence**: Data is stored on disk in pages, supporting durability and efficient I/O.
- **Flexible Schemas**: Support for integer and fixed-size text columns, plus primary key constraints.
- **SQL-like CLI**: Interactive command shell for creating tables, inserting records, and querying data.
- **Meta-Commands**: View existing tables, inspect table B+Tree structure, and clean exit.

---
## Getting Started

### Build

Make sure you have `gcc` installed. Run:

```sh
make
```

This produces the executable: `bplus_db`

### Usage

To create or open a database:

```sh
./bplus_db mydatabase.db
```

You’ll enter the CLI prompt. Some commands:

```
CREATE TABLE users 3           # Create a new table with 3 columns
INSERT users 1 Alice 30        # Insert a row into 'users'
SELECT * FROM users            # Display all records in users
SELECT name FROM users WHERE id = 1  # Query with filter
.tables                        # List all tables
.btree users                   # Print the B+Tree structure for 'users'
.exit                          # Quit the CLI
```

---
## Directory Structure

- `src/`     — Source code for all subsystems: CLI, B+Tree, table, pager.
- `include/` — Header files for all modules.
- `Makefile` — Build system.

---
## Technical Overview
- **B+Tree Index:** Used for primary key and row organization.
- **Pager:** Loads/saves 4KB pages to disk, supporting a large database file.
- **Table/Schema Management:** Flexible table definitions—set column name/type/PK when created.
- **Row Serialization:** Efficient binary layout for storage/retrieval.
- **Write-Ahead Log (WAL):** For crash safety (if enabled).

---
## Example Session
```
$ ./bplus_db example.db
Multi-Table B+Tree Database with SQL-like syntax
Commands:
  CREATE TABLE <table> <num_columns>
  INSERT <table> <val1> ...
  SELECT * FROM <table>
  .tables
  .btree <table>
  .exit

db > CREATE TABLE users 3
Column 1: id int PRIMARY KEY
Column 2: name text 32
Column 3: age int
Table 'users' created successfully
PRIMARY KEY: id (fast lookups enabled)
db > INSERT users 1 Alice 30
Executed.
db > SELECT * FROM users
Key: 1 | id: 1, name: Alice, age: 30
(1 rows matched)
[Optimized: B+tree range scan]
db > .exit
```

---
## Learning Goals
- Understand B+Tree internals
- See how persistence, schemas, and indexes are built from scratch
- Appreciate the mechanics behind relational databases

---
## Author
This project is intended for learning and experimentation.


