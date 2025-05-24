# SQLite
## Introduction

This is a recreation of SQLite using C that aims to create a lightweight database system that flushes to the disk after closing the database. The current approach uses a b+ tree setup to efficently search and store based on ID. Right now it is only able to create a table with 3 paramters (ID,USER,EMAIL) but in following commits it will be able to create a table with any number of parameters.

## Current Features

Currently there are two main commands that can be used with this SQLite recreation:

- `INSERT (ID, USER, EMAIL)`: This inserts a new row into the table with the given parameters.
- `SELECT`: This shows you the current table

And there are 3 meta commands:

- `.exit`: This exits the program
- `.btree`: This will show the current layout of the btree model
- `.constants` : This will show the current constants used in the program

## Roadmap

The next features to be implemented are:

- `CREATE TABLE`: This will create a new table with the given parameters.
- `DROP TABLE`: This will delete the table.
- `UPDATE`: This will update a row in the table.
- `DELETE`: This will delete a row from the table.
- `SELECT *`: This will show all rows in the table.
- `SELECT (***)`: This will show only the given column.



