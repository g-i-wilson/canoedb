# CanoeDB
Java database that converts a directory of .CSV files into a database.

- Relational: each CSV file becomes a table with relationships to other tables
- Auto-dereferencing: references between tables are dereferenced automatically
- Simple ID: left column of table is always the reference ID column
- Simple config: first 5 rows include: table name, column names, references, onRead functions, onWrite functions 

https://gabrielwilson3.github.io/
