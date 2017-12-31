# freeze-db-abstraction
A database access layer supporting automatic data transformation, "hard partitioning" (where identical copies of tables are created and then automatically selected during DML operations), and collection triggers (where when the aggregate of a column is changed, the changes are recorded to a callback function for caching use). It implements DBMS-native full text searching, along with all primary DDL and DML operations.  At present, MySQL, PostgreSQL, and SQL Server are supported.