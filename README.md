Frozen Database Abstraction Layer
==========================
This is an advanced database abstraction layer developed alongside [FreezeMessenger](github.com/FreezeWarp/freeze-messenger/). It is powerful and flexible, with a number of features detailed below, though should still be considered "beta" and it's overall method format will likely be significantly overhauled in the future.


Some Database Principles
------------------------

The database abstraction layer follows a handful of basic database principles when making design decisions. These are:

-   In most software, data is written far less often than it is read. As a result, it can be tremendously beneficial to perform calculations when data is written, and not when it is read. (For instance, if you calculate slowFunction(columnA, columnB), store the result in columnC instead of calling slowFunction every time data is read.)

-   Data is most commonly stored on disk at rest. In order to avoid unnecessary disk arm movements, avoid reading columns from a row that may be stored on a separate disk sector. This includes TEXT and BLOB columns -- these will often be stored in a separate disk location from the main row struct. On the other hand, an aggregation that may span multiple disk sectors could be stored in a TEXT/BLOB column in a single row (e.g. userGroupMembers could be stored as caches both in users.memberOfGroups and groups.members). As this value will only be stored in a single disk location, and will possibly transfer less data, it is often preferable to reading the full aggregation. (In practice, however, most databases will optimise these better themselves, especially if proper indexing is used.)

-   Likewise, as data is most commonly stored on disk, avoid reading as much data as possible from the database. Plus, if you only query columns that are part of an index, you may be able to use the indexes alone for the query.

-   Memory tables, where supported, can be very, very fast. But be mindful of how memory tables work -- typically, every column will be fixed-length, so a VARCHAR(1000) will always take up 1000 bytes in every row, instead of being flexible.


Supported Drivers
-----------------

1.  MySQL

    1.  Primarily for testing purposes, the old __mysql__ driver is implemented, though it is not actively supported. It should be avoided as much as possible, in lieu of mysqli or pdoMysql.
    2.  __mysqli__ is the driver used primarily in development, and generally the best supported. In many cases, it is faster than other drivers. Note that it does not use parameterised queries, and relies on escape() to escape inputs.
    3.  __pdoMysql__ is the driver recommended for security-conscious individuals; all queries are fully parameterised. In practice, there is no reason to believe there is any injection potential in any driver (as the abstraction layer is itself parameterised), but if the abstraction layer fails to properly escape information, pdoMysql most likely won't. Note that pdoMysql will typically have higher memory usage than other drivers, because it must read in an entire result set before making it available for consumption.

2.  Postgres

    1.  __pgsql__ is a newer driver that supports postgres' LISTEN/NOTIFY functionality. It will typically be somewhat slower than mysql (due to lacking the memory tables used to ensure validation occurs quickly, and requiring certain data to be transformed after retrieval), and older versions (<9.5) will also not ensure full ACIDity, as the abstraction layer will emulate upsert in these versions by executing chained "IF SELECT() THEN UPDATE ELSE INSERT" queries (in the form of three separate queries).
    2.  __pdoPgsql__ is planned.

3.  SqlServer

    1.  __sqlsrv__ is experimentally available, and may work with most functionality at this time. Note, however, that only experienced DBAs should use FreezeMessenger with SqlServer (as many tables may need to be further optimised on a per-installation basis to maintain performance, and a fulltext storage object must first be created prior to using FreezeMessenger), and that, at this time, SqlServer is not guaranteed to be injection proof, due to the SqlServer driver not supporting parameterised queries on CREATE statements, and also not having any escape() function.
    2.  __pdoSqlsrv__ is planned.


Language-Specific Notes
-----------------------

-   At present, MySQL supports both foreign keys and partitions, but not simultaneously (at least in InnoDB). As such, foreign key support is disabled in favour of partitions on MySQL.
-   Only MySQL uses partitioning. Postgres has no partitioning functionality, and SqlServer's is unimplemented. Only MySQL supports database creation. The user must manually create a database prior to using Postgres or SqlServer.
-   Only MySQL supports automatically setting the table charset to UTF-8. Postgre's charsets are per-database, and thus must be set by the user.
-   For memory tables, MySQL's MEMORY engine is used, while Postgre's UNLOGGED table attribute is used. While SqlServer supports memory-optimized tables, they are unimplemented.
-   MySQL will use MySIAM on versions < 5.6, as InnoDB did not have FULLTEXT capabilities in these versions. It will use InnoDB on versions >= 5.6.


Why?
----

FreezeMessenger has no SQL-like commands. Every call is implemented with structured arrays that are converted into SQL-like commands. This has various benefits:

-   Easy portability between database backends, including those that don't use SQL-like syntax.

-   Easy on-demand data conversion: while rare, it is occasionally quite helpful to be able to radically transform data right before it is sent to the database. For instance, the hexadecimal-encoded roomIds (described above) are only converted to this space-saving format by the database access layer, right before it includes the values in INSERT, UPDATE, or WHERE clauses. This can make porting tremendously easier, to, for instance, convert all binary data to base64.

-   Type-safety: by simple virtue of delimiting data with an array data structure (instead of composing strings of delimited data), it is much more difficult to accidentally introduce exploits. Similarly, comparisons are fairly explicit; an example WHERE clause of (`userName` = "bob" AND `age` > 50 AND `height` > `weight`) is encoded as ["userName" => "bob", "age" => $db->int(50, "gt"), "height" => $db->column("weight", "gt")]. And modifying a column to include an exploit is essentially impossible here -- say `height` changed to `` AND (DROP TABLE) AND ``. Our database access layer will insist on ensuring that this were encoded as ``` AND (DROP TABLE) AND ```, which would not be valid. (It is, in-fact, more restrictive than this in-practice, forbidding most non-alphanumeric characters entirely, as well as enforcing a maximum column length.) TODO: wait, does it still do that? Make sure it still does that. It's a good idea I may have removed in one of the rewrites.


Thorough Database Definition Language
-------------------------------------

### Standard Indexes
The DAL exposes the following functionality during index creation:

1. Index Type: Users can specify whether an index should be primary, unique, standard, or a __fulltext__ index. All currently supported DBMSes support all four types.
2. Index Storage: Users can specify whether an index should be stored as a btree or a hash, based on its type. On MySQL and PostgreSQL > 10.0, the DAL will ask the DBMS to use this storage.


### Updating Indexes
Indexes are named in a consistent format across database engines, which ensures that indexes can be updated after the initial table definition.


### Documenting Your Tables
Tables, columns, and indexes all support comments during table creation.


### Table Engine
The DAL exposes the ability to select a memory or general table engine during table creation.

- On MySQL, the MEMORY table engine will be used if "memory" is specified. Either InnoDB or MySIAM (depending on the version of MySQL) will be used if "general" is specified.
- On PostgreSQL, if "memory" is specified then UNLOGGED tables will created.


Query Joining / Queueing
------------------------

The DAL is capable of (experimental) query-joining through the queue system: either by invoking autoQueue or using the queueInsert, queueDelete, and queueUpdate, queries can be stored and then executed as a group. When the execution happens, similar queries (e.g. multiple updates to the same row of the same table) will be executed as a single query instead of multiple. Right now, this is limited, and only works in the following situations:

1.  Multiple deletions to the same table with different conditions can be combined into a single query by ORing the list of conditions. For instance, queueDelete("tableName", [id => 3]) and queueDelete("tableName", [name => "Bob"]) will be executed as delete("tableName", ["either" => [[id => 3], [name => "Bob"]]), which is a single SQL query: DELETE FROM tableName WHERE id = 3 OR name = "Bob".

2.  Multiple updates to the same table based on the same selection criteria can be combined into a single criteria by merging the update parameters. For instance:

    1.  `queueUpdate("tableName", [name => Bob], [id => 1])` and
    2.  `queueUpdate("tableName", [address => "123 Downing St"], [id => 1])` can be combined into
    3.  `update("tableName", [name => Bob, address => "123 Downing St"], [id => 1])`

3.  At present, multiple insertions are not combined in any way, as the SQL backend is currently incapable of inserting multiple rows at once. This may change in the future.


Transactions
------------

While advanced transactions may or may not fully work (depending on the backend), at least one level of transactions should always be supported. A transaction can be initiated by invoking startTransaction, and it can be finished by invoking endTransaction. The DAL makes no assurances as to what state data is stored in before a transaction is complete, merely that all changes made in a transaction are reversible.

To reverse a transaction, invoke rollbackTransaction. This will be automatically invoked if a query error occurs, however it may not be invoked if the DAL throws an exception. This should be fixed in the future.


Automatic Data Transformation
-----------------------------

Data can be automatically encoded for storage, and decoded on retrieval, by configuring the following DAL directives:

-   __encode__ - An array of table columns to transform on send and retrieval. Formatted as [tableName => [columnName => [encodeFunction, encodeType, decodeFunction], ...], ...]
-   __encodeCopy__ - An array of table columns that should be copied to a secondary column and encoded. No decode will occur. Formatted as [tableName => [columnName => [encodeFunction, encodeType, columnCopyName], ...], ...]
-   __insertIdColumns__ - This merely specifies which columns are returned by the insert ID (that is, are autoincremented on insert). It only needs to be specified if it is a column copied in encodeCopy.

Encoding will happen in the following situations:

-   Affected columns are updated/upserted.
-   Affected columns are inserted/upserted.

Copy-and-encoding will happen in the following situations:

-   Affected columns are updated/upserted.
-   Affected columns are inserted/upserted.
-   Affected columns are automatically generated on insert. (Must specify in insertIdColumns)

Decoding will happen in the following situations:

-   Affected columns are retrieved by select()

Performance-wise, automatic data transformation is reasonably quick, as it uses isset to check for the existence of transformation directives. Of-course, it must perform this check for every single column returned, but in general this should not result in an appreciable performance decrease.


Triggers
--------

Most DBMS software supports triggers of some form, but they are generally quite difficult to express in a standardised way, with each implementation suffering from their own limitations. As such, the database access layer implements a very basic, singular trigger for ON CHANGE, which can be registered to any table. When the data in that table changes, this trigger will fire. The trigger is a PHP function, meaning it can execute any PHP code, but is principally used for maintaining list caches in columns.

Triggers should only fire once when operations are correctly queued.

Triggers affecting the row being INSERT/UPDATE/DELETEd are partially possible using data transformation.


Query Logger (SQL Backend)
--------------------------

If a log file is specified to the DatabaseSQL::queryLogToFile property, a log of all queries will be written on termination of the object. This is a good way to profile any application using the DAL, as the full SQL query and the time it took to execute will both be recorded in the log file.

On FreezeMessenger, setting the logQueriesFile directive to a fully-resolved filename (e.g. /var/log/fm-querylog) will activate the DAL log.
