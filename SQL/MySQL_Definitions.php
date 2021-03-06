<?php

namespace Database\SQL;

use Database\Engine;
use Database\Type;
use Database\Index;

/**
 * The conventions of all MySQL-language drivers.
 *
 * The following is a basic changelog of key MySQL features since 4.1, without common upgrade conversions, slave changes, and logging changes included:
 * * 4.1.0: Database, table, and column names are now stored UTF-8 (previously ASCII).
 * * 4.1.0: Binary values are treated as strings instead of numbers by default now. (use CAST())
 * * 4.1.0: DELETE statements no longer require that named tables be used instead of aliases (e.g. "DELETE t1 FROM test AS t1, test2 WHERE ..." previously had to be "DELETE test FROM test AS t1, test2 WHERE ...").
 * * 4.1.0: LIMIT can no longer be negative.
 * * 4.1.1: User-defined functions must contain xxx_clear().
 * * 4.1.2: When comparing strings, the shorter string will now be right-padded with spaces. Previously, spaces were truncated entirely. Indexes should be rebuilt as a result.
 * * 4.1.2: Float previously allowed higher values than standard. While previously FLOAT(3,1) could be 100.0, it now must not exceed 99.9.
 * * 4.1.2: When using "SHOW TABLE STATUS", the old column "type" is now "engine".
 * * 4.1.3: InnoDB indexes using latin1_swedish_ci from 4.1.2 and earlier should be rebuilt (using OPTIMIZE).
 * * 4.1.4: Tables with TIMESTAMP columns created between 4.1.0 and 4.1.3 must be rebuilt.
 * * 5.0.0: ISAM removed. Do not use. (To update, run "ALTER TABLE tbl_name ENGINE = MyISAM;")
 * * 5.0.0: RAID features in MySIAM removed.
 * * 5.0.0: User variables are not case sensitive in 5.0, but were prior.
 * * 5.0.2: "SHOW STATUS" behavior is global before 5.0.2 and sessional afterwards. 'SHOW [!50002 GLOBAL] STATUS;' can be used to trigger global in both.
 * * 5.0.2: NOT Parsing. Prior to 5.0.2, "NOT a BETWEEN b AND c" was parsed as "(NOT a) BETWEEN b AND ". Beginning in 5.0.2, it is parsed as "NOT (a BETWEEN b AND c)". The SQL mode "HIGH_NOT_PRECEDENCE" can be used to trigger the old mode. (http://dev.mysql.com/doc/refman/5.0/en/server-sql-mode.html#sqlmode_high_not_precedence).
 * * 5.0.3: User defined functions must contain aux. symbols in order to run.
 * * 5.0.3: The BIT Type. Prior to 5.0.3, the BIT type is a synonym for TINYINT(1). Beginning with 5.0.3, the BIT type accepts an additional argument (e.g. BIT(2)) that species the number of bits to use. (http://dev.mysql.com/doc/refman/5.0/en/bit-type.html)
 *   * Due to performance, BIT is far better than TINYINT(1). Both, however, are supported in this class.
 * * 5.0.3: Trailing spaces are not removed from VARCHAR and VARBINARY columns, but they were prior
 * * 5.0.3: Decimal handling was changed (tables created prior to the change will maintain the old behaviour): (http://dev.mysql.com/doc/refman/5.0/en/precision-math-decimal-changes.html)
 *   * Decimals are handled as binary; prior they were handled as strings.
 *   * When handled as strings, the "-" sign could be replaced with any number, extending the range of DECIMAL(5,2) from the current (and standard) [-999.99,999.99] to [-999.99,9999.99], while preceeding zeros and +/- signs were maintained when stored.
 *   * Additionally, prior to 5.0.3, the maximum number of digits is 264 (precise to ~15 depending on the host machine), from 5.0.3 to 5.0.5 it is 64 (precise to 64), and from 5.0.6 the maximum number of digits is 65 (precise to 65).
 *   * Finally, while prior to this change both exact- and approximate-value literals were handled as double-precision floating point, now exact-value literals will be handled as decimal.
 * * 5.0.6: Tables with DECIMAL columns created between 5.0.3 and 5.0.5 must be rebuilt.
 * * 5.0.8: "DATETIME+0" yields YYYYMMDDHHMMSS.000000, but previously yielded YYYYMMDDHHMMSS.
 * * 5.0.12: NOW() and SYSDATE() are no longer identical, with the latter be the time at script execution and the former at statement execution time (approximately).
 * * 5.0.13: The GREATEST() and LEAST() functions return NULL when a passed parameter is NULL. Prior, they ignored NULL values.
 * * 5.0.13: Substraction from an unsigned integer varies. Prior to 5.0.13, the bits of the subtracted value is used for the result (e.g. i-1, where i is TINYINT and 0, is the same as 0-2^64). In 5.0.13, it retains the bits of the original (e.g. it now would be 0-2^8). If comparing
 * * 5.0.15: The pad value for BINARY has changed from a space to \0, as has the handling of these. Using a BINARY(3) type with a value of 'a ' to illustrate: in the original, SELECT, DISTINCT, and ORDER BY operations remove all trailing spaces ('a'), while in the new version SELECT, DISTINCT, and ORDER BY maintain all additional null bytes ('a \0'). InnoDB still uses trailing spaces ('a  '), and did not remove the trailing spaces until 5.0.19 ('a').
 * * 5.0.15: CHAR() returns a binary string instead of a character set. A "USING" may be used instead to specify a character set. For instance, SELECT CHAR() returns a VARBINARY but previously would have returned VARCHAR (similarly, CHAR(ORD('A')) is equvilent to 'a' prior to this change, but now would only be so if a latin character set is specified.).
 * * 5.0.25: lc_time_names will affect the display of DATE_FORMAT(), DAYNAME(), and MONTHNAME().
 * * 5.0.42: When DATE and DATETIME interact, DATE is now converted to DATETIME with 00:00:00. Prior to 5.0.42, DATETIME would instead lose its time portion. CAST() can be used to mimic the old behavior.
 * * 5.0.50: Statesments containing "/*" without "*\/" are no longer accepted.
 * * 5.1.0: table_cache -> table_open_cache
 * * 5.1.0: "-", "*", "/", POW(), and EXP() now return NULL if an error is occured during floating-point operations. Previously, they may return "+INF", "-INF", or NaN.
 * * 5.1.23: In stored routines, a cursor may no longer be used in SHOW and DESCRIBE statements.
 * * 5.1.15: READ_ONLY
 * * Other incompatibilities that may be encountered:
 * * Reserved Words Added in 5.0: http://dev.mysql.com/doc/mysqld-version-reference/en/mysqld-version-reference-reservedwords-5-0.html.
 *   * This class puts everything in quotes to avoid this and related issues.
 *   * Some upgrades may require rebuilding indexes. We are not concerned with these, but a script that automatically rebuilds indexes as part of databaseSQL.php would have its merits. It could then also detect version upgrades.
 *   * Previously, TIMESTAMP(N) could specify a width of "N". It was ignored in 4.1, deprecated in 5.1, and removed in 5.5. Don't use it.
 *   * UDFs should use a database qualifier to avoid issues with defined functions.
 *   * The JOIN syntax was changed in MySQL 5.0.12. The new syntax will work with old versions, however (just not the other way around).
 *   * Avoid equals comparison with floating point values.
 *   * Timestamps are seriously weird in MySQL. Honestly, avoid them.
 *      * 4.1 especially contains oddities: (http://dev.mysql.com/doc/refman/4.1/en/timestamp.html)
 * * Further Reading: http://dev.mysql.com/doc/refman/5.0/en/upgrading-from-previous-series.html
 *
 * @package Database\SQL
 */
abstract class MySQL_Definitions extends SQL_Definitions
{
    public $tableQuoteStart = '`';
    public $tableQuoteEnd = '`';
    public $tableAliasQuoteStart = '`';
    public $tableAliasQuoteEnd = '`';
    public $columnQuoteStart = '`';
    public $columnQuoteEnd = '`';
    public $columnAliasQuoteStart = '`';
    public $columnAliasQuoteEnd = '`';
    public $databaseQuoteStart = '`';
    public $databaseQuoteEnd = '`';
    public $databaseAliasQuoteStart = '`';
    public $databaseAliasQuoteEnd = '`';
    public $indexQuoteStart = '`';
    public $indexQuoteEnd = '`';

    public $dataTypes = [
        'columnIntLimits' => [
            2         => 'TINYINT',
            4         => 'SMALLINT',
            7         => 'MEDIUMINT',
            9         => 'INT',
            'default' => 'BIGINT'
        ],

        'columnStringPermLimits' => [
            255          => 'CHAR',
            1000         => 'VARCHAR', // In MySQL, TEXT types are stored outside of the table. For searching purposes, we only use VARCHAR for relatively small values (I decided 1000 would be reasonable).
            65535        => 'TEXT',
            16777215     => 'MEDIUMTEXT',
            '4294967295' => 'LONGTEXT'
        ],

        'columnStringTempLimits' => [ // In MySQL, TEXT is not allowed in memory tables.
            255   => 'CHAR',
            65535 => 'VARCHAR'
        ],


        'columnBlobPermLimits' => [
            // In MySQL, BINARY values get right-padded. This is... difficult to work with, so we don't use it.
            1000         => 'VARBINARY',  // In MySQL, BLOB types are stored outside of the table. For searching purposes, we only use VARBLOB for relatively small values (I decided 1000 would be reasonable).
            65535        => 'BLOB',
            16777215     => 'MEDIUMBLOB',
            '4294967295' => 'LONGBLOB'
        ],

        'columnBlobTempLimits' => [ // In MySQL, BLOB is not allowed outside of
            65535 => 'VARBINARY'
        ],

        'columnNoLength' => [
            'MEDIUMTEXT', 'LONGTEXT',
            'MEDIUMBLOB', 'LONGBLOB',
        ],

        'columnBitLimits' => [
            8         => 'TINYINT UNSIGNED',
            16        => 'SMALLINT UNSIGNED',
            24        => 'MEDIUMINT UNSIGNED',
            32        => 'INTEGER UNSIGNED',
            64        => 'BIGINT UNSIGNED',
            'default' => 'INTEGER UNSIGNED',
        ],

        Type\Type::float     => 'REAL',
        Type\Type::bool      => 'BIT(1)',
        Type\Type::timestamp => 'INTEGER UNSIGNED',
        Type\Type::blob      => 'BLOB',
        Type\Type::json      => false,
    ];

    /**
     * @var bool MySQL does support a native bit() type that acts as we expect it to.
     */
    public $nativeBitfield = true;

    /**
     * @var string We enable MySQL's unique ON DUPLICATE KEY functionality. (This notably means that upserts are not entirely portable between MySQL and other DBs, though well-written ones that correctly specify the key constraints will work across DBMSs.)
     */
    public $upsertMode = 'onDuplicateKey';

    /**
     * @var string We enable MySQL's bog-standard ENUM() type.
     */
    public $enumMode = 'useEnum';

    /**
     * @var string We enable MySQL's COMMENT= tag on tables.
     */
    public $commentMode = 'useAttributes';

    /**
     * @var string We enable MySQL's INDEX tag on columns.
     */
    public $indexMode = 'useTableAttribute';

    /**
     * @var bool MySQL (well, InnoDB, at least) only supports either foreign keys or partioning. For performance, we use partioning.
     */
    public $foreignKeyMode = false;

    /**
     * @var bool We enable MySQL's PARTITION table attribute.
     */
    public $usePartition = true;

    /**
     * @var bool We enable MySQL's RENAME TABLE syntax.
     */
    public $tableRenameMode = 'renameTable';

    /**
     * @var bool We enable specifying the storage of indexes written during table creation.
     */
    public $indexStorageOnCreate = true;

    public $useCreateIfNotExist = true;

    public $tableTypes = [
        Engine::general => 'InnoDB',
        Engine::memory  => 'MEMORY',
    ];

    public $indexStorages = [
        Index\Storage::btree => 'BTREE',
        Index\Storage::hash  => 'HASH',
    ];

    public function getTablesAsArray(DatabaseSQL $database)
    {
        return $database->rawQueryReturningResult('SELECT * FROM '
            . $database->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE_TABLE, 'INFORMATION_SCHEMA', 'TABLES')
            . ' WHERE TABLE_SCHEMA = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
        )->getColumnValues('TABLE_NAME');
    }

    public function getTableColumnsAsArray(DatabaseSQL $database)
    {
        return $database->rawQueryReturningResult('SELECT * FROM '
            . $database->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE_TABLE, 'INFORMATION_SCHEMA', 'COLUMNS')
            . ' WHERE TABLE_SCHEMA = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
        )->getColumnValues(['TABLE_NAME', 'COLUMN_NAME']);
    }

    public function getTableConstraintsAsArray(DatabaseSQL $database)
    {
        return $database->rawQueryReturningResult('SELECT * FROM '
            . $database->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE_TABLE, 'INFORMATION_SCHEMA', 'KEY_COLUMN_USAGE')
            . ' WHERE TABLE_SCHEMA = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
            . ' AND REFERENCED_TABLE_NAME IS NOT NULL'
        )->getColumnValues(['TABLE_NAME', 'CONSTRAINT_NAME']);
    }

    public function getTableIndexesAsArray(DatabaseSQL $database)
    {
        return $database->rawQueryReturningResult('SELECT * FROM '
            . $database->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE_TABLE, 'INFORMATION_SCHEMA', 'STATISTICS')
            . ' WHERE TABLE_SCHEMA = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
        )->getColumnValues(['TABLE_NAME', 'INDEX_NAME']);
    }

    public function getLanguage()
    {
        return 'mysql';
    }

    /**
     * Use MySIAM instead of InnoDB on old versions of MySQL for FULLTEXT indexes
     */
    public function versionCheck()
    {
        if (floatval($this->getVersion()) < 5.6) {
            $tableTypes[Engine::general] = 'MySIAM';
        }
    }
}