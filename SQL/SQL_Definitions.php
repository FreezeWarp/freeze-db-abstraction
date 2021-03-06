<?php

namespace Database\SQL;

use Database\ResultInterface;
use Database\Index;
use Database\Engine;
use Database\Type;
use Database\Type\Comparison;

/**
 * The expected conventions of a typical SQL driver.
 *
 * @package Database\SQL
 */
abstract class SQL_Definitions implements DriverInterface
{

    /*********************************************************
     ************************ START **************************
     ***************** Query Format Constants ****************
     *********************************************************/

    /**
     * @var string The token that comes before database names.
     */
    public $databaseQuoteStart = '"';

    /**
     * @var string The token that comes after database names.
     */
    public $databaseQuoteEnd = '"';

    /**
     * @var string The token that comes before database aliases.
     */
    public $databaseAliasQuoteStart = '"';

    /**
     * @var string The token that comes after database aliases.
     */
    public $databaseAliasQuoteEnd = '"';

    /**
     * @var string The token that comes before table names.
     */
    public $tableQuoteStart = '"';

    /**
     * @var string The token that comes after table names.
     */
    public $tableQuoteEnd = '"';

    /**
     * @var string The token that comes before table aliases.
     */
    public $tableAliasQuoteStart = '"';

    /**
     * @var string The token that comes after table aliases.
     */
    public $tableAliasQuoteEnd = '"';

    /**
     * @var string The token that comes before column names.
     */
    public $columnQuoteStart = '"';

    /**
     * @var string The token that comes after column names.
     */
    public $columnQuoteEnd = '"';

    /**
     * @var string The token that comes before column aliases.
     */
    public $columnAliasQuoteStart = '"';

    /**
     * @var string The token that comes after column aliases.
     */
    public $columnAliasQuoteEnd = '"';

    /**
     * @var string The token that comes before an index.
     */
    public $indexQuoteStart = '"';

    /**
     * @var string The token that comes after an index.
     */
    public $indexQuoteEnd = '"';

    /**
     * @var string The token that comes before strings.
     */
    public $stringQuoteStart = '\'';

    /**
     * @var string The token that comes after strings.
     */
    public $stringQuoteEnd = '\'';

    /**
     * @var string The wildcard token when used in strings in LIKE clauses.
     */
    public $stringFuzzy = '%';

    /**
     * @var string The token that comes before binary blobs.
     */
    public $binaryQuoteStart = '\'';

    /**
     * @var string The token that comes after binary blobs.
     */
    public $binaryQuoteEnd = '\'';

    /**
     * @var string The token that comes before arrays.
     */
    public $arrayQuoteStart = '(';

    /**
     * @var string The token that comes after arrays.
     */
    public $arrayQuoteEnd = ')';

    /**
     * @var string The token that comes between array elements.
     */
    public $arraySeperator = ', ';

    /**
     * @var string The token that comes between statements.
     */
    public $statementSeperator = ', ';

    /**
     * @var string The token that comes before ints.
     */
    public $intQuoteStart = '';

    /**
     * @var string The token that comes after ints.
     */
    public $intQuoteEnd = '';

    /**
     * @var string The token that comes before floats.
     */
    public $floatQuoteStart = '';

    /**
     * @var string The token that comes after floats.
     */
    public $floatQuoteEnd = '';

    /**
     * @var string The token that comes before timestamps.
     */
    public $timestampQuoteStart = '';

    /**
     * @var string The token that comes after timestamps.
     */
    public $timestampQuoteEnd = '';

    /**
     * @var string The token that comes between a database name and a column name.
     */
    public $databaseTableDivider = '.';

    /**
     * @var string The token that comes between a table name and a column name.
     */
    public $tableColumnDivider = '.';

    /**
     * @var string The token that designates ascending order.
     */
    public $sortOrderAsc = 'ASC';

    /**
     * @var string The token that designates descending order.
     */
    public $sortOrderDesc = 'DESC';

    /**
     * @var string The token that comes between a table name and a table alias.
     */
    public $tableAliasDivider = ' AS ';

    /**
     * @var string The token that comes between a column name and a column alias.
     */
    public $columnAliasDivider = ' AS ';

    /**
     * @var array The tokens corresponding to DatabaseTypeComparison enumerations.
     */
    public $comparisonTypes = [
        Comparison::equals            => '=',
        Comparison::assignment        => '=',
        Comparison::in                => 'IN',
        Comparison::notin             => 'NOT IN',
        Comparison::lessThan          => '<',
        Comparison::lessThanEquals    => '<=',
        Comparison::greaterThan       => '>',
        Comparison::greaterThanEquals => '>=',
        Comparison::search            => 'LIKE',
        Comparison::binaryAnd         => '&',
    ];

    /**
     * @var array The tokens corresponding with 'both' and 'either' concatenations.
     */
    public $concatTypes = [
        'both' => ' AND ', 'either' => ' OR ', 'not' => ' NOT '
    ];



    /*********************************************************
     ************************ START **************************
     ****************** Misc. Information ********************
     *********************************************************/

    /**
     * @var array The phrases that identify the three supported key types, 'primary', 'unique', and 'index'
     */
    public $keyTypeConstants = [
        Index\Type::fulltext => 'FULLTEXT',
        Index\Type::primary  => 'PRIMARY',
        Index\Type::unique   => 'UNIQUE',
        Index\Type::index    => '',
    ];

    /**
     * @var array The phrases that correspond with the supported default phrases, currently only '__TIME__'
     */
    public $defaultPhrases = [
        '__TIME__' => 'CURRENT_TIMESTAMP',
    ];

    /**
     * @var array A list of distinct DB engine classifications the DBMS supports.
     *   'memory' is an engine that stores all or most of its data in memory, and whose data may be lost on restart
     *   'general' is an engine that stores all or most of its data on disk, and which supports transactions, permanence, and so-on.
     */
    public $storeTypes = [Engine::memory, Engine::general];

    /*
     * todo: remove (replace with storeTypes)
     */
    public $tableTypes = [Engine::memory, Engine::general];

    /**
     * @var array Various datatype information. This information should only be needed when creating and altering table columns.
     */
    public $dataTypes = [];

    /**
     * @var array The tokens to use for different index types. If empty, index types will not be specified on creation.
     */
    public $indexStorages = [];

    /**
     * @var array The values that should be used for boolean "true" and "false".
     */
    public $boolValues = [
        true => 1, false => 0,
    ];



    /*********************************************************
     ************************ START **************************
     ***************** SQL Function Support ******************
     *********************************************************/

    /**
     * @var bool If native bitfields are supported.
     *   true - use native BIT(length) type.
     *   false - simulate with integers
     */
    public $nativeBitfield = false;

    /**
     * @var bool Whether or not IF NOT EXISTS is supported in CREATE DATABASE/TABLE statements.
     */
    public $useCreateIfNotExist = false;

    /**
     * @var bool Whether or not IF EXISTS is supported in DROP INDEX statements.
     */
    public $useDropIndexIfExists = false;

    /**
     * @var bool Whether or not indexes should be unique to a table or to a database.
     */
    public $perTableIndexes = true;

    /**
     * @var bool Whether or not PARTITION is supported in table definitions.
     */
    public $usePartition = false;

    /**
     * @var bool Whether or not to use index storage specification when indexes are created with CREATE TABLE.
     */
    public $indexStorageOnCreate = false;

    /**
     * @var string {
     *     Mode used to support column comments. Options:
     *
     *    'useAttributes' - Use "COMMENT=" attribute on columns/tables.
     *    'useCommentOn' - Execute "COMMENT ON" queries after table insertion.
     */
    public $commentMode = false;

    /**
     * @var string {
     *     Mode used to support index creation. Options:
     *
     *    'useTableAttribute' - Embed in CREATE TABLE statement.
     *    'useCreateIndex' - Execute "CREATE INDEX" queries after table insertion.
     */
    public $indexMode = false;

    /**
     * @var string {
     *     Mode used to support enums. Options:
     *
     *    'useEnum' - Use native ENUM(val1, val2) type.
     *    'useCreateType' - Create a custom enumerated type with CREATE TYPE.
     *    'useCheck' - Use a CHECK() clause when creating the type.
     */
    public $enumMode = false;

    /**
     * @var string {
     *     Mode used to support enums. Options:
     *
     *    'onDuplicateKey' - Use MySQL on-duplicate-key handling.
     *    'onConflictDoUpdate' - Use PostGres 9.5+ on-conflict-do-update handling.
     *    'selectThenInsertOrUpdate' - Select on key condition, and either insert if no results or update if there are.
     *    'tryCatch' - Attempt an insert, update on failure (not yet implemented).
     */
    public $upsertMode = false;


    /**
     * @var string {
     *     Mode used to support foreign keys. Options:
     *
     *    'useAlterTableForeignKey' - Use ALTER TABLE DROP FOREIGN KEY.
     *    'useAlterTableConstraint' - Use ALTER TABLE DROP CONSTRAINT.
     */
    public $foreignKeyMode = false;


    /**
     * @var string {
     *     Mode used to support table renames. Options:
     *
     *    'alterTable' - Use ALTER TABLE syntax.
     *    'renameTable' - Use RENAME TABLE sytnax.
     */
    public $tableRenameMode = false;


    /**
     * @var string {
     *     Mode used to support foreign keys. Options:
     *
     *    'autoIncrement' - Use AUTO_INCREMENT.
     *    'identity' - Use IDENTITY(1,1)
     */
    public $serialMode = 'autoIncrement';

}