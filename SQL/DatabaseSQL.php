<?php
namespace Database\SQL;

use Exception;

use Database\Database;
use Database\Result;
use Database\ResultInterface;

use Database\Engine;
use Database\Index;
use Database\Type;
use Database\Type\Comparison;

/**
 * A SQL implementation of {@see Database}.
 * Note that DatabaseSQL, unlike {@see Database}, has only partial support for adding new SQL variants. While much functionality is supported through the {@link DatabaseSQLInterface} implementations, in general a lot of per-driver functionality must still be added in this file.
 *
 * @package Database\SQL
 */
class DatabaseSQL extends Database
{
    public $classVersion = 3;
    public $classProduct = 'fim';

    /**
     * @var string The full version of the DBMS we are connected to.
     */
    public $versionString = '0.0.0';

    /**
     * @var int|string The primary version (e.g. 4 in 4.2.1) of the DBMS we are connected to.
     */
    public $versionPrimary = 0;

    /**
     * @var int|string The secondary version (e.g. 2 in 4.2.1) of the DBMS we are connected to.
     */
    public $versionSecondary = 0;

    /**
     * @var int|string The tertiary version (e.g. 1 in 4.2.1) of the DBMS we are connected to.
     */
    public $versionTertiary = 0;

    /**
     * @var string The database mode. This will always be SQL for us.
     */
    public $mode = 'SQL';

    /**
     * @var string The driver currently in use. One of "mysql", "mysqli", "pdo-mysql", "pgsql", "pdo-pgsql"
     */
    public $driver;

    /**
     * @var string The language currently in used. One of "mysql", "pgsql"
     */
    public $language;

    /**
     * @var SQL_Definitions
     */
    public $sqlInterface;

    /**
     * @var array Maps between drivers and languages.
     *            TODO: remove
     */
    private $driverMap = array(
        'mysql' => 'mysql',
        'mysqli' => 'mysql',
        'pdoMysql' => 'mysql',
        'pgsql' => 'pgsql',
    );

    /**
     * @var array All queries will be stored here during execution.
     */
    public $queryLog = array();

    /**
     * @var bool|string If set to a file string, queries will be logged to this file.
     */
    public $queryLogToFile = false;

    /**
     * @var int When this is greater than 0, rawQuery should return its query instead of executing it. Ideal for simulation and testing.
     */
    public $returnQueryString = 0;

    /**
     * @var bool If enabled, triggers will be placed in {@link DatabaseSQL::triggerQueue}, and won't be run.
     */
    protected $holdTriggers = false;

    /**
     * @var array A queue for triggers when {@link DatabaseSQL::holdTriggers} is enabled.
     */
    protected $triggerQueue = [];



    /*********************************************************
     ************************ START **************************
     ******************* General Functions *******************
     *********************************************************/

    public function __destruct() {
        $this->rollbackTransaction();
        $this->close();

        if ($this->queryLogToFile) {
            file_put_contents($this->queryLogToFile, '*****' . $_SERVER['SCRIPT_FILENAME'] . '***** (Max Memory: ' . memory_get_peak_usage() . ') ' . PHP_EOL . print_r($this->queryLog, true) . PHP_EOL . PHP_EOL . PHP_EOL, FILE_APPEND | LOCK_EX);
        }
    }


    /**
     * Autodetect how to format.
     * In most cases, this will format either an an integer or a string. Refer to {@link database::auto()} for more information.
     */
    const FORMAT_VALUE_DETECT = 'detect';


    /**
     * Format for fuzzy searching.
     */
    const FORMAT_VALUE_SEARCH = 'search';

    /**
     * Format as a column alias.
     * When used, the first variable argument is the column name and the second variable argument is the alias name.
     */
    const FORMAT_VALUE_COLUMN_ALIAS = 'columnAlias';

    /**
     * Format as a table name.
     */
    const FORMAT_VALUE_TABLE = 'table';

    /**
     * Format as a column name with a table name.
     * When used, the first variable argument is the table name and the second variable argument is the column name.
     */
    const FORMAT_VALUE_TABLE_COLUMN = 'tableColumn';

    /**
     * Format as a table alias.
     * When used, the first variable argument is the table name and the second variable argument is the table alias.
     */
    const FORMAT_VALUE_TABLE_ALIAS = 'tableAlias';

    /**
     * Format as a column name with a table name.
     * When used, the first variable argument is the table name, the second variable argument is the column name, and the third variable argument is the column alias.
     */
    const FORMAT_VALUE_TABLE_COLUMN_NAME_ALIAS = 'tableColumnNameAlias';

    /**
     * Format as a database name.
     */
    const FORMAT_VALUE_DATABASE = 'database';

    /**
     * Format as a table name with an attached database name.
     * When used, the first variable argument is the database name and the second variable argument is the table name.
     */
    const FORMAT_VALUE_DATABASE_TABLE = 'databaseTable';

    /**
     * Format as a table index name.
     */
    const FORMAT_VALUE_INDEX = 'index';

    /**
     * Format as an array of values used in an ENUM.
     */
    const FORMAT_VALUE_ENUM_ARRAY = 'enumArray';

    /**
     * Format as a table alias with an attached table name.
     * When used, the first variable argument is the table name and the second variable argument is the alias name.
     */
    const FORMAT_VALUE_TABLE_NAME_ALIAS = 'tableNameAlias';

    /**
     * Format as an array of update clauses.
     */
    const FORMAT_VALUE_UPDATE_ARRAY = 'updateArray';

    /**
     * Format as an array of columns.
     */
    const FORMAT_VALUE_COLUMN_ARRAY = 'columnArray';

    /**
     * Format as an array of table columns and corresponding values.
     * When used, the first variable argument is the table name, the second variable argument is a list of columns, and the third variable argument is a list of values.
     */
    const FORMAT_VALUE_TABLE_COLUMN_VALUES = 'tableColumnValues';

    /**
     * Format as an array of table columns and corresponding values.
     * When used, the first variable argument is the table name and the second variable argument is a an associative array of columns-value pairs, indexed by column name.
     */
    const FORMAT_VALUE_TABLE_UPDATE_ARRAY = 'tableUpdateArray';

    /**
     * Format a value to represent the specified type in an SQL query.
     *
     * @param Type|FORMAT_VALUE_* type The type to format the value(s) as. All DatabaseTypeType constants can be used (and will format as expected). The other types are all databaseSQL constants named "FORMAT_VALUE_*"; refer to their documentation seperately.
     * @param mixed $values,... The values to be formatted. Instances of DatabaseTypeType typically only take one value. For FORMAT_VALUE_* types, refer to their own documentation.
     *
     * @return mixed Value, formatted as specified.
     *
     * @throws Exception
     */
    public function formatValue($type)
    {
        $values = func_get_args();

        switch ($type) {
            case DatabaseSQL::FORMAT_VALUE_DETECT:
                $item = $this->auto($values[1]);

                return $this->formatValue($item->type, $item->value);
                break;

            case Type\Type::null:
                return 'NULL';
                break;

            case DatabaseSQL::FORMAT_VALUE_SEARCH:
                return $this->sqlInterface->stringQuoteStart
                    . $this->sqlInterface->stringFuzzy
                    . $this->escape(addcslashes($values[1], '%_\\'), $type) // TODO?
                    . $this->sqlInterface->stringFuzzy
                    . $this->sqlInterface->stringQuoteEnd;
                break;

            case Type\Type::string:
                return $this->sqlInterface->stringQuoteStart
                    . $this->escape($values[1], $type)
                    . $this->sqlInterface->stringQuoteEnd;
                break;

            case Type\Type::bool:
                return $this->sqlInterface->boolValues[$values[1]];
                break;

            case Type\Type::blob:
                return $this->sqlInterface->binaryQuoteStart
                    . $this->escape($values[1], $type)
                    . $this->sqlInterface->binaryQuoteEnd;
                break;

            case Type\Type::bitfield:
                if ($this->sqlInterface->nativeBitfield)
                    return 'B\'' . decbin((int) $values[1]) . '\'';
                else
                    return $this->formatValue(Type\Type::integer, $values[1]);
            break;

            case Type\Type::integer:
                return $this->sqlInterface->intQuoteStart
                    . $this->escape((int)$values[1], $type)
                    . $this->sqlInterface->intQuoteEnd;
                break;

            case Type\Type::float:
                return $this->sqlInterface->floatQuoteStart
                    . (float) $this->escape($values[1], $type)
                    . $this->sqlInterface->floatQuoteEnd;
            break;

            case Type\Type::timestamp:
                return $this->sqlInterface->timestampQuoteStart
                    . $this->escape((int) $values[1], $type)
                    . $this->sqlInterface->timestampQuoteEnd;
                break;

            case Type\Type::column:
                return $this->sqlInterface->columnQuoteStart
                    . $this->escape($values[1], $type)
                    . $this->sqlInterface->columnQuoteEnd;
                break;

            case Type\Type::equation:  // Only partially implemented, because equations are stupid. Don't use them if possible.
                return preg_replace_callback('/\$(([a-zA-Z_]+)\.|)([a-zA-Z]+)/', function ($matches) {
                    if ($matches[1])
                        return $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN, $matches[2], $matches[3]);
                    else
                        return $this->formatValue(Type\Type::column, $matches[3]);
                }, $values[1]);
            break;

            case Type\Type::arraylist:
                foreach ($values[1] AS &$item) {
                    $item = $this->auto($item);
                    $item = $this->formatValue($item->type, $item->value);
                }

                return $this->sqlInterface->arrayQuoteStart
                    . implode($this->sqlInterface->arraySeperator, $values[1])
                    . $this->sqlInterface->arrayQuoteEnd;
            break;

            case DatabaseSQL::FORMAT_VALUE_COLUMN_ALIAS:
                return $this->sqlInterface->columnAliasQuoteStart
                    . $this->escape($values[1], $type)
                    . $this->sqlInterface->columnAliasQuoteEnd;
                break;

            case DatabaseSQL::FORMAT_VALUE_TABLE:
                return $this->sqlInterface->tableQuoteStart
                    . $this->escape($values[1], $type)
                    . $this->sqlInterface->tableQuoteEnd;
                break;

            case DatabaseSQL::FORMAT_VALUE_TABLE_ALIAS:
                return $this->sqlInterface->tableAliasQuoteStart
                    . $this->escape($values[1], $type)
                    . $this->sqlInterface->tableAliasQuoteEnd;
                break;

            case DatabaseSQL::FORMAT_VALUE_DATABASE:
                return $this->sqlInterface->databaseQuoteStart
                    . $this->escape($values[1], $type)
                    . $this->sqlInterface->databaseQuoteEnd;
                break;

            case DatabaseSQL::FORMAT_VALUE_INDEX:
                return $this->sqlInterface->indexQuoteStart
                    . $this->escape($values[1], $type)
                    . $this->sqlInterface->indexQuoteEnd;
                break;

            case DatabaseSQL::FORMAT_VALUE_ENUM_ARRAY:
                foreach ($values[1] AS &$item) {
                    $item = $this->str($item);
                }

                return $this->formatValue(Type\Type::arraylist, $values[1]);
            break;

            case DatabaseSQL::FORMAT_VALUE_COLUMN_ARRAY:
                foreach ($values[1] AS &$item)
                    $item = $this->formatValue(Type\Type::column, $item);

                return $this->sqlInterface->arrayQuoteStart
                    . implode($this->sqlInterface->arraySeperator, $values[1])
                    . $this->sqlInterface->arrayQuoteEnd;
                break;

            case DatabaseSQL::FORMAT_VALUE_UPDATE_ARRAY:
                $update = array();

                foreach ($values[1] AS $column => $value) {
                    $update[] = $this->formatValue(Type\Type::column, $column)
                        . $this->sqlInterface->comparisonTypes[Comparison::assignment]
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_DETECT, $value);
                }

                return implode($update, $this->sqlInterface->statementSeperator);
                break;

            case DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN:
                return $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $values[1])
                    . $this->sqlInterface->tableColumnDivider
                    . $this->formatValue(Type\Type::column, $values[2]);
                break;

            case DatabaseSQL::FORMAT_VALUE_DATABASE_TABLE:
                return $this->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE, $values[1])
                    . $this->sqlInterface->databaseTableDivider
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $values[2]);
                break;

            case DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_NAME_ALIAS:
                return $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $values[1])
                    . $this->sqlInterface->tableColumnDivider
                    . $this->formatValue(Type\Type::column, $values[2])
                    . $this->sqlInterface->columnAliasDivider
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_COLUMN_ALIAS, $values[3]);
                break;

            case DatabaseSQL::FORMAT_VALUE_TABLE_NAME_ALIAS:
                return $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $values[1])
                    . $this->sqlInterface->tableAliasDivider
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_ALIAS, $values[2]);
                break;

            case DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_VALUES:
                $tableName = $values[1];

                /* Copy & transform values
                 * Some columns get inserted as-is, but a transformed copy is also then added. When we are modifying such a column, we create the copy here.
                 * If the copy is modified independently, it will not be altered here -- but it should also not be modified independently.
                 * TODO: items stored as DatabaseType will not be detected properly
                 */
                if (isset($this->encodeCopy[$tableName])) { // Do we have copy & transform values for the table we are inserting into?
                    foreach ($this->encodeCopy[$tableName] AS $startColumn => $endResult) { // For each copy & transform value in our table...
                        list($endFunction, $typeOverride, $endColumn) = $endResult;

                        if (($key = array_search($startColumn, $values[2])) !== false) { // Check to see if we are, in-fact, inserting the column
                            $values[2][] = $endColumn;

                            foreach ($values[3] AS &$valuesArray) {
                                $valuesArray[] = $this->applyTransformFunction($endFunction, $valuesArray[$key], $typeOverride); // And if we are, add the new copy column to the list of insert columns
                            }
                        }
                    }
                }

                // Columns
                foreach ($values[2] AS $key => &$column) {
                    if (isset($this->encode[$tableName]) && isset($this->encode[$tableName][$column])) {
                        list($function, $typeOverride) = $this->encode[$tableName][$column];

                        foreach ($values[3] AS &$valuesArray) {
                            $valuesArray[$key] = $this->applyTransformFunction($function, $valuesArray[$key], $typeOverride);
                        }
                    }

                    $column = $this->formatValue(Type\Type::column, $column);
                }

                // Values
                $valueStatements = [];
                foreach ($values[3] AS $valuesArray) {
                    $valueStatements[] = $this->formatValue(Type\Type::arraylist, $valuesArray);
                }


                // Return query componenet
                return $this->sqlInterface->arrayQuoteStart
                    . implode($this->sqlInterface->arraySeperator, $values[2])
                    . $this->sqlInterface->arrayQuoteEnd
                    . ' VALUES '
                    . implode($this->sqlInterface->arraySeperator, $valueStatements);
            break;

            case DatabaseSQL::FORMAT_VALUE_TABLE_UPDATE_ARRAY:
                $tableName = $values[1];
                $update = array();

                /* Copy & transform values
                 * Some columns get inserted as-is, but a transformed copy is also then added. When we are modifying such a column, we create the copy here.
                 * If the copy is modified independently, it will not be altered here -- but it should also not be modified independently. *
                 */
                if (isset($this->encodeCopy[$tableName])) { // Do we have copy & transform values for the table we are updating?
                    foreach ($this->encodeCopy[$tableName] AS $startColumn => $endResult) { // For each copy & transform value in our table...
                        list($endFunction, $typeOverride, $endColumn) = $endResult;

                        if (isset($values[2][$startColumn])) // Check to see if we are, in-fact, updating the column
                            $values[2][$endColumn] = $this->applyTransformFunction($endFunction, $values[2][$startColumn], $typeOverride); // And if we are, add the new copy column to the list of update columns
                    }
                }

                /* Process each column and value pair */
                foreach ($values[2] AS $column => $value) {
                    /* Transform values
                     * Some columns get transformed prior to being sent to the database: we handle those here. */
                    if (isset($this->encode[$tableName]) && isset($this->encode[$tableName][$column])) {
                        list($function, $typeOverride) = $this->encode[$tableName][$column];

                        $value = $this->applyTransformFunction($function, $value, $typeOverride);
                    }

                    /* Format and add the column/value pair to our list */
                    $update[] = $this->formatValue(Type\Type::column, $column)
                        . $this->sqlInterface->comparisonTypes[Comparison::assignment]
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_DETECT, $value);
                }

                /* Return our list of paired values as an string */
                return implode($update, $this->sqlInterface->statementSeperator);
            break;

            default:
                throw new Exception("databaseSQL->formatValue does not recognise type '$type'");
                break;
        }
    }



    /** Formats two columns or table names such that one is an alias.
     *
     * @param string value - The value (column name/table name).
     *
     * @internal Needless to say, this is quite the simple function. However, I feel that the syntax merits it, as there are certainly other ways an "AS" could be structure. (Most wouldn't comply with SQL, but strictly speaking I would like this class to work with slight modifications of SQL as well, if any exist.)
     *
     * @param string alias - The alias.
     * @return string - The formatted SQL string.
     * @author Joseph Todd Parsons <josephtparsons@gmail.com>
     */
    /*  private function formatAlias($value, $alias, $type) {
        switch ($type) {
          case 'column': case 'table': return "$value AS $alias"; break;
        }
      }*/


    private function setDatabaseVersion($versionString)
    {
        $versionString = (string)$versionString;
        $this->versionString = $versionString;
        $strippedVersion = '';

        // Get the version without any extra crap (e.g. "5.0.0.0~ubuntuyaypartytimeohohohoh").
        for ($i = 0; $i < strlen($versionString); $i++) {
            if (in_array($versionString[$i], array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.'), true)) $strippedVersion .= $versionString[$i];
            else break;
        }

        // Divide the decimal versions into an array (e.g. 5.0.1 becomes [0] => 5, [1] => 0, [2] => 1) and set the first three as properties.
        $strippedVersionParts = explode('.', $strippedVersion);

        $this->versionPrimary = $strippedVersionParts[0];
        $this->versionSecondary = $strippedVersionParts[1];
        $this->versionTertiary = $strippedVersionParts[2];


        // Compatibility check. We're really not sure how true any of this, and we have no reason to support older versions, but meh.
        // todo: move
        switch ($this->driver) {
            case 'mysql':
            case 'mysqli':
                if ($strippedVersionParts[0] <= 4) { // MySQL 4 is a no-go.
                    throw new Exception('You have attempted to connect to a MySQL version 4 database, which is unsupported.');
                }
                elseif ($strippedVersionParts[0] == 5 && $strippedVersionParts[1] == 0 && $strippedVersionParts[2] <= 4) { // MySQL 5.0.0-5.0.4 is also a no-go (we require the BIT type, even though in theory we could work without it)
                    $this->sqlInterface->nativeBitfield = false;
                }
                break;
        }
    }


    public function connect($host, $port, $user, $password, $database, $driver, $tablePrefix = '')
    {
        $this->sqlPrefix = $tablePrefix;


        /* Detect Incompatible MySQLi */
        if ($driver === 'mysqli' && PHP_VERSION_ID < 50209) { // PHP_VERSION_ID isn't defined with versions < 5.2.7, but this obviously isn't a problem here (it will eval to 0, which is indeed less than 50209).
            $driver = 'mysql';
        }


        /* Load DatabaseSQLInterface Driver from File */
        $className = '\Database\SQL\Drivers\\' . ucfirst($driver);

        if (!class_exists($className)) {
            throw new Exception('The specified DatabaseSQL driver is not installed.');
        }
        else {
            $this->sqlInterface = new $className();
        }


        /* Perform Connection */
        if (!$this->sqlInterface->connect($host, $port, $user, $password, $database)) { // Make the connection.
            $this->triggerError('Could Not Connect: ' . $this->sqlInterface->getLastError(), array( // Note: we do not include "password" in the error data.
                'host'     => $host,
                'port'     => $port,
                'user'  => $user,
                'database' => $database
            ), 'connection');

            return false;
        }


        /* Select Database, If Needed (TODO: catch errors) */
        if (!$this->activeDatabase && $database) { // Some drivers will require this.
            if (!$this->selectDatabase($database)) { // Error will be issued in selectDatabase.
                return false;
            }
        }

        return true;
    }


    /**
     * Fetches {@link databaseSQL::versionPrimary}, {@link databaseSQL::versionSecondary}, and {@link databaseSQL::versionTertiary} from the current database connection.
     */
    public function loadVersion()
    {
        if ($this->versionPrimary > 0) // Don't reload information.
            return true;

        $this->setDatabaseVersion($this->sqlInterface->getVersion());

        return true;
    }


    public function close()
    {
        return $this->sqlInterface->close();
    }


    /**
     * Returns a properly escaped string for raw queries.
     *
     * @param string int|string - Value to escape.
     * @param string context - The value type, in-case the escape method varies based on it.
     * @return string
     * @author Joseph Todd Parsons <josephtparsons@gmail.com>
     */

    protected function escape($string, $context = Type\Type::string)
    {
        return $this->sqlInterface->escape($string, $context); // Return the escaped string.
    }


    /**
     * Sends a raw, unmodified query string to the database server.
     * The query may be logged if it takes a certain amount of time to execute successfully.
     *
     * @param string $query - The raw query to execute.
     * @return bool - True on success, false on failure.
     */
    public function rawQuery($query)
    {
        if ($query === false) {
            if ($this->returnQueryString > 0)
                $this->returnQueryString--;

            return false;
        }

        elseif ($this->returnQueryString > 0) {
            $this->returnQueryString--;

            return $this->sqlInterface->query($query, true);
        }

        else {
            $start = microtime(true);

            if ($queryData = $this->sqlInterface->query($query)) {
                $this->newQuery($query, microtime(true) - $start);

                return true;
            }

            else {
                $this->newQuery($query);
                $this->triggerError($this->sqlInterface->getLastError(), $query);

                return false;
            }
        }
    }


    /**
     * Sends a raw, unmodified query string to the database server, expecting a resultset to be returned.
     * The query may be logged if it takes a certain amount of time to execute successfully.
     *
     * @param string $query - The raw query to execute.
     *
     * @return Result The database result returned by the query, or false on failure.
     */
    public function rawQueryReturningResult($query, $reverseAlias = false, int $paginate = 0) {

        $start = microtime(true);

        // todo: probably rewrite this, iunno
        $queryData = $this->sqlInterface->queryReturningResult($query);
        if ($queryData->source) {
            $this->newQuery($query, microtime(true) - $start);

            return $this->databaseResultPipe($queryData, $reverseAlias, $query, $this, $paginate);
        }

        else {
            $this->newQuery($query);
            $this->triggerError($this->sqlInterface->getLastError(), $query);

            return false;
        }

    }


    /**
     * Configures the Database abstraction layer to return the query string instead of executing it for the next query.
     * It will be automatically turned off as soon as a query is executed.
     * Can be chained.
     * Obviously, this should not be used in conjunction with functions that execute multiple queries simultaneously (though these are rare).
     *
     * @return $this
     */
    public function returnQueryString() {
        $this->returnQueryString++;

        return $this;
    }


    /**
     * @see Database::databaseResultPipe()
     */
    protected function databaseResultPipe($queryData, $reverseAlias, string $sourceQuery, Database $database, int $paginated = 0)
    {
        return new Result($queryData, $reverseAlias, $sourceQuery, $database, $paginated);
    }


    /**
     * Add the text of a query to the log. This should normally only be called by rawQuery(), but is left protected since other purposes could exist by design.
     *
     * @return string - The query text of the last query executed.
     * @author Joseph Todd Parsons <josephtparsons@gmail.com>
     */

    protected function newQuery($queryText, $microtime = false)
    {
        $this->queryCounter++;
        $this->queryLog[] = [$queryText/*, debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS)*/, $microtime];
    }


    /**
     * Get the text of the last query executed.
     *
     * @return string - The query text of the last query executed.
     * @author Joseph Todd Parsons <josephtparsons@gmail.com>
     */
    public function getLastQuery()
    {
        return end($this->queryLog)[0];
    }


    /**
     * Clears the query log.
     *
     * @return string - The query text of the last query executed.
     * @author Joseph Todd Parsons <josephtparsons@gmail.com>
     */
    public function clearQueries()
    {
        $this->queryLog = array();
    }

    /*********************************************************
     ************************* END ***************************
     ******************* General Functions *******************
     *********************************************************/


    /*********************************************************
     ************************ START **************************
     ********************* Transactions **********************
     *********************************************************/


    /* Basic Usage:
     * Transactions are effectively automatic. Scripts should call start and end transaction. A rollback will occur as part of a database error, and the database connection will automatically be closed.
     * In other words, these transactions are super duper basic. This has benefits -- it means writing less code, which, honestly, is something I'm happy with. */


    public function startTransaction()
    {
        $this->transaction = true;

        $this->sqlInterface->startTransaction();
    }


    public function rollbackTransaction()
    {
        if ($this->transaction) {
            $this->transaction = false;

            $this->sqlInterface->rollbackTransaction();
        }
    }


    public function endTransaction()
    {
        $this->transaction = false;

        $this->sqlInterface->endTransaction();
    }

    public function holdTriggers($holdTriggers)
    {
        if (!$holdTriggers && $this->holdTriggers) {
            foreach ($this->triggerQueue AS $trigger) {
                $this->rawQuery($trigger);
            }

            $this->triggerQueue = [];
        }

        $this->holdTriggers = $holdTriggers;
    }



    /*********************************************************
     ************************* END ***************************
     ********************* Transactions **********************
     *********************************************************/


    /*********************************************************
     ************************ START **************************
     ****************** Database Functions *******************
     *********************************************************/

    public function selectDatabase($database)
    {
        $error = false;

        if ($this->sqlInterface->selectDatabase($database)) { // Select the database.
            if ($this->sqlInterface->getLanguage() == 'mysql' || $this->sqlInterface->getLanguage() == 'mysqli') {
                if (!$this->rawQuery('SET NAMES "utf8"')) { // Sets the database encoding to utf8 (unicode).
                    $error = 'SET NAMES Query Failed';
                }
            }
        } else {
            $error = 'Failed to Select Database';
        }

        if ($error) {
            $this->triggerError($error);
            return false;
        } else {
            $this->activeDatabase = $database;
            return true;
        }
    }


    public function createDatabase($database)
    {
        if ($this->sqlInterface->useCreateIfNotExist) {
            return $this->rawQuery('CREATE DATABASE IF NOT EXISTS ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE, $database));
        }
        else {
            try {
                return @$this->rawQuery('CREATE DATABASE ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE, $database));
            } catch (Exception $ex) {
                return true;
            }
        }
    }

    /*********************************************************
     ************************* END ***************************
     ****************** Database Functions *******************
     *********************************************************/


    /*********************************************************
     ************************ START **************************
     ******************* Table Functions *********************
     *********************************************************/


    /**
     * Parses an array of column names, along with a table name and engine identifier,
     *
     * @param $tableName    string The name of the table whose columns are being parsed.
     * @param $tableIndexes array  The current known table indexes that apply to the columns.
     * @param $tableColumns array {
     *     An array of table column properties indexed by the table column name. Valid parameters:
     *
     *     'restrict'      array            An array of values to restrict the column to.
     *     'maxlen'        int              The maximum size of the data that can be put into the column.
     *     'autoincrement' bool             If the column should be a "serial" column that increments for each row.
     *     'default'       mixed            The default value for the column.
     *     'comment'       string           Information about the column for documentation purposes.
     *     'type'          DatabaseTypeType The column's type.
     * }
     * @param $engine Engine The engine the table is using.
     *
     * @return array An array of four things: (1.) an array of SQL column statements, (2.) an array of triggers to run after creating columns (which will typically maintain default values), (3.) the array of indexes, which may have been modified, and (4.) additional SQL parameters to append to the CREATE TABLE statement, for instance "AUTO_INCREMENT=".
     * @throws Exception If enums are not supported and need to be used.
     */
    private function parseTableColumns($tableName, $tableColumns, $tableIndexes = [], $engine = Engine::general) {
        /**
         * Additional table parameters to be appended at the end of the CREATE TABLE statement. For instance, "AUTO_INCREMENT=".
         */
        $tableProperties = '';

        /**
         * A list of SQL statements that contain the column components to be used with an SQL query.
         */
        $columns = [];

        /**
         * A list of SQL statements that contain triggers and should be run when creating a column.
         */
        $triggers = [];

        /* Process Each Column */
        foreach ($tableColumns AS $columnName => $column) {
            /**
             * Our column parameters. Defaults set, but checking is not performed.
             */
            $column = array_merge([
                'restrict' => false,
                'maxlen' => 10,
                'autoincrement' => false,
                'default' => null,
                'comment' => '',
                'preferAscii' => false
            ], $column);

            /**
             * The SQL type identifier, e.g. "INT"
             */
            $typePiece = '';


            /* Process Column Types */
            switch ($column['type']) {
                /* The column is integral. */
                case Type\Type::integer:
                    // If we have limits of "serial" (sequential) datatypes, and we are a serial type (that is, we're autoincrementing), using the serial limits.
                    if (isset($this->sqlInterface->dataTypes['columnSerialLimits']) && $column['autoincrement'])
                        $intLimits = $this->sqlInterface->dataTypes['columnSerialLimits'];

                    // If we don't have "serial" datatype limits, or we aren't using a serial datatype (aren't autoincrementing), use normal integer limits.
                    else
                        $intLimits = $this->sqlInterface->dataTypes['columnIntLimits'];

                    // Go through our integer limits (keyed by increasing order)
                    foreach ($intLimits AS $length => $type) {
                        if ($column['maxlen'] <= $length) {
                            $typePiece = $intLimits[$length];
                            break;
                        }
                    }

                    // If we haven't found a valid type identifer, use the default.
                    if (!strlen($typePiece)) $typePiece = $intLimits['default'];

                    // If we don't have serial limits and are autoincrementing, use the AUTO_INCREMENT orthogonal type identifier.
                    if (!isset($this->sqlInterface->dataTypes['columnSerialLimits']) && $column['autoincrement']) {
                        switch ($this->sqlInterface->serialMode) {
                            case 'autoIncrement':
                                $typePiece .= ' AUTO_INCREMENT'; // On the type itself.
                                $tableProperties .= ' AUTO_INCREMENT = ' . (int)$column['autoincrement']; // And also on the table definition.
                                break;

                            case 'identity':
                                $typePiece .= ' IDENTITY(1,1)'; // On the type itself.
                                break;
                        }

                        // And also create an index for it, if we don't already have one.
                        if (!isset($tableIndexes[$columnName])) {
                            $tableIndexes[$columnName] = [
                                'type' => 'index',
                            ];
                        }
                    }
                break;


                /* The column is an integral that encodes bitwise information. */
                case Type\Type::bitfield:
                    // If our SQL engine support a BIT type, use it.
                    if ($this->sqlInterface->nativeBitfield) {
                        $typePiece = 'BIT(' . $column['bits'] . ')';
                    }

                    // Otherwise, use a predefined type identifier.
                    else {
                        if ($column['bits']) { // Do we have a bit size definition?
                            foreach ($this->sqlInterface->dataTypes['columnBitLimits'] AS $bits => $type) { // Search through our bit limit array, which should be in ascending order of bits.
                                if ($column['bits'] <= $bits) { // We have a definition that fits our constraint.
                                    $typePiece = $type;
                                    break;
                                }
                            }
                        }

                        if (!strlen($typePiece)) { // If no type identifier was found...
                            $typePiece = $this->sqlInterface->dataTypes['columnBitLimits']['default']; // Use the default.
                        }
                    }
                break;


                /* The column encodes time information, most often using an integral and unix timestamp. */
                case Type\Type::timestamp:
                    $typePiece = $this->sqlInterface->dataTypes[Type\Type::timestamp];
                break;


                /* The column encodes a boolean, most often using a BIT(1) or other small integral. */
                case Type\Type::bool:
                    $typePiece = $this->sqlInterface->dataTypes[Type\Type::bool];
                break;


                /* The column encodes a floating point, with unspecified precision. */
                case Type\Type::float:
                    $typePiece = $this->sqlInterface->dataTypes[Type\Type::float];
                break;


                /* The column is a textual string or a binary string. */
                case Type\Type::string:
                case Type\Type::blob:
                case Type\Type::json:
                    if ($column['type'] === Type\Type::json && $this->sqlInterface->dataTypes[Type\Type::json]) {
                        $typePiece = $this->sqlInterface->dataTypes[Type\Type::json];
                    }
                    else {
                        // Limits may differ depending on table type and column type. Get the correct array encoding limits.
                        $stringLimits = $this->sqlInterface->dataTypes['column' . ($column['type'] === Type\Type::blob ? 'Blob' : 'String') . ($engine === Engine::memory ? 'Temp' : 'Perm') . 'Limits'];

                        // Search through the array encoding limits. This array should be keyed in increasing size.
                        foreach ($stringLimits AS $length => $type) {
                            if (!is_int($length)) continue; // allow default key

                            if ($column['maxlen'] <= $length) { // If we have found a valid type definition for our column's size...
                                if (in_array($type, $this->sqlInterface->dataTypes['columnNoLength']))
                                    $typePiece = $type; // If the particular datatype doesn't encode size information, omit it.
                                else
                                    $typePiece = $type . '(' . $column['maxlen'] . ')'; // Otherwise, use the type identifier with our size information.

                                break;
                            }
                        }

                        // If no type identifier was found...
                        if (!strlen($typePiece)) {
                            $typePiece = $stringLimits['default']; // Use the default.
                        }

                        // Use latin1 character set on MySQL if preferAscii is set.
                        if ($column['preferAscii'] && $this->sqlInterface->getLanguage() === 'mysql') {
                            $typePiece .= ' CHARACTER SET latin1';
                        }
                    }
                break;


                /* The column is an enumeration of values. */
                case Type\Type::enum:
                    // There are many different ways ENUMs may be supported in SQL DBMSs. Select our supported one.
                    switch ($this->sqlInterface->enumMode) {
                        // Here, we create a special type to use as an enum. PostGreSQL does this.
                        case 'useCreateType':
                            $typePiece = $this->formatValue(DatabaseSQL::FORMAT_VALUE_INDEX, $tableName . '_' . $columnName);
                            $this->rawQuery('DROP TYPE IF EXISTS ' . $typePiece . ' CASCADE');
                            $this->rawQuery('CREATE TYPE ' . $typePiece . ' AS ENUM' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_ENUM_ARRAY, $column['restrict']));
                        break;

                        // Here, we use the built-in SQL ENUM. MySQL does this.
                        case 'useEnum':
                            $typePiece = 'ENUM' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_ENUM_ARRAY, $column['restrict']);
                        break;

                        // And here we use the CHECK() clause at the end of the type. MSSQL does this.
                        case 'useCheck':
                            $lengths = array_map('strlen', $column['restrict']);
                            $typePiece = 'VARCHAR('
                                    . max($lengths)
                                . ') CHECK ('
                                    . $this->formatValue(Type\Type::column, $columnName)
                                    . ' IN'
                                    . $this->formatValue(Type\Type::arraylist, $column['restrict'])
                                . ')';
                        break;

                        // We don't support ENUMs in the current database mode.
                        default: throw new Exception('Enums are unsupported in the active database driver.'); break;
                    }
                break;


                /* The column type value is invalid. */
                default:
                    $this->triggerError("Unrecognised Column Type", array(
                        'tableName' => $tableName,
                        'columnName' => $columnName,
                        'columnType' => $column['type'],
                    ), 'validation');
                break;
            }


            /* Process Defaults (only if column default is specified) */
            if ($column['default'] !== null) {
                // We use triggers here when the SQL implementation is otherwise stubborn, but FreezeMessenger is designed to only do this when it would otherwise be tedious. Manual setting of values is preferred in most cases. Right now, only __TIME__ supports them.
                // TODO: language trigger support check
                if ($column['default'] === '__TIME__') {
                    $tableNameEscaped = $this->formatValue(self::FORMAT_VALUE_TABLE, $tableName);
                    $triggerName = "{$tableName}_{$columnName}__TIME__";
                    $triggerNameEscaped = $this->formatValue(self::FORMAT_VALUE_INDEX, $triggerName);
                    $columnNameEscaped = $this->formatValue(Type\Type::column, $columnName);

                    switch ($this->sqlInterface->getLanguage()) {
                        case 'sqlsrv':
                            $triggers[] = 'ALTER TABLE '
                                . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                                . " ADD CONSTRAINT {$triggerNameEscaped} DEFAULT DATEDIFF(s, '1970-01-01 00:00:00', GETUTCDATE()) FOR {$columnNameEscaped}";
                            break;

                        case 'mysql': // This one is kinda just for testing. We should replace it with DEFAULT UNIX_TIMESTAMP.
                            $triggers[] = "DROP TRIGGER IF EXISTS {$triggerNameEscaped}";
                            $triggers[] = "CREATE TRIGGER {$triggerNameEscaped}
                                BEFORE INSERT ON {$tableNameEscaped}
                                FOR EACH ROW
                                    SET NEW.{$columnNameEscaped} = IF(NEW.{$columnNameEscaped}, NEW.{$columnNameEscaped}, UNIX_TIMESTAMP(NOW()))";
                            break;

                        case 'pgsql':
                            $functionNameEscaped = $this->formatValue(self::FORMAT_VALUE_INDEX, "{$triggerName}_function");

                            $triggers[] = "DROP TRIGGER IF EXISTS {$triggerNameEscaped} ON {$tableNameEscaped}";
                            $triggers[] = "CREATE OR REPLACE FUNCTION {$functionNameEscaped}() RETURNS TRIGGER AS $$
                                BEGIN
                                    IF NEW.{$columnNameEscaped} IS NULL THEN
                                        NEW.{$columnNameEscaped} := FLOOR(EXTRACT(EPOCH FROM NOW()));
                                    END IF;
                                    RETURN NEW;
                                END;
                                $$ language 'plpgsql';";
                            $triggers[] = "CREATE TRIGGER {$triggerNameEscaped}
                                BEFORE INSERT ON {$tableNameEscaped}
                                FOR EACH ROW EXECUTE PROCEDURE {$functionNameEscaped}()";
                            break;
                    }
                }


                // If we have a valid identifier for the default, use it. (For instance, __TIME__ could be CURRENT_TIMESTAMP.)
                elseif (isset($this->sqlInterface->defaultPhrases[$column['default']])) {
                    $typePiece .= ' DEFAULT ' . $this->sqlInterface->defaultPhrases[$column['default']];
                }


                // Finally, just normal default constants.
                else {
                    // If we have transformation parameters set for the column, transform our default value first.
                    if (@isset($this->encode[$tableName][$columnName])) {
                        list($function, $typeOverride) = $this->encode[$tableName][$columnName];

                        $column['default'] = $this->applyTransformFunction($function, $column['default'], $typeOverride);
                    }
                    else {
                        $column['default'] = new Type($column['type'], $column['default']);
                    }

                    $typePiece .= ' DEFAULT '
                        . $this->formatValue($column['default']->type === Type\Type::enum
                            ? Type\Type::string
                            : $column['default']->type
                        , $column['default']->value);
                }
            }


            /* Generate COMMENT ON Statements, If Needed */
            if ($column['comment']) {
                switch ($this->sqlInterface->commentMode) {
                    case 'useCommentOn':
                        $triggers[] = $this->returnQueryString()->createTableColumnComment($tableName, $columnName, $column['comment']);
                    break;

                    case 'useAttributes':
                        $typePiece .= ' COMMENT ' . $this->formatValue(Type\Type::string, $column['comment']);
                    break;
                }
            }


            /* Generate Foreign Key Restrictions */
            if ($this->isTypeObject($column['restrict'])) {
                if ($column['restrict']->type === Type\Type::tableColumn) {
                    if ($this->sqlInterface->foreignKeyMode) {
                        if ($returnedQuery = $this->returnQueryString()->deleteForeignKeyConstraintFromColumnName($tableName, $columnName))
                            $triggers[] = $returnedQuery;

                        $triggers[] = $this->returnQueryString()->createForeignKeyConstraint($tableName, $columnName, $column['restrict']->value[0], $column['restrict']->value[1]);
                    }
                }
                else {
                    throw new Exception('$column[\'restrict\'] must be an instance of DatabaseType(Database\Type\Type::tableColumn).');
                }
            }


            /* Put it All Together As an SQL Statement Piece */
            $columns[] = $this->formatValue(Type\Type::column, $columnName)
                . ' ' . $typePiece;
        }

        return [$columns, $triggers, $tableIndexes, $tableProperties];
    }


    private function parseSelectColumns($tableCols) {
        if (is_array($tableCols))
            return $tableCols;

        elseif (is_string($tableCols)) { // Table columns have been defined with a string list, e.g. "a,b,c"
            $columnArray = [];

            $colParts = explode(',', $tableCols); // Split the list into an array, delimited by commas

            foreach ($colParts AS $colPart) { // Run through each list item
                $colPart = trim($colPart); // Remove outside whitespace from the item

                if (strpos($colPart, ' ') !== false) { // If a space is within the part, then the part is formatted as "columnName columnAlias"
                    $colPartParts = explode(' ', $colPart); // Divide the piece

                    $colPartName = $colPartParts[0]; // Set the name equal to the first part of the piece
                    $colPartAlias = $colPartParts[1]; // Set the alias equal to the second part of the piece
                }
                else { // Otherwise, the column name and alias are one in the same.
                    $colPartName = $colPart; // Set the name and alias equal to the piece
                    $colPartAlias = $colPart;
                }

                $columnArray[$colPartName] = $colPartAlias;
            }

            return $columnArray;
        }

        else
            throw new Exception('Unrecognised table column format.');
    }


    /**
     * Creates a new table with given properties.
     *
     * @param $tableName       string The table to alter.
     * @param $tableComment    string A new comment for the table.
     * @param $engine          string A new engine for the table.
     * @param $tableColumns    array The table's columns. See {@link DatabaseSQL::parseTableColumns}} for formatting.
     * @param $tableIndexes    array A new engine for the table. See {@link DatabaseSQL::createTableIndexes}} for formatting.
     * @param $partitionColumn string|bool A new partition column for the table.
     * @param hardPartitionCount int The number of manual table instances that should be created, using the DBAL's hard partitioning scheme.
     * @param $renameOrDeleteExisting bool If true, any existing table with this table name will be renamed or (if rename fails) deleted.
     *
     * @return bool True on success, false on failure.
     * @throws Exception
     */
    public function createTable($tableName,
                                $tableComment,
                                $engine,
                                $tableColumns,
                                $tableIndexes = [],
                                $partitionColumn = false,
                                $hardPartitionCount = 1,
                                $renameOrDeleteExisting = false)
    {
        /* Perform CREATEs */
        $this->startTransaction();

        $return = true;
        for ($i = 0; $i < $hardPartitionCount; $i++) {
            $tableNameI = $tableName . ($hardPartitionCount > 1 ? "__part$i" : '');


            /* Rename/Delete Existing
             * If we are overwriting an existing table, try renaming, then deleting it. */
            if ($renameOrDeleteExisting && in_array(strtolower($tableNameI), $this->getTablesAsArray())) {
                if (!$this->renameTable($tableNameI, $tableNameI . '~' . time())) {
                    if (!$this->deleteTable($tableNameI)) {
                        throw new Exception("Could Not Rename or Delete Table '$tableNameI'");
                    }
                }
            }

            /* If we don't want to overwrite an existing table, and we can't rely on IF NOT EXISTS in the CREATE TABLE statement.
             * then check to see if an existing table exists and skip if it does. */
            else if (!$this->sqlInterface->useCreateIfNotExist && in_array(strtolower($tableName), $this->getTablesAsArray())) {
                continue;
            }


            /* Parse Columns and Indexes */
            list($columns, $triggers, $tableIndexes, $tableProperties) = $this->parseTableColumns($tableNameI, $tableColumns, $tableIndexes, $engine);
            list($indexes, $indexTriggers) = $this->createTableIndexes($tableNameI, $tableIndexes, true);

            $triggers = array_merge($triggers, $indexTriggers);


            /* Table Comments */
            // In this mode, we add comments with separate SQL statements at the end.
            switch ($this->sqlInterface->commentMode) {
                case 'useCommentOn':
                    $triggers[] = $this->returnQueryString()->createTableComment($tableNameI, $tableComment);
                    break;

                case 'useAttributes':
                    $tableProperties .= " COMMENT=" . $this->formatValue(Type\Type::string, $tableComment);
                    break;
            }


            /* Table Engine
             * Currently, only MySQL supports different engines. */
            if ($this->sqlInterface->getLanguage() === 'mysql') {
                $tableProperties .= ' ENGINE=' . $this->formatValue(Type\Type::string, $this->sqlInterface->tableTypes[$engine]);
            }

            /* TODO: a lot more is needed to make this work with SqlServer, but this would be the beginning.
            elseif ($this->sqlInterface->getLanguage() === 'sqlsrv' && $engine === DatabaseEngine::memory) {
                $tableProperties .= 'WITH(MEMORY_OPTIMIZED=ON, DURABILITY=SCHEMA_ONLY)';
            }
            */


            /* Table Charset
             * PgSQL specifies charset when DB is created, so that's up to the enduser. */
            if ($this->sqlInterface->getLanguage() === 'mysql') {
                $tableProperties .= ' DEFAULT CHARSET=' . $this->formatValue(Type\Type::string, 'utf8');
            }


            /* Table partitioning */
            if ($partitionColumn
                && $this->sqlInterface->usePartition) {
                $tableProperties .= ' PARTITION BY HASH(' . $this->formatValue(Type\Type::column, $partitionColumn) . ') PARTITIONS 100';
            }

            $return = $this->rawQuery(
                'CREATE '
                . ($this->sqlInterface->getLanguage() === 'pgsql' && $engine === Engine::memory
                    ? 'UNLOGGED '
                    : '')
                . 'TABLE '
                . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableNameI)
                . ' ('
                . implode(", ", $columns)
                . (count($indexes) > 0
                    ? ',' . implode(", ", $indexes)
                    : '')
                . ')'
                . $tableProperties
            );

            $return = $return &&
                $this->executeTriggers($triggers);
        }

        $this->endTransaction();

        return $return;
    }


    /**
     * Changes the name of a table, and deletes all foreign key constraints attached to it.
     *
     * @param $oldName string The current table name, to be changed.
     * @param $newName string The new table name.
     *
     * @return bool True on success, false on failure.
     */
    public function renameTable($oldName, $newName)
    {
        switch ($this->sqlInterface->tableRenameMode) {
            case 'renameTable':
                return $this->deleteForeignKeyConstraints($oldName) &&
                    $this->rawQuery('RENAME TABLE '
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $oldName)
                        . ' TO '
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $newName)
                    );
                break;

            case 'alterTable':
                return $this->deleteForeignKeyConstraints($oldName) &&
                    $this->rawQuery('ALTER TABLE '
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $oldName)
                        . ' RENAME TO '
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $newName)
                    );
                break;
        }

        return $this->rawQuery(false);
    }


    /**
     * Changes the table comment, engine, or partition column of a table.
     *
     * @param $tableName string The table to alter.
     * @param $tableComment string A new comment for the table.
     * @param $engine string A new engine for the table.
     * @param $partitionColumn string|false A new partition column for the table.
     *
     * @return bool True on success, false on failure.
     */
    public function alterTable($tableName, $tableComment, $engine, $partitionColumn = false) {
        return $this->rawQuery('ALTER TABLE ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
            . (!is_null($engine) && $this->sqlInterface->getLanguage() === 'mysql' ? ' ENGINE=' . $this->formatValue(Type\Type::string, $this->sqlInterface->tableTypes[$engine]) : '')
            . (!is_null($tableComment) ? ' COMMENT=' . $this->formatValue(Type\Type::string, $tableComment) : '')
            . ($partitionColumn ? ' PARTITION BY HASH(' . $this->formatValue(Type\Type::column, $partitionColumn) . ') PARTITIONS 100' : ''));
    }


    /**
     * Deletes a table.
     *
     * @param $tableName string The table to delete.
     *
     * @return bool True on success, false on failure.
     */
    public function deleteTable($tableName)
    {
        return $this->deleteForeignKeyConstraints($tableName)
            && $this->rawQuery('DROP TABLE IF EXISTS '
                . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
            );
    }


    /**
     * Adds columns to a table.
     *
     * @param $tableName string The table to create columns on.
     * @param $tableColumns array The columns to create. See {@link DatabaseSQL::parseTableColumns} for the format to use.
     * @param $engine string The engine of the table the columns will be created in.
     *
     * @return bool True on success, false on failure.
     */
    public function createTableColumns($tableName, $tableColumns, $engine = Engine::general) {
        list ($columns, $triggers, $tableIndexes) = $this->parseTableColumns($tableName, $tableColumns, null, $engine);

        array_walk($columns, function(&$column) { $column = 'ADD ' . $column; });

        return $this->rawQuery('ALTER TABLE '
                . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                . ' '
                . implode($columns, ', ')
            )
            && $this->executeTriggers($triggers);
    }


    /**
     * Modifies columns in a table to have new properties.
     *
     * @param $tableName string The table containing the columns.
     * @param $tableColumns array The columns to update. See {@link DatabaseSQL::parseTableColumns} for the format to use.
     * @param $engine string The engine of the table.
     *
     * @return bool True on success, false on failure.
     */
    public function alterTableColumns($tableName, $tableColumns, $engine = Engine::general) {
        list ($columns, $triggers, $tableIndexes) = $this->parseTableColumns($tableName, $tableColumns, null, $engine);

        array_walk($columns, function(&$column) { $column = 'MODIFY ' . $column; });

        return $this->rawQuery('ALTER TABLE '
                . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                . ' '
                . implode($columns, ', ')
            )
            && $this->executeTriggers($triggers);
    }


    /**
     * Creates new indexes in a table. This has the following known limitations:
     * * On PgSQL, fulltext indexes must not reference more than one column.
     * * On SQLServer, the primary key must come before any fulltext indexes, and a primary key MUST be specified.
     *
     * @param string $tableName
     * @param array  $tableIndexes {
     *     An array of table column properties indexed by a comma-seperated list of columns the table index indexes. Valid parameters:
     *
     *     'type'          string           The type of the index, some value from DatabaseIndexType.
     *     'storage'       string           How the index should be stored, some value from DatabaseIndexStorage.
     * }
     * @param bool   $duringTableCreation When true, this will return index statements to be used with table created.
     *
     * @return bool|array When $duringTableCreation is false, this returns true on success, false on failure. When $duringTableCreation is true, this returns a list of index components to use with the table creation and a list of triggers to run after the table is created.
     * @throws Exception when an index has an invalid index mode.
     */
    public function createTableIndexes($tableName, $tableIndexes, $duringTableCreation = false)
    {

        $triggers = [];
        $indexes = [];


        // Identify primary key, for use with SqlServer
        $primaryKey = false;
        foreach ($tableIndexes AS $indexName2 => $index2) {
            if ($index2['type'] === Index\Type::primary)
                $primaryKey = $this->getIndexName($tableName, $indexName2);
        }


        // Create each index
        foreach ($tableIndexes AS $indexName => $index) {
            /* Default to normal index if type is invalid. */
            if (!isset($this->sqlInterface->keyTypeConstants[$index['type']])) {
                $this->triggerError("Unrecognised Index Type", array(
                    'tableName' => $tableName,
                    'indexName' => $indexName,
                    'indexType' => $index['type'],
                ), 'validationFallback');
                $index['type'] = 'index';
            }

            /* Generate CREATE INDEX Statements, If Needed */
            if ((!$duringTableCreation || $this->sqlInterface->indexMode === 'useCreateIndex')
                && $index['type'] !== Index\Type::primary) {

                // Delete any old index with the given name
                $triggers[] = $this->returnQueryString()->deleteIndex($tableName, $indexName);

                // Add the CREATE INDEX statement to the triggers.
                $triggers[] = $this->returnQueryString()->createIndex($tableName, $indexName, $index['type'], $index['storage'] ?? '', $index['comment'] ?? '', $primaryKey);
            }

            // If we are in useTableAttribute index mode and this is during table creation, or the index is primary, prepare to return the index statement.
            elseif (
                ($duringTableCreation
                    && $this->sqlInterface->indexMode === 'useTableAttribute')
                || $index['type'] === Index\Type::primary
            ) {

                $alteredIndexName = $this->getIndexName($tableName, $indexName);

                /* Build the Index Statement */
                $indexStatement = "";

                // Use CONSTRAINT syntax if the index is a primary key (we do this so we can reference the primary key by name as a constraint)
                if ($index['type'] === Index\Type::primary)
                    $indexStatement .= " CONSTRAINT "
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_INDEX, $alteredIndexName)
                        . ' ';

                // Append the index type
                $indexStatement .= $this->sqlInterface->keyTypeConstants[$index['type']] . " KEY ";

                // Append the index name if it's not a primary key
                if ($index['type'] !== Index\Type::primary)
                    $indexStatement .= $this->formatValue(DatabaseSQL::FORMAT_VALUE_INDEX, $alteredIndexName);

                // Append the list of columns
                $indexStatement .= $this->formatValue(Type\Type::arraylist, $this->getIndexColsFromIndexName($indexName));

                // If we have storage (and are allowed to use it), use it
                if (isset($this->sqlInterface->indexStorages[$index['storage'] ?? ''])
                    && $this->sqlInterface->indexStorageOnCreate)
                    $indexStatement .= ' USING ' . $this->sqlInterface->indexStorages[$index['storage']];

                // Index comments
                if (isset($index['comment']) && $index['comment']) {
                    switch ($this->sqlInterface->commentMode) {
                        case 'useAttributes':
                            $indexStatement .= ' COMMENT ' . $this->formatValue(Type\Type::string, $index['comment']);
                        break;

                        case 'useCommentOn':
                            $triggers[] = $this->returnQueryString()->createIndexComment($alteredIndexName, $index['comment']);
                        break;
                    }
                }


                /* Add the Index Statement to the List */
                $indexes[] = $indexStatement;

            }

            // Throw an exception if the index mode is unrecognised.
            else
                throw new Exception("Invalid index mode: {$this->sqlInterface->indexMode}");
        }


        return $duringTableCreation
            ? [$indexes, $triggers]
            : $this->executeTriggers($triggers);
    }


    /**
     * Deletes an existing table index. Will do nothing if the index does not exist.
     *
     * @param $tableName string The table with a foreign key constraint.
     * @param $constraintName string The foreign key constraint's name.
     *
     * @return bool True on success, false on failure.
     */
    public function deleteIndex($tableName, $indexName)
    {

        $alteredIndexName = $this->getIndexName($tableName, $indexName);

        if ($this->sqlInterface->useDropIndexIfExists) {
            return $this->rawQuery('DROP INDEX IF EXISTS '
                . $this->formatValue(DatabaseSQL::FORMAT_VALUE_INDEX, $alteredIndexName)
                . ($this->sqlInterface->perTableIndexes ?
                    ' ON '
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                    : ''
                )
            );
        }
        else {
            $tableIndexes = $this->getTableIndexesAsArray();

            if (isset($tableIndexes[strtolower($tableName)]) && in_array($alteredIndexName, $tableIndexes[strtolower($tableName)])) {
                return $this->rawQuery('DROP INDEX '
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_INDEX, $alteredIndexName)
                    . ($this->sqlInterface->perTableIndexes ?
                        ' ON '
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                        : ''
                ));
            }

            return $this->rawQuery(false);
        }

    }

    /**
     * Create a new index on a table.
     *
     * @param $tableName string The table to create a new index on.
     * @param $indexName string The name of the index to create.
     * @param $indexType string The type of index, some value in {@see Index\Type}.
     * @param $indexStorage string The storage method for the index, some value in {@see Index\Storage}.
     * @param $indexComment string A comment for the index.
     * @param $primaryKey string The primary key that already exists in the table, if any; this is solely used when creation SQL Server full text indexes.
     *
     * @return bool True on success, false on failure.
     */
    public function createIndex($tableName, $indexName, $indexType = Index\Type::index, $indexStorage = '', $indexComment = '', $primaryKey = null)
    {

        $triggers = [];

        // Transfrom the index name into one that is unique to the database.
        $alteredIndexName = $this->getIndexName($tableName, $indexName);

        // Get the columns referenced by an index name.
        $indexCols = $this->getIndexColsFromIndexName($indexName);



        /* CREATE x INDEX ON table */
        // Begin the index statement
        $indexStatement = "CREATE " . $this->sqlInterface->keyTypeConstants[$indexType] . " INDEX";

        // Unless we're a fulltext index on SQL Server, add the index name.
        if (!($indexType === Index\Type::fulltext
            && $this->sqlInterface->getLanguage() === 'sqlsrv')) {
            $indexStatement .= ' ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_INDEX, $alteredIndexName);
        }

        // Add the ON table_name
        $indexStatement .= " ON "
            . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName);

        // Add PgSQL GIN indexes for fulltext, if applicable (TODO: this doesn't support multi-column fulltext indexes.)
        if ($indexType === Index\Type::fulltext
            && $this->sqlInterface->getLanguage() === 'pgsql') {
            $indexStatement .= ' USING GIN (to_tsvector(\'english\', ' . $this->formatValue(Type\Type::column, $indexName) . '))';
        }

        // Add the list of columns unless we're a PgSQL GIN index
        else {
            $indexStatement .= ' '
                . $this->formatValue(Type\Type::arraylist, $indexCols);
        }

        // Add a USING storage_type clause of a specific storage engine is available
        if (isset($this->sqlInterface->indexStorages[$indexStorage])) {
            $indexStatement .= " USING " . $this->sqlInterface->indexStorages[$indexStorage];
        }

        // SqlSrv: WHERE NOT NULL (allow multiple nulls in SqlSrv unique indexes)
        if ($indexType === Index\Type::unique
            && $this->sqlInterface->getLanguage() === 'sqlsrv') {
            $indexColsConditions = [];
            foreach ($indexCols AS $col) {
                $indexColsConditions["!{$col->value}"] = $this->type(Type\Type::null);
            }

            $indexStatement .= ' WHERE ' . $this->recurseBothEither($indexColsConditions, $this->reverseAliasFromConditionArray($tableName, $indexColsConditions), 'both');
        }

        // SqlSrv: KEY INDEX for full text indexes
        if ($indexType === Index\Type::fulltext
            && $this->sqlInterface->getLanguage() === 'sqlsrv') {
            $indexStatement .= ' KEY INDEX ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_INDEX, $primaryKey);
        }

        // Create comments

        // Create Comment Trigger, if Needed
        if ($indexComment) {
            switch ($this->sqlInterface->commentMode) {
                case 'useAttributes':
                    $indexStatement .= ' COMMENT ' . $this->formatValue(Type\Type::string, $indexComment);
                break;

                case 'useCommentOn':
                    //disabled because the order doesn't work with PDO
                    //$triggers[] = $this->returnQueryString()->createIndexComment($alteredIndexName, $indexComment);
                break;
            }
        }


        return $this->rawQuery($indexStatement);

        // doesn't quite work with PDO
        /*if ($query = $this->rawQuery($indexStatement)) {
            $this->executeTriggers($triggers);
            return $query;
        };*/
    }

    private function getIndexName($tableName, $indexName) : string
    {
        return "i_{$tableName}_{$indexName}";
    }


    /**
     * Pull out columns from index name.
     * If an index name is comma-seperated, it is using multiple columns.
     */
    private function getIndexColsFromIndexName($indexName) : array
    {

        if (strpos($indexName, ',') !== false) {
            $indexCols = explode(',', $indexName);

            foreach ($indexCols AS &$indexCol) {
                $indexCol = $this->col($indexCol);
            }

            return $indexCols;
        }

        else {
            return [$this->col($indexName)];
        }

    }



    /**
     * Creates a foreign key constraint on a table.
     *
     * @param $tableName string The table to have a foreign key constraint on.
     * @param $tableName string The column to have a foreign key constraint on.
     * @param $foreignTableName string The foreign table to be referenced.
     * @param $foreignColumnName string The foreign column to be referenced.
     *
     * @return bool True on success, false on failure.
     */
    public function createForeignKeyConstraint($tableName, $columnName, $foreignTableName, $foreignColumnName)
    {
        $constraintName = 'fk_' . $tableName . '_' . $columnName;

        if ($this->sqlInterface->foreignKeyMode) {
            return $this->rawQuery('ALTER TABLE '
                . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                . ' ADD CONSTRAINT ' . $this->formatValue(Type\Type::column, $constraintName) . ' FOREIGN KEY ('
                . $this->formatValue(Type\Type::column, $columnName)
                . ') REFERENCES ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $foreignTableName) . '('
                . $this->formatValue(Type\Type::column, $foreignColumnName)
                . ')'
            );
        }

        return false;
    }


    /**
     * Deletes all existing foreign key constraints on a single table.
     *
     * @param $tableName string The table whose foreign key constraints will be dropped.
     *
     * @return bool True on success, false on failure.
     */
    public function deleteForeignKeyConstraints($tableName) {
        $return = true;
        $tableConstraints = $this->getTableConstraintsAsArray();

        if (isset($tableConstraints[strtolower($tableName)])) {
            foreach ($tableConstraints[strtolower($tableName)] AS $constraintName) {
                $return = $this->deleteForeignKeyConstraint($tableName, $constraintName)
                    && $return;
            }
        }

        return $return;
    }


    /**
     * Deletes an existing foreign key constraint. Will do nothing if the constraint does not exist.
     *
     * @param $tableName string The table with a foreign key constraint.
     * @param $constraintName string The foreign key constraint's name.
     *
     * @return bool True on success, false on failure.
     */
    public function deleteForeignKeyConstraint($tableName, $constraintName)
    {
        $tableConstraints = $this->getTableConstraintsAsArray();

        if (!$this->sqlInterface->foreignKeyMode) {
            return $this->rawQuery(false);
        }

        if (isset($tableConstraints[strtolower($tableName)]) && in_array($constraintName, $tableConstraints[strtolower($tableName)])) {
            switch ($this->sqlInterface->foreignKeyMode) {
                case 'useAlterTableForeignKey':
                    return $this->rawQuery('ALTER TABLE '
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                        . ' DROP FOREIGN KEY '
                        . $this->formatValue(Type\Type::column, $constraintName));

                case 'useAlterTableConstraint':
                    return $this->rawQuery('ALTER TABLE '
                        . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                        . ' DROP CONSTRAINT '
                        . $this->formatValue(Type\Type::column, $constraintName)
                        . ' CASCADE ');
                    break;
            }
        }

        return $this->rawQuery(false);
    }


    /**
     * Deletes an existing foreign key constraint named based on table and column name. Will do nothing if the constraint does not exist.
     *
     * @param $tableName string The table with a foreign key constraint.
     * @param $columnName string The column with a foreign key constraint.
     *
     * @return bool True on success, false on failure.
     */
    public function deleteForeignKeyConstraintFromColumnName($tableName, $columnName)
    {
        return $this->deleteForeignKeyConstraint($tableName, 'fk_' . $tableName . '_' . $columnName);
    }


    /**
     * Adds a comment to a table column.
     *
     * @param $tableName string The table containing the column.
     * @param $columnName string The column to add a comment to.
     *
     * @return bool True on success, false on failure.
     */
    public function createTableColumnComment($tableName, $columnName, $comment)
    {
        return $this->rawQuery('COMMENT ON COLUMN '
            . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN, $tableName, $columnName)
            . ' IS '
            . $this->formatValue(Type\Type::string, $comment)
        );
    }


    /**
     * Adds a comment to an index.
     *
     * @param $indexName string The index to add a comment to.
     *
     * @return bool True on success, false on failure.
     */
    public function createIndexComment($indexName, $comment)
    {
        return $this->rawQuery('COMMENT ON INDEX '
            . $this->formatValue(DatabaseSQL::FORMAT_VALUE_INDEX, $indexName)
            . ' IS '
            . $this->formatValue(Type\Type::string, $comment)
        );
    }


    /**
     * Adds a comment to a table.
     *
     * @param $tableName string The table to add a comment to.
     *
     * @return bool True on success, false on failure.
     */
    public function createTableComment($tableName, $comment)
    {
        return $this->rawQuery('COMMENT ON TABLE '
            . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
            . ' IS '
            . $this->formatValue(Type\Type::string, $comment)
        );
    }


    /**
     * Run a series of SQL statements in sequence, returning true if all run successfully.
     * If {@link holdTriggers} is enabled, the trigger statements will instead by placed in {@link triggerQueue}
     *
     * @param $tableName string The SQL tablename.
     * @param $triggers array List of SQL statements.
     *
     * @return bool true if all queries successful, false if any fails
     */
    public function executeTriggers($triggers)
    {
        $return = true;

        foreach ((array) $triggers AS $triggerText) {
            if ($this->holdTriggers || $this->returnQueryString) {
                $this->triggerQueue[] = $triggerText;
            }
            else {
                $return = $return
                    && $this->rawQuery($triggerText); // Make $return false if any query return false.
            }
        }

        return $return;
    }


    /**
     * @return array The current SQL statements set to run when {@see DatabaseSQL::holdTriggers()} is set to off.
     */
    public function getTriggerQueue()
    {
        return $this->triggerQueue;
    }


    /**
     * @return array A list of tables in the current database.
     */
    public function getTablesAsArray(): array
    {
        return array_map('strtolower', $this->sqlInterface->getTablesAsArray($this));
    }


    /**
     * @return array The table columns in the current database, grouped by table.
     */
    public function getTableColumnsAsArray(): array
    {
        return array_change_key_case(
            $this->sqlInterface->getTableColumnsAsArray($this), CASE_LOWER
        );
    }


    /**
     * @return array The table constraints in the current database, grouped by table.
     */
    public function getTableConstraintsAsArray(): array
    {
        return array_change_key_case(
            $this->sqlInterface->getTableConstraintsAsArray($this), CASE_LOWER
        );
    }


    /**
     * @return array The table indexes in the current database, grouped by table.
     */
    public function getTableIndexesAsArray(): array
    {
        return array_change_key_case(
            $this->sqlInterface->getTableIndexesAsArray($this), CASE_LOWER
        );
    }

    /*********************************************************
     ************************* END ***************************
     ******************* Table Functions *********************
     *********************************************************/


    /*********************************************************
     ************************ START **************************
     ******************** Row Functions **********************
     *********************************************************/


    /**
     * @see Database::select()
     */
    public function select($columns, $conditionArray = false, $sort = false, $limit = false, $page = 0)
    {
        /* Define Variables */
        $finalQuery = array(
            'columns' => array(),
            'tables' => array(),
            'join' => array(),
            'where' => '',
            'sort' => array(),
            'group' => '',
            'limit' => 0
        );
        $reverseAlias = array();
        $joins = array();



        /* Where()/sort()/limit() overrides */
        if ($this->conditionArray) {
            if ($conditionArray) throw new Exception('Condition array declared both in where() and select().');

            $conditionArray = $this->conditionArray; $this->conditionArray = array();

        }
        if ($this->sortArray) {
            if ($sort !== false) throw new Exception("Sort array declared both in sort() and select().");

            $sort = $this->sortArray; $this->sortArray = array();
        }

        if ($this->limit) {
            if ($limit !== false) throw new Exception("Limit declared both in limit() and select().");

            $limit = $this->limit; $this->limit = false;
        }

        if ($this->page) {
            if ($page !== 0) throw new Exception("Page declared both in page() and select().");

            $page = $this->page; $this->page = 0;
        }


        /* Process Columns */
        // If columns is a string, then it is a table name, whose columns should be guessed from the other parameters. For now, this guessing is very limited -- just taking the array_keys of $conditionArray (TODO).
        if (is_string($columns)) {
            $columns = array(
                "$columns" => array_keys($conditionArray)
            );
        }

        elseif (!is_array($columns)) {
            $this->triggerError('Invalid Select Array (Columns Not String or Array)', array(), 'validation');
        }

        elseif (!count($columns)) {
            $this->triggerError('Invalid Select Array (Columns Array Empty)', array(), 'validation');
        }


        // Process $columns
        foreach ($columns AS $tableName => $tableCols) {
            // Make sure tableName is a valid string.
            if (!is_string($tableName) || !strlen($tableName)) {
                $this->triggerError('Invalid Select Array (Invalid Table Name)', array(
                    'tableName' => $tableName,
                ), 'validation');
            }


            if (strpos($tableName, 'join ') === 0) { // If the table is identified as being part of a join.
                $tableName = substr($tableName, 5);

                /*foreach ($this->parseSelectColumns($tableCols['columns']) AS $columnAlias => $columnName) {
                    $finalQuery['columns'][] = $this->formatValue(databaseSQL::FORMAT_VALUE_TABLE_COLUMN_NAME_ALIAS, $tableName, $columnAlias, $columnName);
                }*/

                $joins[$tableName] = $tableCols['conditions'];
                $tableCols = $tableCols['columns'];
            }

            elseif (strstr($tableName, ' ') !== false) { // A space can be used to create a table alias, which is sometimes required for different queries.
                $tableParts = explode(' ', $tableName);

                $finalQuery['tables'][] = $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_NAME_ALIAS, $tableParts[0], $tableParts[1]); // Identify the table as [tableName] AS [tableAlias]; note: may be removed if the table is part of a join.

                $tableName = $tableParts[1];
            }

            else {
                if (isset($this->hardPartitions[$tableName])) { // This should be used with the above stuff too, but that would really require a partial rewrite at this point, and I'm too close to release to want to do that.
                    list($column, $partitionCount) = $this->hardPartitions[$tableName];

                    if (isset($conditionArray[$column]))
                        $found = $conditionArray[$column];
                    elseif (isset($conditionArray['both'][$column]))
                        $found = $conditionArray['both'][$column];
                    else
                        $this->triggerError("Selecting from a hard partitioned table, " . $tableName . ", without the partition column, " . $column . " at the top level is unsupported. It likely won't ever be supported, since any boolean logic is likely to require cross-partition selection, which is far too complicated a feature for this DAL. Use native RDBMS partitioning for that if you can.");

                    // I'm not a fan of this hack at all, but I'd really have to rewrite to
                    $finalQuery['tables'][] = $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_NAME_ALIAS, $this->getTableNameTransformation($tableName, [$column => $found]), $tableName);
                }
                else {
                    $finalQuery['tables'][] = $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName); // Identify the table as [tableName]; note: may be removed if the table is part of a join.
                }

            }


            $tableCols = $this->parseSelectColumns($tableCols);

            foreach ($tableCols AS $colName => $colAlias) {
                if (is_int($colName)) $colName = $colAlias;

                if (strlen($colName) > 0) {
                    if (strstr($colName, ' ') !== false) { // A space can be used to create identical columns in different contexts, which is sometimes required for different queries.
                        $colParts = explode(' ', $colName);
                        $colName = $colParts[0];
                    }

                    if (is_array($colAlias)) { // Used for advance structures and function calls.
                        if (isset($colAlias['context'])) {
                            throw new Exception('Deprecated context.'); // TODO
                        }

                        $finalQuery['columns'][] = $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_NAME_ALIAS, $tableName, $colName, $colAlias['alias']);

                        $reverseAlias[$colAlias['alias']] = [$tableName, $colName];
                    }

                    else {
                        $finalQuery['columns'][] = $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_NAME_ALIAS, $tableName, $colName, $colAlias);
                        $reverseAlias[$colAlias] = [$tableName, $colName];
                    }
                }

                else {
                    $this->triggerError('Invalid Select Array (Empty Column Name)', array(
                        'tableName' => $tableName,
                        'columnName' => $colName,
                    ), 'validation');
                }
            }
        }


        /* Process Conditions (Must be Array) */
        if (is_array($conditionArray) && count($conditionArray)) {
            $finalQuery['where'] = $this->recurseBothEither($conditionArray, $reverseAlias, 'both');
        }


        /* Process Joins */
        if (count($joins) > 0) {
            foreach ($joins AS $table => $join) {
                $finalQuery['join'][] = ' LEFT JOIN '
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $table)
                    . ' ON '
                    . $this->recurseBothEither($join, $reverseAlias);
            }
        }



        /* Process Sorting (Must be Array) */
        if ($sort !== false) {
            if (is_string($sort)) {
                $sortParts = explode(',', $sort); // Split the list into an array, delimited by commas
                $sort = [];

                foreach ($sortParts AS $sortPart) { // Run through each list item
                    $sortPart = trim($sortPart); // Remove outside whitespace from the item

                    if (strpos($sortPart,' ') !== false) { // If a space is within the part, then the part is formatted as "columnName direction"
                        $sortPartParts = explode(' ', $sortPart); // Divide the piece

                        $sortColumn = $sortPartParts[0]; // Set the name equal to the first part of the piece
                        $directionSym = $sortPartParts[1];
                    }
                    else { // Otherwise, we assume asscending
                        $sortColumn = $sortPart; // Set the name equal to the sort part.
                        $directionSym = 'asc'; // Set the alias equal to the default, ascending.
                    }

                    $sort[$sortColumn] = $directionSym;
                }
            }

            if (count($sort) > 0) {
                foreach ($sort AS $sortColumn => $direction) {
                    $sortColumn = explode(' ', $sortColumn)[0];

                    if ($direction instanceof Type) {
                        if ($direction->type == Type\Type::arraylist) {
                            if (count($direction->value) == 0) continue;

                            switch ($this->sqlInterface->getLanguage()) {
                                case 'mysql':
                                    $list = $direction->value;
                                    rsort($list);
                                    $list = array_merge([$this->col($sortColumn)], $list);

                                    $finalQuery['sort'][] = 'FIELD' . $this->formatValue(Type\Type::arraylist, $list) . ' ' . $this->sqlInterface->sortOrderDesc;
                                break;

                                case 'pgsql':
                                    $sortQuery = ' CASE ' . $this->formatValue(Type\Type::column, $sortColumn);

                                    $list = $direction->value;
                                    foreach ($list AS $listEntry) {
                                        $sortQuery .= ' WHEN '
                                            . $this->formatValue(DatabaseSQL::FORMAT_VALUE_DETECT, $listEntry)
                                            . ' THEN 1';
                                    }

                                    $sortQuery .= " ELSE 2 END";
                                    $finalQuery['sort'][] = $sortQuery;
                                break;
                            }
                        }
                        else {
                            $finalQuery['sort'][] = $this->recurseBothEither([$sortColumn => $direction], $reverseAlias) . ' ' . $this->sqlInterface->sortOrderDesc;
                        }
                    }

                    elseif (isset($reverseAlias[$sortColumn])) {
                        switch (strtolower($direction)) {
                            case 'asc': $directionSym = $this->sqlInterface->sortOrderAsc; break;
                            case 'desc': $directionSym = $this->sqlInterface->sortOrderDesc; break;
                            default: $directionSym = $this->sqlInterface->sortOrderAsc; break;
                        }

                        $finalQuery['sort'][] = $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN, $reverseAlias[$sortColumn][0], $reverseAlias[$sortColumn][1]) . " $directionSym";
                    }

                    else {
                        $this->triggerError('Unrecognised Sort Column', array(
                            'sortColumn' => $sortColumn,
                        ), 'validation');
                    }
                }
            }
        }

        $finalQuery['sort'] = implode(', ', $finalQuery['sort']);



        /* Process Limit (Must be Integer) */
        if ($limit !== false) {
            if (is_int($limit)) {
                $finalQuery['limit'] = (int) $limit;
            }
        }
        $finalQuery['page'] = (int) $page;
        if ($finalQuery['page'] < 0) $finalQuery['page'] = 0;


        /* Generate Final Query */
        $finalQueryText = 'SELECT '
            . implode(', ', $finalQuery['columns'])
            . ' FROM '
            . implode(', ', $finalQuery['tables'])
            . ($finalQuery['join']
                ? implode("\n", $finalQuery['join'])
                : ''
            ) . ($finalQuery['where']
                ? ' WHERE '
                    . $finalQuery['where']
                : ''
            ) . ($finalQuery['sort']
                ? ' ORDER BY ' . $finalQuery['sort']
                : ''
            );

        if ($finalQuery['limit'])
            if ($this->sqlInterface->getLanguage() === 'sqlsrv') {
                $finalQueryText .= ' OFFSET ' .
                    ($finalQuery['limit'] * $finalQuery['page'])
                    . ' ROWS FETCH NEXT '
                    . ($finalQuery['limit'] > 1 ? $finalQuery['limit'] + 1 : $finalQuery['limit'])
                    . ' ROWS ONLY';
            }
            else {
                $finalQueryText .= ' LIMIT '
                    . ($finalQuery['limit'] > 1 ? $finalQuery['limit'] + 1 : $finalQuery['limit'])
                    . ' OFFSET ' . ($finalQuery['limit'] * $finalQuery['page']);
            }

        /* And Run the Query */
        return $this->rawQueryReturningResult($finalQueryText, $reverseAlias, $finalQuery['limit']);
    }


    /**
     * Used to perform subqueries.
     *
     * @param $columns
     * @param bool $conditionArray
     * @param bool $sort
     * @param bool $limit
     * @return bool|object|resource|string
     * @throws Exception
     */
    public function subSelect($columns, $conditionArray = false, $sort = false, $limit = false)
    {
        return $this->returnQueryString()->select($columns, $conditionArray, $sort, $limit, true);
    }
/*
    public function subJoin($columns, $conditionArray = false)
    {
        return $this->formatValue();
    }
*/


    /**
     * Recurses over a specified "where" array, returning a valid where clause.
     *
     * @param array|mixed $conditionArray - The conditions to transform into proper SQL.
     * @param array $reverseAlias - An array corrosponding to column aliases and their database counterparts.
     *
     * @return string
     * @author Joseph Todd Parsons <josephtparsons@gmail.com>
     */
    private function recurseBothEither($conditionArray, $reverseAlias = false, $type = 'both', $tableName = false)
    {
        $i = 0;

        if (!is_array($conditionArray))
            throw new Exception('Condition array must be an array.');
        elseif (!count($conditionArray))
            return $this->sqlInterface->boolValues[true] . ' = ' . $this->sqlInterface->boolValues[true];

        // $key is usually a column, $value is a formatted value for the select() function.
        foreach ($conditionArray AS $key => $value) {
            /* @var $value Type */

            $i++;

            if (strstr($key, ' ') !== false) list($key) = explode(' ', $key); // A space can be used to reference the same key twice in different contexts. It's basically a hack, but it's better than using further arrays.

            /* Key is Combiner */
            if ($key === 'both' || $key === 'either' || $key === 'neither') { // TODO: neither?
                $sideTextFull[$i] = $this->recurseBothEither($value, $reverseAlias, $key, $tableName);
            }

            /* Key is List Index, Hopefully */
            elseif (is_int($key)) {
                $sideTextFull[$i] = $this->recurseBothEither($value, $reverseAlias, 'both', $tableName);
            }

            /* Key is Column */
            else {
                // Defaults
                $sideTextFull[$i] = '';
                if (!$this->isTypeObject($value)) $value = $this->str($value);  // If value is not a DatabaseType, treat it as a string.

                // lvalue
                $column = ($this->startsWith($key, '!') ? substr($key, 1) : $key);

                // Detect Bad Column
                if (!isset($reverseAlias[$column]))
                    throw new Exception("Column '$column' cannot be queried; was it in the select fields?");


                /* Full Text Searching */
                if ($value->comparison === Comparison::fulltextSearch) {
                    switch ($this->sqlInterface->getLanguage()) {
                        case 'mysql':
                            $sideTextFull[$i] = 'MATCH (' . $this->formatValue(Type\Type::column, $column) . ') AGAINST (' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_DETECT, $value) . ' IN NATURAL LANGUAGE MODE)';
                        break;

                        case 'pgsql':
                            $sideTextFull[$i] = 'to_tsvector (\'english\', ' . $this->formatValue(Type\Type::column, $column) . ') @@ to_tsquery(\'english\', ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_DETECT, $value) . ')';
                        break;

                        case 'sqlsrv':
                            $sideTextFull[$i] = 'CONTAINS(' . $this->formatValue(Type\Type::column, $column) . ', ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_DETECT, $value) . ')';
                        break;

                        default:
                            throw new Exception('Fulltext search is unsupported in the current engine.');
                        break;
                    }
                }

                /* Normal Boolean Logic */
                else {
                    $sideText['left'] = ($reverseAlias ? $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN, $reverseAlias[$column][0], $reverseAlias[$column][1]) : $column); // Get the column definition that corresponds with the named column. "!column" signifies negation.


                    // comparison operator
                    $symbol = $this->sqlInterface->comparisonTypes[$value->comparison];


                    // rvalue
                    if ($value->type === Type\Type::column)
                        $sideText['right'] = ($reverseAlias ? $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN, $reverseAlias[$value->value][0], $reverseAlias[$value->value][1]) : $value->value); // The value is a column, and should be returned as a reverseAlias. (Note that reverseAlias should have already called formatValue)

                    elseif ($value->type === Type\Type::arraylist && count($value->value) === 0) {
                        $this->triggerError('Array nullified', false, 'validationFallback');
                        $sideTextFull[$i] = "{$this->sqlInterface->boolValues[false]} = {$this->sqlInterface->boolValues[true]}"; // Instead of throwing an exception, which should be handled above, instead simply cancel the query in the cleanest way possible. Simply specifying "false" works on most DBMS, but not SqlServer.
                        continue;
                    }

                    else {
                        // Apply transform function, if set

                        if (isset($reverseAlias[$column])) { // If we have reverse alias data for the column...
                            $transformColumnName = $reverseAlias[$column][1] ?? $column;
                            $transformTableName = $reverseAlias[$column][2] ?? $reverseAlias[$column][0]; // If the second index is set, it is storing the "original" table name, in case of a partition. Otherwise, the 0th index, containing the regular table name, is used.

                            if (isset($this->encode[$transformTableName][$transformColumnName])) { // Do we have conversion data available?
                                list($function, $typeOverride) = $this->encode[$transformTableName][$transformColumnName]; // Fetch the function used for transformation, and the type override if available.

                                $value = $this->applyTransformFunction($function, $value, $typeOverride); // Apply the transformation to the value for usage in our query.
                            }
                        }

                        // Build rvalue
                        $sideText['right'] = $this->formatValue(
                            ($value->comparison === Comparison::search ? DatabaseSQL::FORMAT_VALUE_SEARCH : $value->type),  // The value is a data type, and should be processed as such.
                            $value->value
                        );
                    }


                    // Combine l and rvalues
                    // TODO: $this->null(NOT EQUALS)
                    if ($value->type === Type\Type::null) {
                        $sideTextFull[$i] = "{$sideText['left']} IS "
                            . ($this->startsWith($key, '!')
                                ? $this->sqlInterface->concatTypes['not'] . " "
                                : ""
                            ) . "NULL";
                    }

                    elseif ((strlen($sideText['left']) > 0) && (strlen($sideText['right']) > 0)) {
                        $sideTextFull[$i] =
                            ($this->startsWith($key, '!')
                                ? $this->sqlInterface->concatTypes['not']
                                : ''
                            )
                            . "({$sideText['left']} {$symbol} {$sideText['right']}"
                            . ($value->comparison === Comparison::binaryAnd
                                ? ' = ' . $this->formatValue(Type\Type::integer, $value->value)
                                : ''
                            ) // Special case: postgres binaryAnd
                            . ")";
                    }

                    else {
                        $sideTextFull[$i] = "FALSE"; // Instead of throwing an exception, which should be handled above, instead simply cancel the query in the cleanest way possible. Here, it's specifying "FALSE" in the where clause to prevent any results from being returned.

                        $this->triggerError('Query Nullified', array('Key' => $key, 'Value' => $value, 'Side Text' => $sideText, 'Reverse Alias' => $reverseAlias), 'validationFallback');
                    }
                }
            }
        }


        if (!isset($this->sqlInterface->concatTypes[$type])) {
            $this->triggerError('Unrecognised Concatenation Operator', array(
                'operator' => $type,
            ), 'validation');
        }


        return '(' . implode($this->sqlInterface->concatTypes[$type], $sideTextFull) . ')'; // Return condition string. We wrap parens around to support multiple levels of conditions/recursion.
    }


    /**
     * Get a "reverse alias" array (for use with {@link databaseSQL::recurseBothEither()}) given a tablename and condition array.
     *
     * @param string      $tableName         The table name.
     * @param array       $conditionArray    A standard condition array; see {@link database::select()} and {@link databaseSQL::recurseBothEither()}) for more.
     * @param bool|string $originalTableName $tableName is aliased, and this is the original.
     *
     * @return array
     */
    private function reverseAliasFromConditionArray($tableName, $conditionArray, $originalTableName = false, $combineReverseAlias = []) {
        $reverseAlias = $combineReverseAlias;
        foreach ($conditionArray AS $column => $value) {
            if (substr($column, 0, 1) === '!')
                $column = substr($column, 1);

            if ($column === 'either' || $column === 'both') {
                foreach ($value AS $subValue) {
                    $reverseAlias = array_merge($reverseAlias, $this->reverseAliasFromConditionArray($tableName, $subValue));
                }
            }
            $reverseAlias[$column] = [$tableName, $column];

            // We also keep track of the original table name if it's been renamed through hard partitioning, and will use it to determine triggers.
            if ($originalTableName)
                $reverseAlias[$column][] = $originalTableName;
        }
        return $reverseAlias;
    }


    /**
     * Gets a transformed table name if hard partitioning is enabled.
     *
     * @param $tableName string The source tablename.
     * @param $dataArray array The data array that contains the partition column. Currently, advanced data arrays are not supported; the partition column must be identified by string as a top-level index on the array.
     *
     * @return string
     */
    private function getTableNameTransformation($tableName, $dataArray)
    {
        if (isset($this->hardPartitions[$tableName])) {
            return $tableName . "__part" . filter_var($this->auto($dataArray[$this->hardPartitions[$tableName][0]])->value, FILTER_SANITIZE_NUMBER_INT) % $this->hardPartitions[$tableName][1];
        }

        return $tableName;
    }


    public function insert($tableName, $dataArray)
    {
        /* Query Queueing */
        if ($this->autoQueue) {
            $this->queueInsert($this->getTableNameTransformation($tableName, $dataArray), $dataArray);
        }

        else {
            /* Collection Trigger */
            if (isset($this->collectionTriggers[$tableName])) {
                foreach ($this->collectionTriggers[$tableName] AS $params) {
                    list($triggerColumn, $aggregateColumn, $function) = $params;
                    if (isset($dataArray[$triggerColumn], $dataArray[$aggregateColumn])) {
                        call_user_func($function, $dataArray[$triggerColumn], ['insert' => [$dataArray[$aggregateColumn]]]);
                    }
                }
            }

            return $this->insertCore($tableName, [$dataArray]);
        }
    }


    /**
     * @see Database::getLastInsertId()
     */
    public function getLastInsertId()
    {
        return $this->sqlInterface->getLastInsertId();
    }


    /**
     * Perform the standard callback following insertion into a table.
     *
     * @param string $table The table that has been inserted into.
     */
    private function insertIdCallback($table)
    {
        /* Transform code for insert ID
         * If we are supposed to copy over an insert ID into a new, transformed column, we do it here. */
        if (isset($this->insertIdColumns[$table]) && isset($this->encodeCopy[$table][$this->insertIdColumns[$table]])) {
            $insertId = $this->getLastInsertId();

            list($function, $typeOverride, $column) = $this->encodeCopy[$table][$this->insertIdColumns[$table]];

            $this->update($table, [
                $column => $this->applyTransformFunction($function, $insertId, $typeOverride)
            ], [
                $this->insertIdColumns[$table] => $insertId
            ]);
        }
    }


    /**
     * Performs the core, SQL-only part of insertion, without concern for partitioning, collection triggers, etc.
     * @see DatabaseSQL::insert()
     *
     * @param string $tableName {@see DatabaseSQL::insert()}
     * @param mixed  $dataArrays {@see DatabaseSQL::insert()}
     *
     * @return bool
     * @throws Exception
     */
    private function insertCore($tableName, $dataArrays)
    {
        // Get the list of columns that composes all data arrays
        $columns = [];
        foreach ($dataArrays AS $dataArray) {
            $columns = array_merge($columns, array_diff(array_keys($dataArray), $columns));
        }

        // Rebuild the data array so that all columns are in common
        $mergedDataArrays = [];
        foreach ($dataArrays AS $dataArray) {
            $mergedDataArray = [];

            foreach($columns AS $index => $column) {
                $mergedDataArray[$index] = $dataArray[$column] ?? null;
            }

            $mergedDataArrays[] = $mergedDataArray;
        }

        // If our columns includes an IDENTITY column in SQL Server, enable identity insert.
        $serialDisabled = false;
        if ($this->sqlInterface->getLanguage() === 'sqlsrv'
            && isset($this->insertIdColumns[$tableName])
            && in_array($this->insertIdColumns[$tableName], $columns)) {
            $serialDisabled = true;
            $this->rawQuery('SET IDENTITY_INSERT ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName) . ' ON');
        }

        // Build the query
        $query = 'INSERT INTO '
            . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $this->getTableNameTransformation($tableName, $dataArray))
            . ' '
            . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_VALUES, $tableName, $columns, $mergedDataArrays);

        // Return, performing the insertIdCallback and disabling identity_insert if we enabled it.
        if ($queryData = $this->rawQuery($query)) {
            $this->insertIdCallback($tableName);

            if ($serialDisabled)
                $this->rawQuery('SET IDENTITY_INSERT ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName) . ' OFF');

            return $queryData;
        }
        else {
            return false;
        }
    }


    /**
     * @see Database::delete()
     */
    public function delete($tableName, $conditionArray = false)
    {
        $originalTableName = $tableName;

        if (isset($this->hardPartitions[$tableName])) {
            $partitionAt = array_merge($this->partitionAt, $conditionArray);

            if (!isset($partitionAt[$this->hardPartitions[$tableName][0]])) {
                $this->triggerError("The table $tableName is partitioned. To delete from it, you _must_ specify the column " . $this->hardPartitions[$tableName][0] . ". Note that you may instead use partitionAt() if you know _any_ column that would apply to the partition (for instance, if you wish to delete the last row from a table before inserting a new one, you can specify the relevant condition using partitionAt().)" . print_r($partitionAt, true));
            }

            $tableName = $this->getTableNameTransformation($tableName, $partitionAt);
        }

        $this->partitionAt = [];

        if ($this->autoQueue)
            return $this->queueDelete($tableName, $conditionArray);

        else {
            // This table has a collection trigger.
            if (isset($this->collectionTriggers[$tableName])) {

                foreach ($this->collectionTriggers[$tableName] AS $entry => $params) {
                    list($triggerColumn, $aggregateColumn, $function) = $params;

                    // Trigger column present -- that is, we are deleting data belonging to a specific list.
                    if (isset($conditionArray[$triggerColumn])) {
                        // Aggregate column present -- we will be narrowly deleting a pair of [triggerColumn, aggregateColumn]
                        if (isset($conditionArray[$aggregateColumn])) {
                            call_user_func($function, $conditionArray[$triggerColumn], ['delete' => [$conditionArray[$aggregateColumn]]]);
                        }

                        // Aggregate column NOT present -- we will be deleting the entire collection belonging to triggerColumn. Mark it for de
                        else {
                            call_user_func($function, $conditionArray[$triggerColumn], ['delete' => '*']);
                        }
                    }

                    // Trigger column not present, but the table has a collection trigger. As this is a deletion, this is too unpredictable, and we throw an error.
                    else {
                        $this->triggerError("Cannot perform deletion on " . $tableName . ", as it has a collection trigger, and you have not specified a condition for the trigger column, " . $triggerColumn);
                    }
                }
            }

            $this->deleteCore($tableName, $conditionArray, $originalTableName);
        }
    }


    /**
     * Performs the core, SQL-only part of deletion, without concern for partitioning, collection triggers, etc.
     * @see DatabaseSQL::delete()
     *
     * @param string $tableName {@see DatabaseSQL::delete()}
     * @param mixed  $conditionArray {@see DatabaseSQL::delete()}
     * @param mixed  $originalTableName If the table name has been changed for partitioning, pass this as the original table name, used for analysing the condition array.
     *
     * @return bool
     * @throws Exception
     */
    private function deleteCore($tableName, $conditionArray = false, $originalTableName = false)
    {
        return $this->rawQuery(
            'DELETE FROM ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName) .
            ($conditionArray ? ' WHERE ' . $this->recurseBothEither($conditionArray, $this->reverseAliasFromConditionArray($tableName, $conditionArray, $originalTableName), 'both', $tableName) : '')
        );
    }


    /**
     * @see Database::update()
     */
    public function update($tableName, $dataArray, $conditionArray = [])
    {
        $originalTableName = $tableName;

        if (isset($this->hardPartitions[$tableName])) {
            if (!isset($conditionArray[$this->hardPartitions[$tableName][0]])) {
                $this->triggerError("The table $tableName is partitioned. To update it, you _must_ specify the column " . $this->hardPartitions[$tableName][0] . ' ' . print_r($conditionArray, true));
            }
            elseif (isset($dataArray[$this->hardPartitions[$tableName][0]])) {
                $this->triggerError("The table $tableName is partitioned by column " . $this->hardPartitions[$tableName][0] . ". As such, you may not apply updates to this column. (...Okay, yes, it would in theory be possible to add such support, but it'd be a pain to make it portable, and is outside of the scope of my usage. Feel free to contribute such functionality.)");
            }

            $tableName = $this->getTableNameTransformation($tableName, $conditionArray);
        }


        if ($this->sqlInterface->getLanguage() === 'pgsql') {
            // Workaround for equations to use unambiguous excluded dataset.
            foreach ($dataArray AS &$dataElement) {
                if ($this->isTypeObject($dataElement) && $dataElement->type === Type\Type::equation) {
                    $dataElement->value = str_replace('$', "\${$tableName}.", $dataElement->value);
                }
            }
        }


        if ($this->autoQueue)
            return $this->queueUpdate($tableName, $dataArray, $conditionArray);
        else
            return $this->rawQuery(
                'UPDATE ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName) .
                ' SET ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_UPDATE_ARRAY, $tableName, $dataArray) .
                ' WHERE ' . $this->recurseBothEither($conditionArray, $this->reverseAliasFromConditionArray($tableName, $conditionArray, $originalTableName), 'both', $tableName)
            );
    }


    /**
     * If a row matching $conditionArray already exists, it will be updated to reflect $dataArray.
     * If it does not exist, a row will be inserted that is a composite of $conditionArray, $dataArray,
     * and $dataArrayOnInsert.
     *
     * On systems that support OnDuplicateKey, this will NOT test the existence of $conditionArray,
     * relying instead on the table's keys to do so.
     * Thus, this function's $conditionArray should always match the table's own keys.
     *
     * @param string $tableName
     * @param array $conditionArray
     * @param array $dataArray
     * @param array $dataArrayOnInsert
     *
     * @return bool
     * @throws Exception
     */
    public function upsert($tableName, $conditionArray, $dataArray, $dataArrayOnInsert = [])
    {
        if (isset($this->hardPartitions[$tableName])) {
            if (!isset($conditionArray[$this->hardPartitions[$tableName][0]])) {
                $this->triggerError("The table $tableName is partitioned. To update it, you _must_ specify the column " . $this->hardPartitions[$tableName][0]);
            }
            elseif (isset($dataArray[$this->hardPartitions[$tableName][0]])) {
                $this->triggerError("The table $tableName is partitioned by column " . $this->hardPartitions[$tableName][0] . ". As such, you may not apply updates to this column. (...Okay, yes, it would in theory be possible to add such support, but it'd be a pain to make it portable, and is outside of the scope of my usage. Feel free to contribute such functionality.)");
            }

            $tableName = $this->getTableNameTransformation($tableName, $conditionArray);
        }

        /* Query Queueing */
        if ($this->autoQueue) {
            if (!empty($dataArrayOnInsert)
                || ($this->sqlInterface->upsertMode !== 'onDuplicateKey'
                    && $this->sqlInterface->upsertMode !== 'onConflictDoUpdate')
            ) {
                return $this->upsertCore($tableName, $conditionArray, $dataArray, $dataArrayOnInsert);
            }

            return $this->queueUpsert($tableName, $conditionArray, $dataArray);
        }

        else
            return $this->upsertCore($tableName, $conditionArray, $dataArray, $dataArrayOnInsert);
    }



    /**
     * Performs the core, SQL-only part of upsertion, without concern for partitioning, collection triggers, etc.
     * @see DatabaseSQL::upsert()
     *
     * @param string $tableName {@see DatabaseSQL::insert()}
     * @param mixed  $dataArrays {@see DatabaseSQL::insert()}
     *
     * @return bool
     * @throws Exception
     */
    private function upsertCore($tableName, $conditionArray, $dataArray, $dataArrayOnInsert = [])
    {
        $allArray = array_merge($dataArray, $dataArrayOnInsert, $conditionArray);
        $allColumns = array_keys($allArray);
        $allValues = array_values($allArray);

        switch ($this->sqlInterface->upsertMode) {
            case 'onDuplicateKey':
                $query = 'INSERT INTO ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_VALUES, $tableName, $allColumns, [$allValues])
                    . ' ON DUPLICATE KEY UPDATE ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_UPDATE_ARRAY, $tableName, $dataArray);
            break;

            case 'onConflictDoUpdate':
                // Workaround for equations to use unambiguous excluded dataset.
                foreach ($dataArray AS &$dataElement) {
                    if ($this->isTypeObject($dataElement) && $dataElement->type === Type\Type::equation) {
                        $dataElement = $this->equation(str_replace('$', 'excluded.$', $dataElement->value)); // We create a new one because we don't want to update the one pointed to in allArray.
                    }
                }

                $query = 'INSERT INTO ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_VALUES, $tableName, $allColumns, [$allValues])
                    . ' ON CONFLICT '
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_COLUMN_ARRAY, array_keys($conditionArray))
                    . ' DO UPDATE SET ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_UPDATE_ARRAY, $tableName, $dataArray);
            break;

            case 'selectThenInsertOrUpdate':
                if ($this->select([$tableName => array_keys($conditionArray)], $conditionArray)->getCount() > 0) {
                    return $this->update($tableName, $dataArray, $conditionArray);
                }
                else {
                    return @$this->insert($tableName, $allArray);
                }
            break;

            case 'tryCatch':
                try {
                    return $this->insert($tableName, $allArray);
                } catch (Exception $ex) {
                    return $this->update($tableName, $dataArray, $conditionArray);
                }
            break;

            default:
                throw new \Exception('Unrecognised upsert mode: ' . $this->sqlInterface->upsertMode);
        }

        if ($queryData = $this->rawQuery($query)) {
            $this->insertIdCallback($tableName);

            return $queryData;
        }
        else return false;
    }


    /**
     * Performs the core, SQL-only part of upsertion, without concern for partitioning, collection triggers, etc.
     * @see DatabaseSQL::upsert()
     *
     * @param string $tableName {@see DatabaseSQL::insert()}
     * @param mixed  $dataArrays {@see DatabaseSQL::insert()}
     *
     * @return bool
     * @throws Exception
     */
    private function upsertCoreMulti($tableName, $updateCondition, $updateColumns, $dataArrays)
    {
        // Get the list of columns that composes all data arrays
        $columns = [];
        foreach ($dataArrays AS $dataArray) {
            $columns = array_merge($columns, array_diff(array_keys($dataArray), $columns));
        }

        // Rebuild the data array so that all columns are in common
        $mergedDataArrays = [];
        foreach ($dataArrays AS $dataArray) {
            $mergedDataArray = [];

            foreach($columns AS $index => $column) {
                $mergedDataArray[$index] = $dataArray[$column] ?? null;
            }

            $mergedDataArrays[] = $mergedDataArray;
        }

        switch ($this->sqlInterface->upsertMode) {
            case 'onDuplicateKey':
                $updateConditions = [];
                foreach ($updateColumns AS $updateColumn) {
                    $updateConditions[] = $this->formatValue(Type\Type::column, $updateColumn) . ' = VALUES(' . $this->formatValue(Type\Type::column, $updateColumn) . ')';
                }

                $query = 'INSERT INTO ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_VALUES, $tableName, $columns, $mergedDataArrays)
                    . ' ON DUPLICATE KEY UPDATE ' . implode($this->sqlInterface->arraySeperator, $updateConditions);
            break;

            case 'onConflictDoUpdate':
                $updateConditions = [];
                foreach ($updateColumns AS $updateColumn) {
                    $updateConditions[] = $this->formatValue(Type\Type::column, $updateColumn) . ' = excluded.' . $this->formatValue(Type\Type::column, $updateColumn) . ')';
                }

                $query = 'INSERT INTO ' . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE, $tableName)
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_TABLE_COLUMN_VALUES, $tableName, $columns, $mergedDataArrays)
                    . ' ON CONFLICT '
                    . $this->formatValue(DatabaseSQL::FORMAT_VALUE_COLUMN_ARRAY, $updateCondition)
                    . ' DO UPDATE SET ' . implode($this->sqlInterface->arraySeperator, $updateConditions);
            break;

            default:
                throw new Exception('Multiple upsert not supported in the current mode.');
                break;
        }

        if ($queryData = $this->rawQuery($query)) {
            $this->insertIdCallback($tableName);

            return $queryData;
        }
        else return false;
    }


    /*********************************************************
     ************************* END ***************************
     ******************** Row Functions **********************
     *********************************************************/




    /*********************************************************
     ************************ START **************************
     ******************** Queue Functions ********************
     * TODO: queue functions do not currently support simultaneous partitioning and transformations.
     *********************************************************/

    public function autoQueue(bool $on) {
        $previous = $this->autoQueue;
        $this->autoQueue = $on;

        // If we just turned autoQueue off (it wasn't off before), process all the queued calls.
        if ($previous && !$on)
            $this->processQueue();
    }

    public function queueUpdate($tableName, $dataArray, $conditionArray = false) {
        $this->updateQueue[$tableName][json_encode($conditionArray)][] = $dataArray;
    }

    public function queueDelete($tableName, $dataArray) {
        $this->deleteQueue[$tableName][] = $dataArray;
    }

    public function queueInsert($tableName, $dataArray) {
        $this->insertQueue[$tableName][] = $dataArray;
    }

    public function queueUpsert($tableName, $conditionArray, $dataArray) {
        $this->upsertQueue[$tableName][implode(',', array_keys($conditionArray))][implode(',', array_keys($dataArray))][] = array_merge($conditionArray, $dataArray);
    }


    public function processQueue() {
        $this->startTransaction();

        $triggerCallbacks = [];

        foreach ($this->deleteQueue AS $tableName => $deleteConditions) {
            // This table has a collection trigger.
            if (isset($this->collectionTriggers[$tableName])) {

                foreach ($this->collectionTriggers[$tableName] AS $entry => $params) {
                    list($triggerColumn, $aggregateColumn, $function) = $params;

                    foreach ($deleteConditions AS $deleteCondition) {
                        // Trigger column present -- that is, we are deleting data belonging to a specific list.
                        if (isset($deleteCondition[$triggerColumn])) {
                            // Don't try to add entries if the whole list has been marked for deletion.
                            if (@$triggerCallbacks[$tableName][$entry][$deleteCondition[$triggerColumn]]['delete'] === '*')
                                continue;

                            // Aggregate column present -- we will be narrowly deleting a pair of [triggerColumn, aggregateColumn]
                            elseif (isset($deleteCondition[$aggregateColumn])) {
                                @$triggerCallbacks[$tableName][$entry][$deleteCondition[$triggerColumn]]['delete'][] = $deleteCondition[$aggregateColumn];
                            }

                            // Aggregate column NOT present -- we will be deleting the entire collection belonging to triggerColumn. Mark it for de
                            else {
                                @$triggerCallbacks[$tableName][$entry][$deleteCondition[$triggerColumn]]['delete'] = '*';
                            }
                        }

                        // Trigger column not present, but the table has a collection trigger. As this is a deletion, this is too unpredictable, and we throw an error.
                        else {
                            $this->triggerError("Cannot perform deletion on " . $tableName . ", as it has a collection trigger, and you have not specified a condition for the trigger column, " . $triggerColumn);
                        }
                    }
                }
            }

            $deleteConditionsCombined = ['either' => $deleteConditions];
            $this->deleteCore($tableName, $deleteConditionsCombined);
        }
        $this->deleteQueue = [];


        foreach ($this->updateQueue AS $tableName => $update) {
            foreach ($update AS $conditionArray => $dataArrays) {
                $conditionArray = json_decode($conditionArray, true);
                $mergedDataArray = [];

                foreach ($dataArrays AS $dataArray) {
                    // The order here is important: by specifying dataArray second, later entries in the queue overwrite earlier entries in it. This is important to maintain.
                    $mergedDataArray = array_merge($mergedDataArray, $dataArray);
                }

                $this->update($tableName, $mergedDataArray, $conditionArray);
            }
        }
        $this->updateQueue = [];


        foreach ($this->insertQueue AS $tableName => $dataArrays) {
            // The table has a collection trigger
            if (isset($this->collectionTriggers[$tableName])) {
                foreach ($this->collectionTriggers[$tableName] AS $entry => $params) {
                    list($triggerColumn, $aggregateColumn, $function) = $params;

                    foreach ($dataArrays AS $dataArray) {
                        // We are inserting a specific value (aggregateColumn) into the collection (triggerColumn)
                        if (isset($dataArray[$triggerColumn], $dataArray[$aggregateColumn])) {
                            @$triggerCallbacks[$tableName][$entry][$dataArray[$triggerColumn]]['insert'][] = $dataArray[$aggregateColumn];
                        }
                    }
                }
            }

            $this->insertCore($tableName, $dataArrays);
        }
        $this->insertQueue = [];


        foreach ($this->upsertQueue AS $tableName => $upserts) {
            foreach ($upserts AS $upsertCondition => $upsertParams) {
                foreach ($upsertParams AS $upsertParam => $upsertData) {
                    $this->upsertCoreMulti($tableName, explode(',', $upsertCondition), explode(',', $upsertParam), $upsertData);
                }
            }
        }
        $this->upsertQueue = [];


        foreach ($triggerCallbacks AS $table => $collectionTriggers) {
            foreach ($collectionTriggers AS $entry => $entryPair) {
                foreach ($entryPair AS $entryValue => $dataOperations) {
                    list($triggerColumn, $aggregateColumn, $function) = $this->collectionTriggers[$table][$entry];

                    call_user_func($function, $entryValue, $dataOperations);
                }
            }
        }

        $this->endTransaction();
    }

    /*********************************************************
     ************************* END ***************************
     ******************** Queue Functions ********************
     *********************************************************/

}
