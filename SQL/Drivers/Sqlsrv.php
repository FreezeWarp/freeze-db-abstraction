<?php
namespace Database\SQL\Drivers;

use Database\ResultInterface;
use Database\Engine;
use Database\SQL\DatabaseSQL;
use Database\SQL\SQL_Definitions;
use Database\Type;

/**
 * The conventions of Microsoft's SQL Server DBMS.
 * This driver is still largely experimental; it will have quirks.
 *
 * @package Database\SQL
 */
class Sqlsrv extends SQL_Definitions {

    /**
     * @var resource
     */
    public $connection = null;

    /**
     * @var array
     */
    public $preparedParams = [];

    public $storeTypes = array(Engine::general);

    public $dataTypes = array(
        'columnIntLimits' => array(
            2 => 'TINYINT',
            4 => 'SMALLINT',
            9 => 'INT',
            'default' => 'BIGINT'
        ),

        'columnStringPermLimits' => array(
            4000 => 'NVARCHAR',
            'default' => 'NVARCHAR(MAX)'
        ),

        'columnStringTempLimits' => array(
            4000 => 'NVARCHAR',
            'default' => 'NVARCHAR(MAX)'
        ),

        'columnBlobPermLimits' => array(
            4000 => 'VARBINARY',
            'default' => 'VARBINARY(MAX)',
        ),

        'columnBlobTempLimits' => array(
            4000 => 'VARBINARY',
            'default' => 'VARBINARY(MAX)',
        ),

        'columnNoLength' => [],

        'columnBitLimits' => array(
            8  => 'TINYINT',
            16 => 'SMALLINT',
            32 => 'INTEGER',
            64 => 'BIGINT',
            'default' => 'INTEGER',
        ),

        Type\Type::float     => 'REAL',
        Type\Type::bool      => 'BIT',
        Type\Type::timestamp => 'INTEGER',
        Type\Type::blob      => 'VARBINARY(MAX)',
        Type\Type::json      => false,
    );


    /* Hex is used. */
    public $binaryQuoteStart = '';
    public $binaryQuoteEnd = '';


    public $upsertMode = 'tryCatch';
    public $enumMode = 'useCheck';
    public $indexMode = 'useCreateIndex';
    public $serialMode = 'identity';
    //foreign keys make dropping tables difficult, so they are not enabled by default
    //public $foreignKeyMode = 'useAlterTableConstraint';
    public $useDropIndexIfExists = true;

    public function connect($host, $port, $username, $password, $database = false) {
        return $this->connection = sqlsrv_connect($host, [
            "Database" => $database,
            "UID" => $username,
            "PWD" => $password
        ]);
    }

    public function selectDatabase($database)
    {
        return $this->query("USE " . $database);
    }

    public function getVersion() {
        return sqlsrv_server_info($this->connection)['SQLServerVersion'];
    }

    public function getLastError() {
        return print_r(sqlsrv_errors(), true);
    }

    public function close() {
        if (isset($this->connection)) {
            $function = @sqlsrv_close($this->connection);
            unset($this->connection);

            return $function;
        }
        else {
            return true;
        }
    }

    public function escape($text, $context) {
        switch ($context) {
            case Type\Type::string:
                return str_replace("'", "''", $text);
                break;

            case Type\Type::search:
                return ''; // TODO. We'll be adding full-text indexes, which might make this identitcal to string.
                break;

            case Type\Type::blob:
                $unpacked = unpack('H*hex', $text);
                return '0x' . $unpacked['hex'];
                break;

            default:
                return $text; // SUUUUUUPPER TODO
        }
    }

    public function query($rawQuery) {
        $query = sqlsrv_query($this->connection, $rawQuery, $this->preparedParams, [ "Scrollable" => SQLSRV_CURSOR_KEYSET ]);
        $this->preparedParams = [];
        return $query;
    }

    public function queryReturningResult($rawQuery) : ResultInterface {
        return $this->getResult($this->query($rawQuery));
    }

    public function getLastInsertId() {
        return $this->queryReturningResult('SELECT @@IDENTITY AS lastval')->fetchAsArray()['lastval'];
    }

    public function startTransaction() {
        sqlsrv_begin_transaction($this->connection);
    }

    public function endTransaction() {
        sqlsrv_commit($this->connection);
    }

    public function rollbackTransaction() {
        sqlsrv_rollback($this->connection);
    }

    protected function getResult($source) {
        return new class($source) implements ResultInterface {
            public $source;

            public function __construct($source) {
                $this->source = $source;
            }

            public function fetchAsArray() {
                return sqlsrv_fetch_array($this->source, SQLSRV_FETCH_ASSOC);
            }

            public function getCount() {
                return sqlsrv_num_rows($this->source);
            }
        };
    }

    public function getTablesAsArray(DatabaseSQL $database) {
        return $database->rawQueryReturningResult('SELECT * FROM '
            . $database->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE_TABLE, 'INFORMATION_SCHEMA', 'TABLES')
            . ' WHERE TABLE_TYPE = \'BASE TABLE\' AND TABLE_CATALOG = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
        )->getColumnValues('TABLE_NAME');
    }

    public function getTableColumnsAsArray(DatabaseSQL $database) {
        return $database->rawQueryReturningResult('SELECT * FROM '
            . $database->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE_TABLE, 'INFORMATION_SCHEMA', 'COLUMNS')
            . ' WHERE TABLE_CATALOG = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
        )->getColumnValues(['TABLE_NAME', 'COLUMN_NAME']);
    }

    public function getTableConstraintsAsArray(DatabaseSQL $database) {
        return $database->rawQueryReturningResult('SELECT * FROM '
            . $database->formatValue(DatabaseSQL::FORMAT_VALUE_DATABASE_TABLE, 'INFORMATION_SCHEMA', 'TABLE_CONSTRAINTS')
            . ' WHERE TABLE_CATALOG = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
            . ' AND CONSTRAINT_TYPE = \'FOREIGN KEY\''
        )->getColumnValues(['TABLE_NAME', 'CONSTRAINT_NAME']);
    }

    public function getTableIndexesAsArray(DatabaseSQL $database) {
        return [];
    }

    public function getLanguage() {
        return 'sqlsrv';
    }
}