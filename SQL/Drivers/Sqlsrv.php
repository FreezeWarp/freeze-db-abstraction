<?php

namespace Database\SQL\Drivers;

use Database\ResultInterface;
use Database\SQL\DatabaseSQL;
use Database\SQL\SQLSrv_Definitions;
use Database\Type;

/**
 * The conventions of Microsoft's SQL Server DBMS.
 * This driver is still largely experimental; it will have quirks.
 *
 * @package Database\SQL
 */
class Sqlsrv extends SQLSrv_Definitions
{

    /**
     * @var resource
     */
    public $connection = null;

    /**
     * @var array
     */
    public $preparedParams = [];


    public function connect($host, $port, $username, $password, $database = false)
    {
        return $this->connection = sqlsrv_connect($host, [
            "Database" => $database,
            "UID"      => $username,
            "PWD"      => $password
        ]);
    }

    public function selectDatabase($database)
    {
        return $this->query("USE " . $database);
    }

    public function getVersion()
    {
        return sqlsrv_server_info($this->connection)['SQLServerVersion'];
    }

    public function getLastError()
    {
        return print_r(sqlsrv_errors(), true);
    }

    public function close()
    {
        if (isset($this->connection)) {
            $function = @sqlsrv_close($this->connection);
            unset($this->connection);

            return $function;
        }
        else {
            return true;
        }
    }

    public function escape($text, $context)
    {
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

    public function query($rawQuery, $delayExecution = false)
    {
        if ($delayExecution) {
            return $rawQuery;
        }
        else {
            $query = sqlsrv_query($this->connection, $rawQuery, $this->preparedParams, ["Scrollable" => SQLSRV_CURSOR_KEYSET]);
            $this->preparedParams = [];

            return $query;
        }
    }

    public function queryReturningResult($rawQuery): ResultInterface
    {
        return $this->getResult($this->query($rawQuery));
    }

    public function getLastInsertId()
    {
        return $this->queryReturningResult('SELECT @@IDENTITY AS lastval')->fetchAsArray()['lastval'];
    }

    public function startTransaction()
    {
        sqlsrv_begin_transaction($this->connection);
    }

    public function endTransaction()
    {
        sqlsrv_commit($this->connection);
    }

    public function rollbackTransaction()
    {
        sqlsrv_rollback($this->connection);
    }

    protected function getResult($source)
    {
        return new class($source) implements ResultInterface
        {
            public $source;

            public function __construct($source)
            {
                $this->source = $source;
            }

            public function fetchAsArray()
            {
                return sqlsrv_fetch_array($this->source, SQLSRV_FETCH_ASSOC);
            }

            public function getCount()
            {
                return sqlsrv_num_rows($this->source);
            }
        };
    }
}