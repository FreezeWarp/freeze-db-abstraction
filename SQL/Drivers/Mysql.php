<?php
namespace Database\SQL\Drivers;

use Database\ResultInterface;
use Database\SQL\MySQL_Definitions;

/**
 * The conventions of the MySQL MySQL driver.
 * This driver is deprecated, and should generally not be used. It is primarily included for testing purposes.
 *
 * @package Database\SQL
 */
class Mysql extends MySQL_Definitions {
    /**
     * @var resource
     */
    public $connection = null;

    public $lastInsertId;

    public function connect($host, $port, $username, $password, $database = false) {
        $this->connection = mysql_connect("$host:$port", $username, $password);

        $this->versionCheck();

        return $this->connection ?: false;
    }

    public function getVersion() {
        return mysql_get_server_info($this->connection);
    }

    public function getLastError() {
        return mysql_error(isset($this->connection) ? $this->connection : null);
    }

    public function close() {
        if (isset($this->connection)) {
            $function = mysql_close($this->connection);
            unset($this->connection);

            return $function;
        }
        else {
            return true;
        }
    }

    public function selectDatabase($database) {
        return mysql_select_db($database, $this->connection);
    }

    public function escape($text, $context) {
        return mysql_real_escape_string($text, $this->connection);
    }

    public function query($rawQuery, $delayExecution = false) {
        if ($delayExecution) {
            return $rawQuery;
        }
        else {
            $query = mysql_query($rawQuery, $this->connection);
            $this->lastInsertId = mysql_insert_id($this->connection) ?: $this->lastInsertId;

            return $query;
        }
    }

    public function queryReturningResult($rawQuery) : ResultInterface {
        return $this->getResult($this->query($rawQuery));
    }

    public function getLastInsertId() {
        return $this->lastInsertId;
    }

    public function startTransaction() {
        $this->query('START TRANSACTION');
    }

    public function endTransaction() {
        $this->query('COMMIT');
    }

    public function rollbackTransaction() {
        $this->query('ROLLBACK');
    }

    protected function getResult($source) : ResultInterface {
        return new class($source) implements ResultInterface {
            /**
             * @var resource The result of the query.
             */
            public $source;

            public function __construct($source) {
                $this->source = $source;
            }

            public function fetchAsArray() {
                return (($data = mysql_fetch_assoc($this->source)) === false ? false : $data);
            }

            public function getCount() {
                return mysql_num_rows($this->source);
            }
        };
    }
}