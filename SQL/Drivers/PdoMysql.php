<?php

namespace Database\SQL\Drivers;

use Database\SQL\MySQL_Definitions;
use Database\SQL\PDO_Trait;
use Database\SQL\ReconnectOnSelectDatabase_Trait;
use PDO;
use PDOException;

/**
 * The conventions of the PDO MySQL driver.
 *
 * @package Database\SQL
 */
class PdoMysql extends MySQL_Definitions
{
    use PDO_Trait, ReconnectOnSelectDatabase_Trait;

    public $stringQuoteStart = '';
    public $stringQuoteEnd = '';
    public $binaryQuoteStart = '';
    public $binaryQuoteEnd = '';

    /**
     * @var string This is handled by escape() instead.
     */
    public $stringFuzzy = '';

    public function connect($host, $port, $username, $password, $database = false)
    {
        // keep the user and password in memory to allow for reconnects with selectdb
        $this->connectionUser = $username;
        $this->connectionPassword = $password;

        try {
            $this->connection = new PDO("mysql:" . ($database ? "dbname=$database" : '') . ";host=$host:$port", $username, $password);
            $this->connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->registerConnection($host, $port, $username, $password);

            $this->versionCheck();
        } catch (PDOException $e) {
            $this->connectionError = $e->getMessage();

            return false;
        }

        return $this->connection;
    }
}