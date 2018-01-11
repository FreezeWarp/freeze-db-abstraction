<?php
namespace Database\SQL\Drivers;

use Database\SQL\Pgsql_Definitions;
use Database\SQL\PDO_Trait;
use Database\SQL\ReconnectOnSelectDatabase_Trait;
use PDO;
use PDOException;

/**
 * The conventions of the PDO MySQL driver.
 *
 * @package Database\SQL
 */
class PdoPgsql extends Pgsql_Definitions {
    use PDO_Trait, ReconnectOnSelectDatabase_Trait;

    public $stringQuoteStart = '';
    public $stringQuoteEnd = '';
    public $binaryQuoteStart = '';
    public $binaryQuoteEnd = '';

    /**
     * @var string This is handled by escape() instead.
     */
    public $stringFuzzy = '';

    public function connect($host, $port, $username, $password, $database = false) {
        // keep the user and password in memory to allow for reconnects with selectdb
        $this->connectionUser = $username;
        $this->connectionPassword = $password;

        try {
            $this->connection = new PDO("pgsql:dbname=$database;host=$host;port=$port;user=$username;password=$password");
            $this->connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->registerConnection($host, $port, $username, $password);

            if (!$this->connection) {
                return false;
            }
            else {
                $this->versionCheck();

                return $this->connection;
            }
        } catch (PDOException $e) {
            $this->connectionError = $e->getMessage();

            return false;
        }
    }

    public function getLastInsertId()
    {
        return $this->connection->lastInsertId();
    }
}