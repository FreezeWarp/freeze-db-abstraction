<?php

namespace Database\SQL\Drivers;

use Database\SQL\SQLSrv_Definitions;
use Database\SQL\PDO_Trait;

/**
 * The conventions of the PDO MySQL driver.
 *
 * @package Database\SQL
 */
class PdoSqlsrv extends SQLSrv_Definitions
{
    use PDO_Trait;

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
        return $this->pdoConnect("sqlsrv", $host, $port, $username, $password, $database);
    }
}