<?php

namespace Database\SQL\Drivers;

use Database\SQL\MySQL_Definitions;
use Database\SQL\PDO_Trait;

/**
 * The conventions of the PDO MySQL driver.
 *
 * @package Database\SQL
 */
class PdoMysql extends MySQL_Definitions
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
        return $this->pdoConnect("mysql", $host, $port, $username, $password, $database);
    }
}