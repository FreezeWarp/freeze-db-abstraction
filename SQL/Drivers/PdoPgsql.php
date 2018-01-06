<?php
namespace Database\SQL\Drivers;

use Database\SQL\Pgsql_Definitions;
use Database\SQL\PDO_Trait;

/**
 * The conventions of the PDO MySQL driver.
 *
 * @package Database\SQL
 */
class PdoPgsql extends Pgsql_Definitions {
    use PDO_Trait;

    public $stringQuoteStart = '';
    public $stringQuoteEnd = '';
    public $binaryQuoteStart = '';
    public $binaryQuoteEnd = '';

    /**
     * @var string This is handled by escape() instead.
     */
    public $stringFuzzy = '';

    public function connect($host, $port, $username, $password, $database = false) {
        return $this->pdoConnect("pgsql", $host, $port, $username, $password, $database);
    }
}