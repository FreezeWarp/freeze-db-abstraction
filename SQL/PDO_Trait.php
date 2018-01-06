<?php

namespace Database\SQL;

use PDO;
use PDOException;
use PDOStatement;
use Exception;

use Database\ResultInterface;
use Database\Index;
use Database\Engine;
use Database\Type;
use Database\Type\Comparison;

/**
 * The conventions of all PDO drivers.
 *
 * @package Database\SQL
 */
trait PDO_Trait
{
    use ManualInsertID_Trait, ReconnectOnSelectDatabase_Trait;


    /**
     * @var PDO
     */
    private $connection = null;

    /**
     * @var array
     */
    private $preparedParams = [];

    /**
     * @var string An error string registered on connection failure.
     */
    private $connectionError;

    /**
     * @var array Additional information about the last query error, if any.
     */
    private $lastQueryError = [];


    public function pdoConnect($protocol, $host, $port, $username, $password, $database = false)
    {
        // keep the user and password in memory to allow for reconnects with selectdb
        $this->connectionUser = $username;
        $this->connectionPassword = $password;

        try {
            $this->connection = new PDO("$protocol:" . ($database ? "dbname=$database" : '') . ";host=$host:$port", $username, $password);
            $this->connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->registerConnection($host, $port, $username, $password);

            $this->versionCheck();
        } catch (PDOException $e) {
            $this->connectionError = $e->getMessage();

            return false;
        }

        return $this->connection;
    }

    public function getVersion()
    {
        return $this->connection->getAttribute(PDO::ATTR_SERVER_VERSION);
    }

    public function getLastError()
    {
        return $this->connectionError ?: ($this->lastQueryError ?: $this->connection->errorInfo()[2]);
    }

    public function close()
    {
        if (isset($this->connection)) {
            unset($this->connection);
        }

        return true;
    }

    public function escape($text, $context)
    {
        switch ($context) {
            case Type\Type::integer:
            case Type\Type::timestamp:
            case Type\Type::bitfield:
            case Type\Type::float:
            case Type\Type::blob:
            case Type\Type::string:
                $this->preparedParams[] = $text;

                return '?';
            break;

            case Type\Type::search:
                $this->preparedParams[] = '%' . $text . '%';

                return '?';
            break;
        }

        return $text;
    }

    public function query($rawQuery, $delayExecution = false)
    {
        // If the query is an instance of PDOStatement, we execute it directly. This most commonly happens when delayExecution was used in the past.
        if (!($rawQuery instanceof PDOStatement)) {
            $query = $this->connection->prepare($rawQuery);

            // Bind all available params as soon as possible, in case we need to return the PDOStatement object instead of executing it.
            $paramNum = 1;
            foreach ($this->preparedParams AS $preparedParam) {
                $query->bindValue($paramNum++, $preparedParam);
            }

            // Clear the list of prepared parameters.
            $this->preparedParams = [];
        }
        else {
            $query = $rawQuery;
        }

        // If delayExecution, return the query as a PDOStatement, without executing it.
        if ($delayExecution) {
            return $query;
        }

        // Otherwise, take a shot at executing it.
        else {
            try {
                if (!$query->execute()) {
                    return false;
                }

                $this->incrementLastInsertId($this->connection->lastInsertId());
                $this->lastQueryError = [];

                return $query;
            } catch (PDOException $ex) {
                $this->lastQueryError = $ex;
                //$query->debugDumpParams();

                return false;
            }
        }
    }

    public function queryReturningResult($rawQuery): ResultInterface
    {
        return $this->getResult($this->query($rawQuery));
    }

    public function startTransaction()
    {
        try {
            return $this->connection->beginTransaction();
        } catch (PDOException $ex) {
            return false;
        }
    }

    public function endTransaction()
    {
        try {
            return $this->connection->commit();
        } catch (PDOException $ex) {
            return false;
        }
    }

    public function rollbackTransaction()
    {
        try {
            return $this->connection->rollBack();
        } catch (PDOException $ex) {
            return false;
        }
    }

    protected function getResult($source): ResultInterface
    {
        return new class($source) implements ResultInterface
        {
            /**
             * @var PDOStatement The result of the query.
             */
            public $source;

            /**
             * @var int A pointer to the current result entry.
             */
            public $resultIndex = 0;

            /**
             * @var array All query information. (Required to support getCount on all PDO drivers; you may want to override this on compatible language implementors.)
             */
            public $data = [];

            public function __construct($source)
            {
                $this->source = $source;

                if ($this->source)
                    $this->data = $this->source->fetchAll(PDO::FETCH_ASSOC);
            }

            public function fetchAsArray()
            {
                if ($this->resultIndex >= count($this->data))
                    return false;
                else
                    return $this->data[$this->resultIndex++];
            }

            public function getCount()
            {
                return count($this->data);
            }
        };
    }
}

?>