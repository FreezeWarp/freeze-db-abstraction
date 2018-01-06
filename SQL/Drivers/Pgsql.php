<?php
namespace Database\SQL\Drivers;

use Database\ResultInterface;
use Database\SQL\Pgsql_Definitions;
use Database\SQL\ReconnectOnSelectDatabase_Trait;
use Database\Type;

/**
 * The conventions of the PostgreSQL DBMS.
 *
 * The following is a PostgreSQL version support list (better list @ http://www.postgresql.org/about/featurematrix/):
 * * 8.0: savepoints, ability to alter column type, table spaces
 * * 8.1: Two-phase commit, new permissions system,
 * * 8.2: RETURNING, nulls in arrays,
 * * 8.3: Full text search, XML, ENUM data types, UUID type,
 * * 8.4: Column permissions, per-database locale,
 * * 9.0: 64-bit WIN support, better LISTEN/NOTIFY perfrmance, per-column triggers
 * * 9.1: Sync. replication, foreign tables,
 * * 9.2: Index-only scans, cascading replication, range data types, JSON data type,
 * * 9.3: Large objects can be up to 4TB (instead of 2GB), better JSON functions, event triggers
 * * 9.4: JSONB
 * * 9.5: Unlogged tables, JSONB functions, ON CONFLICT (i.e. upsert)
 * * 10.0: Hash indexes actually kinda work.
 *
 * @package Database\SQL
 */
class Pgsql extends Pgsql_Definitions {
    use ReconnectOnSelectDatabase_Trait;

    /**
     * @var resource
     */
    public $connection = null;


    public function connect($host, $port, $username, $password, $database = false) {
        // keep the user and password in memory to allow for reconnects with selectdb
        $this->connectionUser = $username;
        $this->connectionPassword = $password;

        $this->connection = pg_connect("host=$host port=$port user=$username password=$password" . ($database ? " dbname=$database" : ''));
        $this->registerConnection($host, $port, $username, $password);

        if (!$this->connection) {
            return false;
        }
        else {
            $this->query('SET bytea_output = "escape"'); // PHP-supported binary escape format.

            if (floatval($this->getVersion()) < 9.5)
                $this->upsertMode = 'selectThenInsertOrUpdate';

            if (floatval($this->getVersion()) < 10)
                $this->indexStorages = [];

            return $this->connection;
        }
    }

    public function getVersion() {
        return pg_version($this->connection)['server'];
    }

    public function getLastError() {
        return pg_last_error($this->connection);
    }

    public function close() {
        if (isset($this->connection)) {
            $function = @pg_close($this->connection);
            unset($this->connection);

            return $function;
        }
        else {
            return true;
        }
    }

    public function escape($text, $context) {
        if ($context === Type\Type::blob)
            return pg_escape_bytea($this->connection, $text);
        else
            return pg_escape_string($this->connection, $text);
    }

    public function query($rawQuery, $delayExecution = false) {
        return $delayExecution
            ? $rawQuery
            : pg_query($this->connection, $rawQuery);
    }

    public function queryReturningResult($rawQuery) : ResultInterface {
        return $this->getResult($this->query($rawQuery));
    }

    public function getLastInsertId() {
        return pg_fetch_array($this->query('SELECT LASTVAL() AS lastval'))['lastval'];
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

    protected function getResult($source) {
        return new class($source) implements ResultInterface {
            /**
             * @var resource The postgres resource returned by query.
             */
            public $source;

            /**
             * @var array An array containing the field numbers corresponding to all binary columns in the current resultset.
             */
            public $binaryFields = [];

            /**
             * @var array An array containing the field numbers corresponding to all integer columns in the current resultset, used to convert NULL to 0 (instead of ""). This may not be needed on new versions of Postgres.
             */
            public $integerFields = [];

            public function __construct($source) {
                $this->source = $source;

                $num = pg_num_fields($this->source);
                for ($i = 0; $i < $num; $i++) {
                    if (pg_field_type($this->source, $i) === 'bytea') {
                        $this->binaryFields[] = pg_field_name($this->source, $i);
                    }

                    // ...not actually positive this is needed. Need more testing.
                    if (pg_field_type($this->source, $i) === 'int2' || pg_field_type($this->source, $i) === 'int4') {
                        $this->integerFields[] = pg_field_name($this->source, $i);
                    }
                }
            }

            public function fetchAsArray() {
                $data = pg_fetch_assoc($this->source);

                // Decode bytea values
                if ($data) {
                    foreach ($this->binaryFields AS $field) {
                        $data[$field] = pg_unescape_bytea($data[$field]);
                    }

                    foreach ($this->integerFields AS $field) {
                        $data[$field] = (int) $data[$field];
                    }
                }

                return $data;
            }

            public function getCount() {
                return pg_num_rows($this->source);
            }
        };
    }


    public function notify() {
        return pg_get_notify($this->connection);
    }
}