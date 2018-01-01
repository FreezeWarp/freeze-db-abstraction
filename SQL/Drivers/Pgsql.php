<?php
namespace Database\SQL\Drivers;

use Database\DatabaseResultInterface;
use Database\DatabaseEngine;
use Database\SQL\DatabaseSQL;
use Database\SQL\ReconnectOnSelectDatabase_Trait;
use Database\SQL\SQL_Definitions;
use Database\Type;
use Database\Index;

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
class Pgsql extends SQL_Definitions {
    use ReconnectOnSelectDatabase_Trait;

    /**
     * @var resource
     */
    public $connection = null;

    public $storeTypes = array(DatabaseEngine::general);

    public $dataTypes = array(
        'columnIntLimits' => array(
            4 => 'SMALLINT',
            9 => 'INTEGER',
            'default' => 'BIGINT',
        ),
        'columnSerialLimits' => array(
            9 => 'SERIAL',
            'default' => 'BIGSERIAL',
        ),
        'columnStringTempLimits' => array(
            'default' => 'VARCHAR',
        ),
        'columnStringPermLimits' => array(
            'default' => 'VARCHAR',
        ),
        'columnNoLength' => array(
            'TEXT', 'BYTEA'
        ),
        'columnBlobTempLimits' => array(
            'default' => 'BYTEA',
        ),
        'columnBlobPermLimits' => array(
            'default' => 'BYTEA',
        ),

        'columnBitLimits' => array(
            15 => 'SMALLINT',
            31 => 'INTEGER',
            63 => 'BIGINT',
            'default' => 'INTEGER',
        ),
        Type\Type::float       => 'REAL',
        Type\Type::bool        => 'SMALLINT', // TODO: ENUM(1,2) AS BOOLENUM better.
        Type\Type::timestamp   => 'INTEGER',
        Type\Type::blob        => 'BYTEA',
        Type\Type::json        => 'JSONB',
    );

    public $concatTypes = array(
        'both' => ' AND ', 'either' => ' OR ', 'not' => ' NOT '
    );

    public $keyTypeConstants = array(
        Index\Type::fulltext => '',
        Index\Type::primary  => 'PRIMARY',
        Index\Type::unique   => 'UNIQUE',
        Index\Type::index    => '',
    );

    public $enumMode = 'useCreateType';
    public $commentMode = 'useCommentOn';
    public $indexMode = 'useCreateIndex';
    public $foreignKeyMode = 'useAlterTableConstraint';
    public $tableRenameMode = 'alterTable';
    public $perTableIndexes = false;

    /**
     * @var bool While Postgres supports a native bitfield type, it has very strange cast rules for it. Thus, it does not exhibit the expected behaviour, and we disable native bitfields.
     */
    public $nativeBitfield = false;

    /**
     * @var bool Enable PgSQL's CREATE TABLE IF NOT EXISTS support.
     */
    public $useCreateIfNotExist = true;

    /**
     * @var bool Enable DROP INDEX IF NOT EXISTS support on PgSQL.
     */
    public $useDropIndexIfExists = true;

    /**
     * @var bool Enable PgSQL's ON CONFLICT DO UPDATE upsert syntax. It will switch to 'selectThenInsertOrUpdate' once connected to the PgSQL server if the detected version is <9.5
     */
    public $upsertMode = 'onConflictDoUpdate';

    /**
     * @var bool Enable btree and hash index selection on PgSQL. This will be disabled if detected version is <10.0, since hash indexes don't work well in old versions.
     */
    public $indexStorages = array(
        Index\Storage::btree => 'btree',
        Index\Storage::hash => 'hash',
    );


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
        if ($context === Type::blob)
            return pg_escape_bytea($this->connection, $text);
        else
            return pg_escape_string($this->connection, $text);
    }

    public function query($rawQuery) {
        return pg_query($this->connection, $rawQuery);
    }

    public function queryReturningResult($rawQuery) : DatabaseResultInterface {
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
        return new class($source) implements DatabaseResultInterface {
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

    public function getTablesAsArray(DatabaseSQL $database) {
        return $database->rawQueryReturningResult('SELECT * FROM information_schema.tables WHERE table_catalog = '
            . $database->formatValue(Type::string, $database->activeDatabase)
            . ' AND table_type = \'BASE TABLE\' AND table_schema NOT IN (\'pg_catalog\', \'information_schema\')'
        )->getColumnValues('table_name');
    }

    public function getTableColumnsAsArray(DatabaseSQL $database) {
        return $database->rawQueryReturningResult('SELECT * FROM information_schema.columns WHERE table_catalog = '
            . $database->formatValue(Type::string, $database->activeDatabase)
            . ' AND table_schema NOT IN (\'pg_catalog\', \'information_schema\')'
        )->getColumnValues(['table_name', 'column_name']);
    }

    public function getTableConstraintsAsArray(DatabaseSQL $database) {
        return $database->rawQueryReturningResult('SELECT * FROM information_schema.table_constraints WHERE table_catalog = '
            . $database->formatValue(Type::string, $database->activeDatabase)
            . ' AND table_schema NOT IN (\'pg_catalog\', \'information_schema\')'
            . ' AND (constraint_type = \'FOREIGN KEY\' OR constraint_type = \'PRIMARY KEY\')'
        )->getColumnValues(['table_name', 'constraint_name']);
    }

    /**
     * It is not possible to get the indexes of tables in PgSQL.
     *
     * @return array an empty array
     */
    public function getTableIndexesAsArray(DatabaseSQL $database) {
        return [];
    }

    public function getLanguage() {
        return 'pgsql';
    }
}