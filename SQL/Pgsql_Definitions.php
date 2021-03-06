<?php

namespace Database\SQL;

use Database\Index;
use Database\Type;
use Database\Engine;

abstract class Pgsql_Definitions extends SQL_Definitions
{
    public $storeTypes = [Engine::general];

    public $dataTypes = [
        'columnIntLimits'        => [
            4         => 'SMALLINT',
            9         => 'INTEGER',
            'default' => 'BIGINT',
        ],
        'columnSerialLimits'     => [
            9         => 'SERIAL',
            'default' => 'BIGSERIAL',
        ],
        'columnStringTempLimits' => [
            'default' => 'VARCHAR',
        ],
        'columnStringPermLimits' => [
            'default' => 'VARCHAR',
        ],
        'columnNoLength'         => [
            'TEXT', 'BYTEA'
        ],
        'columnBlobTempLimits'   => [
            'default' => 'BYTEA',
        ],
        'columnBlobPermLimits'   => [
            'default' => 'BYTEA',
        ],

        'columnBitLimits'    => [
            15        => 'SMALLINT',
            31        => 'INTEGER',
            63        => 'BIGINT',
            'default' => 'INTEGER',
        ],
        Type\Type::float     => 'REAL',
        Type\Type::bool      => 'SMALLINT', // TODO: ENUM(1,2) AS BOOLENUM better.
        Type\Type::timestamp => 'INTEGER',
        Type\Type::blob      => 'BYTEA',
        Type\Type::json      => 'JSONB',
    ];

    public $concatTypes = [
        'both' => ' AND ', 'either' => ' OR ', 'not' => ' NOT '
    ];

    public $keyTypeConstants = [
        Index\Type::fulltext => '',
        Index\Type::primary  => 'PRIMARY',
        Index\Type::unique   => 'UNIQUE',
        Index\Type::index    => '',
    ];

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
    public $indexStorages = [
        Index\Storage::btree => 'btree',
        Index\Storage::hash  => 'hash',
    ];


    public function getTablesAsArray(DatabaseSQL $database)
    {
        return $database->rawQueryReturningResult('SELECT * FROM information_schema.tables WHERE table_catalog = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
            . ' AND table_type = \'BASE TABLE\' AND table_schema NOT IN (\'pg_catalog\', \'information_schema\')'
        )->getColumnValues('table_name');
    }

    public function getTableColumnsAsArray(DatabaseSQL $database)
    {
        return $database->rawQueryReturningResult('SELECT * FROM information_schema.columns WHERE table_catalog = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
            . ' AND table_schema NOT IN (\'pg_catalog\', \'information_schema\')'
        )->getColumnValues(['table_name', 'column_name']);
    }

    public function getTableConstraintsAsArray(DatabaseSQL $database)
    {
        return $database->rawQueryReturningResult('SELECT * FROM information_schema.table_constraints WHERE table_catalog = '
            . $database->formatValue(Type\Type::string, $database->activeDatabase)
            . ' AND table_schema NOT IN (\'pg_catalog\', \'information_schema\')'
            . ' AND (constraint_type = \'FOREIGN KEY\' OR constraint_type = \'PRIMARY KEY\')'
        )->getColumnValues(['table_name', 'constraint_name']);
    }

    /**
     * It is not possible to get the indexes of tables in PgSQL.
     *
     * @return array an empty array
     */
    public function getTableIndexesAsArray(DatabaseSQL $database)
    {
        return [];
    }

    public function getLanguage()
    {
        return 'pgsql';
    }

    public function versionCheck()
    {
        if (floatval($this->getVersion()) < 9.5)
            $this->upsertMode = 'selectThenInsertOrUpdate';

        if (floatval($this->getVersion()) < 10)
            $this->indexStorages = [];
    }
}