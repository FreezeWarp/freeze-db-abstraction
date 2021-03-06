<?php

namespace Database\Index;

/**
 * A faux-enum containing the different index types supported by the Database layer.
 *
 * @package Database
 */
class Type
{
    /**
     * The default value, index.
     */
    const __default = self::index;

    /**
     * A general-purpose index.
     */
    const index = 'index';

    /**
     * A primary-key index.
     */
    const primary = 'primary';

    /**
     * A unique constraint index.
     */
    const unique = 'unique';

    /**
     * A fulltext-search index.
     */
    const fulltext = 'fulltext';
}