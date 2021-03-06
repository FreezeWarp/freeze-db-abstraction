<?php

namespace Database;

/**
 * A faux-enum containing the different value types supported by the Database layer.
 * Individual drivers may not support all of these, however.
 *
 * @package Database
 */
class Engine {
    /**
     * The default value, general.
     */
    const __default = self::general;

    /**
     * A general-purpose storage engine.
     */
    const general = 'general';

    /**
     * An in-memory storage engine.
     */
    const memory = 'memory';
}
