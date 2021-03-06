<?php

namespace Database\Type;

/**
 * A faux-enum containing the different value types supported by the Database layer.
 *
 * @package Database
 */
class Type {
    /**
     * The default value, string.
     */
    const __default = self::string;

    /**
     * A null/empty.
     */
    const null = 'null';

    /**
     * A textual string.
     */
    const string = 'string';

    /**
     * A binary string.
     */
    const blob = 'blob';

    /**
     * A string using search globs.
     */
    const search = 'search';

    /**
     * An integral.
     */
    const integer = 'int';

    /**
     * A floating point.
     */
    const float = 'float';

    /**
     * A type that supports bitwise operations on individual bits.
     */
    const bitfield = 'bitfield';

    /**
     * A boolean.
     */
    const bool = 'bool';

    /**
     * A timestamp, typically as an integer using UNIX timestamp encoding.
     */
    const timestamp = 'time';

    /**
     * A one-dimensional list of values.
     */
    const arraylist = 'list';

    /**
     * Another table column.
     */
    const column = 'column';

    /**
     * A table-column pair.
     */
    const tableColumn = 'tableColumn';

    /**
     * An enumeration of multiple subvalues. Typically only used for defining table schema, not for inserting into it.
     */
    const enum = 'enum';

    /**
     * An equation, e.g. "$tableColumn + 2". Typically only used for inserting/updating into tables.
     */
    const equation = 'equation';

    /**
     * A JSON-formatted string. The contents of this will not generally be checked, except by certain database drivers.
     */
    const json = 'json';
}
