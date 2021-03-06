<?php

namespace Database\Type;

/**
 * A faux-enum containing the different comparison types supported by the Database layer.
 *
 * @package Database
 */
class Comparison {
    /**
     * The default value, equals.
     */
    const __default = self::equals;

    const notin = 'notin';

    /**
     * Less than.
     */
    const lessThan = 'lt';

    /**
     * Less than or equal to.
     */
    const lessThanEquals = 'lte';

    /**
     * Equal to. In most cases, this is expected to be a weak comparison.
     */
    const equals = 'e';

    /**
     * Greater than.
     */
    const greaterThan = 'gt';

    /**
     * Greater than or equal to.
     */
    const greaterThanEquals = 'gte';

    /**
     * Perform an in-text search for a string, e.g. "a" matches "ab", "ba", and "bab".
     */
    const search = 'search';

    /**
     * Perform an optimised full-text search full-text search for a string. The conventions of this are left up to implementing drivers.
     * If a driver does not have native capability for this, it should be treated identically to {@see DatabaseTypeComparison::search}
     */
    const fulltextSearch = 'fulltextSearch';

    /**
     * An item is in a collection, e.g. "a" matches ["a"], ["a", "b"], and ["b", "a", "c"].
     */
    const in = 'in';

    /**
     * A value contains all bits of another value.
     */
    const binaryAnd = 'bAnd';

    /**
     * A special case where a value is being assigned. TODO: refactor.
     */
    const assignment = 1000;
}
