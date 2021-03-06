<?php
namespace Database;

use Database\Type\Comparison;
use Exception;

class Type {
    public $type;
    public $value;
    public $comparison;

    public function __construct($type, $value, $comparison = Comparison::__default) {
        /* Validation Checks */
        if ($type === Type\Type::arraylist && !($comparison === Comparison::in || $comparison === Comparison::notin))
            throw new Exception('Arrays can only be compared with in and notin.');
        if ($type !== Type\Type::arraylist && ($comparison === Comparison::in || $comparison === Comparison::notin)) {
            throw new Exception('in and notin can only be used with arrays.');
        }


        $this->type = $type;
        $this->value = $value;
        $this->comparison = $comparison;
    }
}
