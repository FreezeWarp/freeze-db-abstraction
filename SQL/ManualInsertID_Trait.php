<?php
namespace Database\SQL;

/**
 * The conventions of any driver that doesn't have native functionality for obtaining the last insertId.
 *
 * @package Database\SQL
 */
trait ManualInsertID_Trait {
    /**
     * @var mixed A holder for the last insert ID, since it may be unset by subsequent queries.
     */
    public $lastInsertId;

    public function incrementLastInsertId($insertId) {
        $this->lastInsertId = $insertId ?: $this->lastInsertId;
    }

    public function getLastInsertId() {
        return $this->lastInsertId;
    }
}