<?php

namespace Database\Index;

class Storage {
    const __default = self::btree;

    const btree = 'btree';
    const hash = 'hash';
}