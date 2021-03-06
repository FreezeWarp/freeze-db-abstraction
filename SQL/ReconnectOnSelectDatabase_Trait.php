<?php
namespace Database\SQL;

/**
 * The conventions of any driver that doesn't have the ability to set the active database after especially a connection.
 */
trait ReconnectOnSelectDatabase_Trait {
    protected $connectionUser;
    protected $connectionPassword;
    protected $connectionHost;
    protected $connectionPort;

    public function registerConnection($host, $port, $user, $password) {
        $this->connectionHost = $host;
        $this->connectionPort = $port;
        $this->connectionUser = $user;
        $this->connectionPassword = $password;
    }

    public function selectDatabase($database) {
        return $this->connect($this->connectionHost, $this->connectionPort, $this->connectionUser, $this->connectionPassword, $database);
    }
}