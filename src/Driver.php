<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Driver\DriverInterface;

/**
 * Implements the ClickHouse driver using the HTTP interface.
 *
 * @link https://clickhouse.com/docs/en/interfaces/http
 */
final class Driver implements DriverInterface
{
    public function __construct(
        private readonly Dsn $dsn,
        private readonly string $username = 'default',
        private readonly string $password = '',
    ) {}

    public function getUrl(): string
    {
        return "http://{$this->dsn->host}:{$this->dsn->port}/";
    }

    public function getDsn(): string
    {
        return (string) $this->dsn;
    }

    public function getDatabase(): string
    {
        return $this->dsn->databaseName;
    }

    public function getUsername(): string
    {
        return $this->username;
    }

    public function getPassword(): string
    {
        return $this->password;
    }

    public function getDriverName(): string
    {
        return 'clickhouse';
    }
}
