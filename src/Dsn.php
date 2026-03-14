<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Stringable;

/**
 * Represents a Data Source Name (DSN) for ClickHouse servers that's used to configure a {@see Driver} instance.
 *
 * To get DSN in string format, use the `(string)` type casting operator.
 *
 * @link https://clickhouse.com/docs/en/interfaces/tcp
 */
final class Dsn implements Stringable
{
    /**
     * @param string $host The database host name or IP address.
     * @param string $databaseName The database name to connect to.
     * @param string $port The database port. Default is 8123 (HTTP interface).
     * @param string[] $options The database connection options.
     *
     * @psalm-param array<string,string> $options
     */
    public function __construct(
        public readonly string $host = '127.0.0.1',
        public readonly string $databaseName = 'default',
        public readonly string $port = '8123',
        public readonly array $options = [],
    ) {}

    /**
     * @return string The Data Source Name for ClickHouse HTTP interface.
     *
     * ```php
     * $dsn = new Dsn('127.0.0.1', 'default', '8123');
     * $driver = new Driver($dsn, 'default', '');
     * $connection = new Connection($driver, $schemaCache);
     * ```
     *
     * Will result in the DSN string `clickhouse:host=127.0.0.1;port=8123;dbname=default`.
     */
    public function __toString(): string
    {
        $dsn = "clickhouse:host=$this->host";

        if ($this->port !== '') {
            $dsn .= ";port=$this->port";
        }

        if ($this->databaseName !== '') {
            $dsn .= ";dbname=$this->databaseName";
        }

        foreach ($this->options as $key => $value) {
            $dsn .= ";$key=$value";
        }

        return $dsn;
    }
}
