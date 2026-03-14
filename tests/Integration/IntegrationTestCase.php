<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests\Integration;

use PHPUnit\Framework\TestCase;
use Pushax\Db\ClickHouse\Connection;
use Pushax\Db\ClickHouse\Driver;
use Pushax\Db\ClickHouse\Dsn;
use Yiisoft\Db\Cache\SchemaCache;
use Yiisoft\Test\Support\SimpleCache\MemorySimpleCache;

abstract class IntegrationTestCase extends TestCase
{
    protected Connection $db;

    /** Tables created during the test to be dropped in tearDown */
    private array $tablesToDrop = [];

    protected function setUp(): void
    {
        $host     = $_ENV['CH_HOST']     ?? '127.0.0.1';
        $port     = $_ENV['CH_PORT']     ?? '8123';
        $database = $_ENV['CH_DATABASE'] ?? 'default';
        $username = $_ENV['CH_USERNAME'] ?? 'default';
        $password = $_ENV['CH_PASSWORD'] ?? '';

        $dsn    = new Dsn(host: $host, databaseName: $database, port: $port);
        $driver = new Driver($dsn, $username, $password);

        $schemaCache = new SchemaCache(new MemorySimpleCache());
        $this->db = new Connection($driver, $schemaCache);

        try {
            $this->db->createCommand('SELECT 1')->queryScalar();
        } catch (\Throwable $e) {
            $this->markTestSkipped('ClickHouse is not available: ' . $e->getMessage());
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->tablesToDrop as $table) {
            try {
                $this->db->createCommand("DROP TABLE IF EXISTS `$table`")->execute();
            } catch (\Throwable) {
                // ignore cleanup errors
            }
        }

        $this->db->close();
    }

    /** Register a table to be automatically dropped after the test. */
    protected function dropAfterTest(string $table): void
    {
        $this->tablesToDrop[] = $table;
    }

    /** Unique table name to avoid collisions between parallel test runs. */
    protected function tmpTable(string $suffix = ''): string
    {
        return 'test_' . str_replace('.', '_', uniqid('', true)) . ($suffix ? "_$suffix" : '');
    }
}
