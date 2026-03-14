<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests\Integration;

use Pushax\Db\ClickHouse\Driver;

final class ConnectionTest extends IntegrationTestCase
{
    public function testSelectOne(): void
    {
        $result = $this->db->createCommand('SELECT 1 AS n')->queryScalar();

        $this->assertSame('1', $result);
    }

    public function testSelectCurrentDatabase(): void
    {
        $result = $this->db->createCommand('SELECT currentDatabase()')->queryScalar();

        $this->assertIsString($result);
        $this->assertNotEmpty($result);
    }

    public function testDriverName(): void
    {
        $driver = $this->db->getDriver();

        $this->assertInstanceOf(Driver::class, $driver);
        $this->assertSame('clickhouse', $driver->getDriverName());
    }

    public function testServerVersion(): void
    {
        $version = $this->db->getServerInfo()->getVersion();

        $this->assertNotEmpty($version);
        $this->assertMatchesRegularExpression('/^\d+\.\d+/', $version);
    }

    public function testCloseAndReopen(): void
    {
        $this->db->close();
        $result = $this->db->createCommand('SELECT 42 AS n')->queryScalar();

        $this->assertSame('42', $result);
    }

    public function testQueryAllReturnsRows(): void
    {
        $rows = $this->db->createCommand(
            'SELECT number FROM system.numbers LIMIT 3'
        )->queryAll();

        $this->assertCount(3, $rows);
        $this->assertArrayHasKey('number', $rows[0]);
    }

    public function testQueryColumnReturnsValues(): void
    {
        $values = $this->db->createCommand(
            'SELECT number FROM system.numbers LIMIT 5'
        )->queryColumn();

        $this->assertCount(5, $values);
        $this->assertSame(['0', '1', '2', '3', '4'], $values);
    }

    public function testQueryOneReturnsFirstRow(): void
    {
        $row = $this->db->createCommand(
            'SELECT number, toString(number) AS str FROM system.numbers LIMIT 1'
        )->queryOne();

        $this->assertIsArray($row);
        $this->assertArrayHasKey('number', $row);
        $this->assertArrayHasKey('str', $row);
    }

    public function testBindParams(): void
    {
        $result = $this->db->createCommand(
            'SELECT :value AS val',
            [':value' => 'hello']
        )->queryScalar();

        $this->assertSame('hello', $result);
    }

    public function testShowDatabases(): void
    {
        /** @var \Pushax\Db\ClickHouse\Command $command */
        $command = $this->db->createCommand();
        $databases = $command->showDatabases();

        $this->assertIsArray($databases);
        $this->assertNotEmpty($databases);
    }
}
