<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests;

use PHPUnit\Framework\TestCase;
use Pushax\Db\ClickHouse\Dsn;

final class DsnTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $dsn = new Dsn();

        $this->assertSame('127.0.0.1', $dsn->host);
        $this->assertSame('default', $dsn->databaseName);
        $this->assertSame('8123', $dsn->port);
        $this->assertSame([], $dsn->options);
    }

    public function testToStringWithDefaults(): void
    {
        $dsn = new Dsn();

        $this->assertSame('clickhouse:host=127.0.0.1;port=8123;dbname=default', (string) $dsn);
    }

    public function testCustomHostAndDatabase(): void
    {
        $dsn = new Dsn(host: '10.0.0.1', databaseName: 'analytics');

        $this->assertStringContainsString('host=10.0.0.1', (string) $dsn);
        $this->assertStringContainsString('dbname=analytics', (string) $dsn);
    }

    public function testCustomPort(): void
    {
        $dsn = new Dsn(port: '9000');

        $this->assertStringContainsString('port=9000', (string) $dsn);
    }

    public function testEmptyPortOmitted(): void
    {
        $dsn = new Dsn(port: '');

        $this->assertStringNotContainsString('port=', (string) $dsn);
    }

    public function testEmptyDatabaseNameOmitted(): void
    {
        $dsn = new Dsn(databaseName: '');

        $this->assertStringNotContainsString('dbname=', (string) $dsn);
    }

    public function testExtraOptions(): void
    {
        $dsn = new Dsn(options: ['timeout' => '5', 'compress' => '1']);

        $this->assertStringContainsString('timeout=5', (string) $dsn);
        $this->assertStringContainsString('compress=1', (string) $dsn);
    }

    public function testFullDsn(): void
    {
        $dsn = new Dsn(
            host: 'ch.example.com',
            databaseName: 'logs',
            port: '8443',
            options: ['secure' => '1'],
        );

        $this->assertSame(
            'clickhouse:host=ch.example.com;port=8443;dbname=logs;secure=1',
            (string) $dsn,
        );
    }
}
