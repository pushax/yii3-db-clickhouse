<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests;

use PHPUnit\Framework\TestCase;
use Pushax\Db\ClickHouse\Quoter;
use Pushax\Db\ClickHouse\TableBuilder;
use Pushax\Db\ClickHouse\TableEngine;
use Yiisoft\Db\Connection\ConnectionInterface;

final class TableBuilderTest extends TestCase
{
    private ConnectionInterface $db;

    protected function setUp(): void
    {
        $quoter = new Quoter('`', '`');

        $this->db = $this->createMock(ConnectionInterface::class);
        $this->db->method('getQuoter')->willReturn($quoter);
    }

    public function testBasicMergeTreeTable(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->column('event_date', 'Date')
            ->column('user_id', 'UInt64')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy(['event_date', 'user_id'])
            ->build();

        $this->assertStringContainsString('CREATE TABLE `events`', $sql);
        $this->assertStringContainsString('`event_date` Date', $sql);
        $this->assertStringContainsString('`user_id` UInt64', $sql);
        $this->assertStringContainsString('ENGINE = MergeTree()', $sql);
        $this->assertStringContainsString('ORDER BY (`event_date`, `user_id`)', $sql);
    }

    public function testIfNotExists(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->ifNotExists()
            ->engine(TableEngine::MEMORY)
            ->build();

        $this->assertStringContainsString('CREATE TABLE IF NOT EXISTS `events`', $sql);
    }

    public function testPartitionBy(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MERGE_TREE)
            ->partitionBy('toYYYYMM(event_date)')
            ->orderBy('event_date')
            ->build();

        $this->assertStringContainsString('PARTITION BY toYYYYMM(event_date)', $sql);
    }

    public function testPrimaryKey(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MERGE_TREE)
            ->primaryKey(['event_date', 'user_id'])
            ->build();

        $this->assertStringContainsString('PRIMARY KEY (`event_date`, `user_id`)', $sql);
    }

    public function testOrderByFallsBackToPrimaryKey(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MERGE_TREE)
            ->primaryKey(['event_date'])
            ->build();

        // When no orderBy is set, it should fall back to primaryKey
        $this->assertStringContainsString('ORDER BY (`event_date`)', $sql);
    }

    public function testSampleBy(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy('event_date')
            ->sampleBy('user_id')
            ->build();

        $this->assertStringContainsString('SAMPLE BY user_id', $sql);
    }

    public function testTtl(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy('event_date')
            ->ttl('event_date + INTERVAL 1 MONTH')
            ->build();

        $this->assertStringContainsString('TTL event_date + INTERVAL 1 MONTH', $sql);
    }

    public function testSettings(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy('event_date')
            ->settings(['index_granularity' => 8192])
            ->build();

        $this->assertStringContainsString('SETTINGS index_granularity = 8192', $sql);
    }

    public function testSettingsBooleanValues(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy('event_date')
            ->settings(['allow_nullable_key' => true, 'use_minimalistic_part_header' => false])
            ->build();

        $this->assertStringContainsString('allow_nullable_key = 1', $sql);
        $this->assertStringContainsString('use_minimalistic_part_header = 0', $sql);
    }

    public function testComment(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MEMORY)
            ->comment('My events table')
            ->build();

        $this->assertStringContainsString("COMMENT 'My events table'", $sql);
    }

    public function testOnCluster(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->onCluster('my_cluster')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy('event_date')
            ->build();

        $this->assertStringContainsString("ON CLUSTER 'my_cluster'", $sql);
    }

    public function testDatabase(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->database('analytics')
            ->engine(TableEngine::MEMORY)
            ->build();

        $this->assertStringContainsString('`analytics`.`events`', $sql);
    }

    public function testIndex(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy('event_date')
            ->index('idx_user', 'user_id', 'minmax', 3)
            ->build();

        $this->assertStringContainsString('INDEX `idx_user` user_id TYPE minmax GRANULARITY 3', $sql);
    }

    public function testMemoryEngineHasNoParentheses(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('cache')
            ->engine(TableEngine::MEMORY)
            ->build();

        $this->assertStringContainsString('ENGINE = Memory', $sql);
        $this->assertStringNotContainsString('ENGINE = Memory()', $sql);
    }

    public function testColumnsMethod(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->columns([
                'id'   => 'UInt64',
                'name' => 'String',
            ])
            ->engine(TableEngine::MEMORY)
            ->build();

        $this->assertStringContainsString('`id` UInt64', $sql);
        $this->assertStringContainsString('`name` String', $sql);
    }

    public function testFullTable(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('events')
            ->ifNotExists()
            ->column('event_date', 'Date')
            ->column('event_type', 'LowCardinality(String)')
            ->column('user_id', 'UInt64')
            ->engine(TableEngine::MERGE_TREE)
            ->partitionBy('toYYYYMM(event_date)')
            ->orderBy(['event_date', 'event_type', 'user_id'])
            ->primaryKey(['event_date', 'event_type'])
            ->ttl('event_date + INTERVAL 6 MONTH')
            ->settings(['index_granularity' => 8192])
            ->build();

        $this->assertStringContainsString('CREATE TABLE IF NOT EXISTS `events`', $sql);
        $this->assertStringContainsString('ENGINE = MergeTree()', $sql);
        $this->assertStringContainsString('PARTITION BY toYYYYMM(event_date)', $sql);
        $this->assertStringContainsString('ORDER BY (`event_date`, `event_type`, `user_id`)', $sql);
        $this->assertStringContainsString('PRIMARY KEY (`event_date`, `event_type`)', $sql);
        $this->assertStringContainsString('TTL event_date + INTERVAL 6 MONTH', $sql);
        $this->assertStringContainsString('SETTINGS index_granularity = 8192', $sql);
    }
}
