<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests\Integration;

use Pushax\Db\ClickHouse\PartitionManager;

final class PartitionManagerTest extends IntegrationTestCase
{
    private string $table;

    protected function setUp(): void
    {
        parent::setUp();

        $this->table = $this->tmpTable('partitions');
        $this->dropAfterTest($this->table);

        $this->db->createCommand("
            CREATE TABLE `{$this->table}`
            (
                `event_date` Date,
                `user_id`    UInt64,
                `value`      Float64
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (event_date, user_id)
        ")->execute();

        // Insert data across two partitions
        $this->db->createCommand("
            INSERT INTO `{$this->table}` (event_date, user_id, value) VALUES
            ('2024-01-15', 1, 10.0),
            ('2024-01-20', 2, 20.0),
            ('2024-02-10', 3, 30.0),
            ('2024-02-25', 4, 40.0)
        ")->execute();
    }

    public function testGetPartitions(): void
    {
        $pm = new PartitionManager($this->db);
        $partitions = $pm->getPartitions($this->table);

        $this->assertIsArray($partitions);
        $this->assertNotEmpty($partitions);

        $partitionIds = array_column($partitions, 'partition');
        $this->assertContains('202401', $partitionIds);
        $this->assertContains('202402', $partitionIds);
    }

    public function testGetPartitionsHasExpectedFields(): void
    {
        $pm = new PartitionManager($this->db);
        $partitions = $pm->getPartitions($this->table);

        $first = $partitions[0];
        $this->assertArrayHasKey('partition', $first);
        $this->assertArrayHasKey('total_rows', $first);
        $this->assertArrayHasKey('readable_size', $first);
    }

    public function testDropPartition(): void
    {
        $pm = new PartitionManager($this->db);
        $pm->dropPartition($this->table, '202401');

        $count = $this->db->createCommand(
            "SELECT count() FROM `{$this->table}` WHERE toYYYYMM(event_date) = 202401"
        )->queryScalar();

        $this->assertSame('0', $count);
    }

    public function testDetachAndAttachPartition(): void
    {
        $pm = new PartitionManager($this->db);

        $pm->detachPartition($this->table, '202401');

        // After detach, rows in that partition are gone
        $count = $this->db->createCommand(
            "SELECT count() FROM `{$this->table}` WHERE toYYYYMM(event_date) = 202401"
        )->queryScalar();
        $this->assertSame('0', $count);

        // Reattach
        $pm->attachPartition($this->table, '202401');

        $count = $this->db->createCommand(
            "SELECT count() FROM `{$this->table}` WHERE toYYYYMM(event_date) = 202401"
        )->queryScalar();
        $this->assertSame('2', $count);
    }

    public function testMovePartitionBetweenTables(): void
    {
        $target = $this->tmpTable('partitions_target');
        $this->dropAfterTest($target);

        // Target must have the same structure
        $this->db->createCommand("
            CREATE TABLE `$target`
            (
                `event_date` Date,
                `user_id`    UInt64,
                `value`      Float64
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (event_date, user_id)
        ")->execute();

        $pm = new PartitionManager($this->db);
        $pm->movePartition($this->table, $target, '202401');

        // Source no longer has January data
        $sourceCount = $this->db->createCommand(
            "SELECT count() FROM `{$this->table}` WHERE toYYYYMM(event_date) = 202401"
        )->queryScalar();
        $this->assertSame('0', $sourceCount);

        // Target has it
        $targetCount = $this->db->createCommand(
            "SELECT count() FROM `$target` WHERE toYYYYMM(event_date) = 202401"
        )->queryScalar();
        $this->assertSame('2', $targetCount);
    }

    public function testReplacePartition(): void
    {
        $source = $this->tmpTable('partitions_source');
        $this->dropAfterTest($source);

        $this->db->createCommand("
            CREATE TABLE `$source`
            (
                `event_date` Date,
                `user_id`    UInt64,
                `value`      Float64
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (event_date, user_id)
        ")->execute();

        // Insert one January row into source
        $this->db->createCommand("
            INSERT INTO `$source` (event_date, user_id, value) VALUES ('2024-01-01', 99, 999.0)
        ")->execute();

        $pm = new PartitionManager($this->db);
        $pm->replacePartition($source, $this->table, '202401');

        // The target's January partition is now from source (1 row with user_id=99)
        $count = $this->db->createCommand(
            "SELECT count() FROM `{$this->table}` WHERE toYYYYMM(event_date) = 202401 AND user_id = 99"
        )->queryScalar();
        $this->assertSame('1', $count);
    }
}
