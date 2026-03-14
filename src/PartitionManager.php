<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Connection\ConnectionInterface;

/**
 * Provides functionality for managing ClickHouse table partitions.
 *
 * ClickHouse tables using MergeTree family engines support partitioning,
 * which allows efficient data management and query optimization.
 *
 * Usage example:
 * ```php
 * $pm = new PartitionManager($connection);
 *
 * // List partitions
 * $partitions = $pm->getPartitions('events');
 *
 * // Drop old partition
 * $pm->dropPartition('events', '202301');
 *
 * // Detach and attach partitions
 * $pm->detachPartition('events', '202301');
 * $pm->attachPartition('events', '202301');
 *
 * // Move partition between tables
 * $pm->movePartition('events', 'events_archive', '202301');
 * ```
 *
 * @link https://clickhouse.com/docs/en/sql-reference/statements/alter/partition
 */
final class PartitionManager
{
    public function __construct(
        private readonly ConnectionInterface $db,
    ) {}

    /**
     * Returns partition information for a table.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getPartitions(string $table, string $database = ''): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                partition,
                partition_id,
                name,
                sum(rows) as total_rows,
                sum(bytes_on_disk) as total_bytes,
                formatReadableSize(sum(bytes_on_disk)) as readable_size,
                min(min_date) as min_date,
                max(max_date) as max_date,
                count() as parts_count
            FROM system.parts
            WHERE database = COALESCE(:database, currentDatabase())
                AND table = :table
                AND active = 1
            GROUP BY partition, partition_id, name
            ORDER BY partition
            SQL,
            [
                ':database' => $database ?: null,
                ':table' => $table,
            ],
        )->queryAll();
    }

    /**
     * Drops a partition from a table.
     *
     * @param string $table The table name.
     * @param string $partition The partition expression or ID.
     */
    public function dropPartition(string $table, string $partition): void
    {
        $sql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . " DROP PARTITION " . $this->db->getQuoter()->quoteValue($partition);

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Detaches a partition from a table (moves it to the `detached` directory).
     */
    public function detachPartition(string $table, string $partition): void
    {
        $sql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . " DETACH PARTITION " . $this->db->getQuoter()->quoteValue($partition);

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Attaches a previously detached partition.
     */
    public function attachPartition(string $table, string $partition): void
    {
        $sql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . " ATTACH PARTITION " . $this->db->getQuoter()->quoteValue($partition);

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Moves a partition from one table to another.
     * Both tables must have the same structure.
     */
    public function movePartition(string $sourceTable, string $targetTable, string $partition): void
    {
        $quoter = $this->db->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($sourceTable)
            . " MOVE PARTITION " . $quoter->quoteValue($partition)
            . ' TO TABLE ' . $quoter->quoteTableName($targetTable);

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Replaces a partition in the target table with data from the source table.
     */
    public function replacePartition(string $sourceTable, string $targetTable, string $partition): void
    {
        $quoter = $this->db->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($targetTable)
            . " REPLACE PARTITION " . $quoter->quoteValue($partition)
            . ' FROM ' . $quoter->quoteTableName($sourceTable);

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Freezes a partition (creates a backup in the `shadow` directory).
     */
    public function freezePartition(string $table, string $partition): void
    {
        $sql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . " FREEZE PARTITION " . $this->db->getQuoter()->quoteValue($partition);

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Clears a column in a specific partition.
     */
    public function clearColumnInPartition(string $table, string $partition, string $column): void
    {
        $quoter = $this->db->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' CLEAR COLUMN ' . $quoter->quoteColumnName($column)
            . ' IN PARTITION ' . $quoter->quoteValue($partition);

        $this->db->createCommand($sql)->execute();
    }
}
