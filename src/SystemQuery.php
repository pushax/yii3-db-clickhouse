<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Connection\ConnectionInterface;

/**
 * Provides helper methods for querying ClickHouse system tables and information.
 *
 * ClickHouse exposes extensive system information via the `system.*` tables.
 * This class provides convenient access to commonly needed system queries.
 *
 * @link https://clickhouse.com/docs/en/operations/system-tables
 */
final class SystemQuery
{
    public function __construct(
        private readonly ConnectionInterface $db,
    ) {}

    /**
     * Returns information about table parts.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getTableParts(string $table, string $database = ''): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                partition,
                name,
                rows,
                bytes_on_disk,
                modification_time,
                active
            FROM system.parts
            WHERE database = COALESCE(:database, currentDatabase())
                AND table = :table
            ORDER BY modification_time DESC
            SQL,
            [
                ':database' => $database ?: null,
                ':table' => $table,
            ],
        )->queryAll();
    }

    /**
     * Returns table size information.
     *
     * @return array<string, mixed>|null
     */
    public function getTableSize(string $table, string $database = ''): ?array
    {
        /** @var array<string, mixed>|false */
        $result = $this->db->createCommand(
            <<<SQL
            SELECT
                sum(rows) as total_rows,
                sum(bytes_on_disk) as total_bytes,
                formatReadableSize(sum(bytes_on_disk)) as readable_size,
                count() as parts_count
            FROM system.parts
            WHERE database = COALESCE(:database, currentDatabase())
                AND table = :table
                AND active = 1
            SQL,
            [
                ':database' => $database ?: null,
                ':table' => $table,
            ],
        )->queryOne();

        return $result === false ? null : $result;
    }

    /**
     * Returns currently running queries (processes).
     *
     * @return array<int, array<string, mixed>>
     */
    public function getProcesses(): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                query_id,
                user,
                query,
                elapsed,
                read_rows,
                read_bytes,
                memory_usage
            FROM system.processes
            ORDER BY elapsed DESC
            SQL,
        )->queryAll();
    }

    /**
     * Returns recent query log entries.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getQueryLog(int $limit = 100): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                query_id,
                query,
                type,
                event_time,
                query_duration_ms,
                read_rows,
                read_bytes,
                result_rows,
                result_bytes,
                memory_usage,
                exception
            FROM system.query_log
            WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
            ORDER BY event_time DESC
            LIMIT :limit
            SQL,
            [':limit' => $limit],
        )->queryAll();
    }

    /**
     * Returns disk usage information.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getDiskUsage(): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                name,
                path,
                free_space,
                total_space,
                formatReadableSize(free_space) as readable_free,
                formatReadableSize(total_space) as readable_total
            FROM system.disks
            SQL,
        )->queryAll();
    }

    /**
     * Returns cluster information.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getClusters(): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                cluster,
                shard_num,
                replica_num,
                host_name,
                host_address,
                port,
                is_local
            FROM system.clusters
            ORDER BY cluster, shard_num, replica_num
            SQL,
        )->queryAll();
    }

    /**
     * Returns replication status for replicated tables.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getReplicationStatus(): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                database,
                table,
                is_leader,
                is_readonly,
                future_parts,
                parts_to_check,
                inserts_in_queue,
                merges_in_queue,
                log_pointer,
                total_replicas,
                active_replicas
            FROM system.replicas
            ORDER BY database, table
            SQL,
        )->queryAll();
    }

    /**
     * Returns merge status information.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getMergeStatus(): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                database,
                table,
                elapsed,
                progress,
                num_parts,
                result_part_name,
                total_size_bytes_compressed,
                formatReadableSize(total_size_bytes_compressed) as readable_size
            FROM system.merges
            ORDER BY elapsed DESC
            SQL,
        )->queryAll();
    }

    /**
     * Optimizes a table by forcing a merge of parts.
     *
     * @param string $table The table name.
     * @param bool $final Whether to force merge into a single part.
     * @param string|null $partition Specific partition to optimize.
     */
    public function optimizeTable(string $table, bool $final = false, ?string $partition = null): void
    {
        $sql = 'OPTIMIZE TABLE ' . $this->db->getQuoter()->quoteTableName($table);

        if ($partition !== null) {
            $sql .= ' PARTITION ' . $this->db->getQuoter()->quoteValue($partition);
        }

        if ($final) {
            $sql .= ' FINAL';
        }

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Returns the table engine and its settings.
     *
     * @return array<string, mixed>|null
     */
    public function getTableEngine(string $table, string $database = ''): ?array
    {
        /** @var array<string, mixed>|false */
        $result = $this->db->createCommand(
            <<<SQL
            SELECT
                engine,
                engine_full,
                partition_key,
                sorting_key,
                primary_key,
                sampling_key,
                total_rows,
                total_bytes,
                comment
            FROM system.tables
            WHERE database = COALESCE(:database, currentDatabase())
                AND name = :table
            SQL,
            [
                ':database' => $database ?: null,
                ':table' => $table,
            ],
        )->queryOne();

        return $result === false ? null : $result;
    }

    /**
     * Returns table column information from system tables.
     *
     * @return array<int, array<string, mixed>>
     */
    public function getTableColumns(string $table, string $database = ''): array
    {
        return $this->db->createCommand(
            <<<SQL
            SELECT
                name,
                type,
                default_kind,
                default_expression,
                comment,
                is_in_partition_key,
                is_in_sorting_key,
                is_in_primary_key,
                is_in_sampling_key,
                compression_codec
            FROM system.columns
            WHERE database = COALESCE(:database, currentDatabase())
                AND table = :table
            ORDER BY position
            SQL,
            [
                ':database' => $database ?: null,
                ':table' => $table,
            ],
        )->queryAll();
    }

    /**
     * Returns server uptime in seconds.
     */
    public function getUptime(): int
    {
        return (int) $this->db->createCommand('SELECT uptime()')->queryScalar();
    }

    /**
     * Returns the server version string.
     */
    public function getVersion(): string
    {
        return (string) $this->db->createCommand('SELECT version()')->queryScalar();
    }

    /**
     * Returns current database name.
     */
    public function getCurrentDatabase(): string
    {
        return (string) $this->db->createCommand('SELECT currentDatabase()')->queryScalar();
    }
}
