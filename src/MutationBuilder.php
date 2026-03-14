<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Connection\ConnectionInterface;

/**
 * Provides functionality for building and executing ClickHouse mutations.
 *
 * Mutations are ALTER TABLE operations in ClickHouse that modify or delete data.
 * Unlike traditional RDBMS, mutations in ClickHouse are:
 * - Asynchronous by default
 * - Heavy operations that rewrite data parts
 * - Not suitable for frequent small updates
 *
 * Usage example:
 * ```php
 * $mutation = new MutationBuilder($connection);
 *
 * // Update mutation
 * $mutation->update('events', ['status' => 'processed'], 'event_date < today()');
 *
 * // Delete mutation
 * $mutation->delete('events', 'event_date < today() - 30');
 *
 * // Check mutation status
 * $status = $mutation->getMutationStatus('events');
 * ```
 *
 * @link https://clickhouse.com/docs/en/sql-reference/statements/alter#mutations
 */
final class MutationBuilder
{
    public function __construct(
        private readonly ConnectionInterface $db,
    ) {}

    /**
     * Executes an ALTER TABLE ... UPDATE mutation.
     *
     * @param string $table The table name.
     * @param array<string, mixed> $columns Column => value pairs to update.
     * @param string $condition The WHERE condition.
     */
    public function update(string $table, array $columns, string $condition): void
    {
        $quoter = $this->db->getQuoter();
        $sets = [];

        foreach ($columns as $column => $value) {
            $quotedColumn = $quoter->quoteColumnName($column);
            if ($value === null) {
                $sets[] = "$quotedColumn = NULL";
            } elseif (is_bool($value)) {
                $sets[] = "$quotedColumn = " . ($value ? '1' : '0');
            } elseif (is_int($value) || is_float($value)) {
                $sets[] = "$quotedColumn = $value";
            } else {
                $sets[] = "$quotedColumn = " . $quoter->quoteValue((string) $value);
            }
        }

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' UPDATE ' . implode(', ', $sets)
            . ' WHERE ' . $condition;

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Executes an ALTER TABLE ... DELETE mutation.
     *
     * @param string $table The table name.
     * @param string $condition The WHERE condition for rows to delete.
     */
    public function delete(string $table, string $condition): void
    {
        $quoter = $this->db->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' DELETE WHERE ' . $condition;

        $this->db->createCommand($sql)->execute();
    }

    /**
     * Gets the status of mutations for a given table.
     *
     * @param string $table The table name.
     * @param string $database The database name (empty for current database).
     *
     * @return array<int, array<string, mixed>> List of mutations with their status.
     */
    public function getMutationStatus(string $table, string $database = ''): array
    {
        $sql = <<<SQL
        SELECT
            mutation_id,
            command,
            create_time,
            is_done,
            parts_to_do,
            latest_fail_reason
        FROM system.mutations
        WHERE database = COALESCE(:database, currentDatabase())
            AND table = :table
        ORDER BY create_time DESC
        SQL;

        return $this->db->createCommand($sql, [
            ':database' => $database ?: null,
            ':table' => $table,
        ])->queryAll();
    }

    /**
     * Waits for all mutations on a table to complete.
     *
     * @param string $table The table name.
     * @param int $timeoutSeconds Maximum wait time in seconds.
     *
     * @return bool Whether all mutations completed within the timeout.
     */
    public function waitForMutations(string $table, int $timeoutSeconds = 60): bool
    {
        $startTime = time();

        while (time() - $startTime < $timeoutSeconds) {
            $pendingMutations = $this->db->createCommand(
                <<<SQL
                SELECT count() as cnt
                FROM system.mutations
                WHERE database = currentDatabase()
                    AND table = :table
                    AND is_done = 0
                SQL,
                [':table' => $table],
            )->queryScalar();

            if ((int) $pendingMutations === 0) {
                return true;
            }

            usleep(500_000); // 500ms
        }

        return false;
    }

    /**
     * Kills a running mutation.
     *
     * @param string $mutationId The mutation ID to kill.
     * @param string $table The table name.
     */
    public function killMutation(string $mutationId, string $table): void
    {
        $sql = <<<SQL
        KILL MUTATION
        WHERE database = currentDatabase()
            AND table = :table
            AND mutation_id = :mutationId
        SQL;

        $this->db->createCommand($sql, [
            ':table' => $table,
            ':mutationId' => $mutationId,
        ])->execute();
    }
}
