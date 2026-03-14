<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Connection\ConnectionInterface;

use function array_map;
use function count;
use function implode;
use function is_bool;
use function is_float;
use function is_int;
use function is_null;

/**
 * Provides optimized batch insert functionality for ClickHouse.
 *
 * ClickHouse is optimized for bulk inserts. Inserting data in large batches
 * (thousands or millions of rows) is much more efficient than inserting rows one by one.
 *
 * Usage example:
 * ```php
 * $batch = new BatchInsert($connection, 'events', ['event_date', 'user_id', 'event_type', 'value']);
 *
 * // Add rows
 * $batch->addRow(['2024-01-01', 1, 'click', 1.5]);
 * $batch->addRow(['2024-01-01', 2, 'view', 0.0]);
 *
 * // Or add multiple rows at once
 * $batch->addRows([
 *     ['2024-01-01', 3, 'click', 2.0],
 *     ['2024-01-01', 4, 'purchase', 100.0],
 * ]);
 *
 * // Execute the batch insert
 * $batch->execute();
 * ```
 */
final class BatchInsert
{
    /** @var list<list<mixed>> */
    private array $rows = [];

    /**
     * @param ConnectionInterface $db The database connection.
     * @param string $table The table name.
     * @param string[] $columns The column names.
     * @param int $batchSize Maximum number of rows per insert statement (0 = unlimited).
     */
    public function __construct(
        private readonly ConnectionInterface $db,
        private readonly string $table,
        private readonly array $columns,
        private readonly int $batchSize = 0,
    ) {}

    /**
     * Adds a single row to the batch.
     *
     * @param list<mixed> $row The row values in the same order as columns.
     */
    public function addRow(array $row): self
    {
        $this->rows[] = $row;
        return $this;
    }

    /**
     * Adds multiple rows to the batch.
     *
     * @param list<list<mixed>> $rows The rows to add.
     */
    public function addRows(array $rows): self
    {
        foreach ($rows as $row) {
            $this->rows[] = $row;
        }
        return $this;
    }

    /**
     * Executes the batch insert.
     *
     * @return int The total number of rows inserted.
     */
    public function execute(): int
    {
        if (empty($this->rows)) {
            return 0;
        }

        $totalRows = 0;

        if ($this->batchSize > 0) {
            $batches = array_chunk($this->rows, $this->batchSize);
        } else {
            $batches = [$this->rows];
        }

        foreach ($batches as $batch) {
            $sql = $this->buildSql($batch);
            $this->db->createCommand($sql)->execute();
            $totalRows += count($batch);
        }

        $this->rows = [];

        return $totalRows;
    }

    /**
     * Returns the number of pending rows.
     */
    public function getPendingCount(): int
    {
        return count($this->rows);
    }

    /**
     * Clears all pending rows without executing.
     */
    public function clear(): self
    {
        $this->rows = [];
        return $this;
    }

    /**
     * @param list<list<mixed>> $rows
     */
    private function buildSql(array $rows): string
    {
        $quoter = $this->db->getQuoter();

        $quotedColumns = array_map($quoter->quoteColumnName(...), $this->columns);

        $sql = 'INSERT INTO ' . $quoter->quoteTableName($this->table)
            . ' (' . implode(', ', $quotedColumns) . ') VALUES ';

        $valueSets = [];

        foreach ($rows as $row) {
            $values = [];
            foreach ($row as $value) {
                $values[] = $this->quoteScalar($value, $quoter);
            }
            $valueSets[] = '(' . implode(', ', $values) . ')';
        }

        return $sql . implode(', ', $valueSets);
    }

    private function quoteScalar(mixed $value, \Yiisoft\Db\Schema\QuoterInterface $quoter): string
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        return $quoter->quoteValue((string) $value);
    }
}
