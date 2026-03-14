<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Expression\ExpressionInterface;
use Yiisoft\Db\Query\QueryInterface;
use Yiisoft\Db\QueryBuilder\AbstractDMLQueryBuilder;

use function implode;

/**
 * Implements a DML (Data Manipulation Language) SQL statements for ClickHouse.
 *
 * ClickHouse DML specifics:
 * - INSERT is the primary write operation
 * - No traditional UPDATE (use ALTER TABLE ... UPDATE for mutations)
 * - No traditional DELETE (use ALTER TABLE ... DELETE for mutations)
 * - Supports INSERT ... SELECT
 * - No UPSERT in the traditional sense (use ReplacingMergeTree engine)
 */
final class DMLQueryBuilder extends AbstractDMLQueryBuilder
{
    /**
     * @throws NotSupportedException
     */
    public function insertReturningPks(string $table, array|QueryInterface $columns, array &$params = []): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * @throws NotSupportedException
     */
    public function resetSequence(string $table, int|string|null $value = null): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * Builds an UPDATE statement using ALTER TABLE ... UPDATE mutation syntax.
     *
     * Note: In ClickHouse, UPDATE operations are mutations that are processed asynchronously.
     * They modify data on disk and are not suitable for frequent small updates.
     */
    public function update(
        string $table,
        array $columns,
        array|ExpressionInterface|string $condition,
        array|ExpressionInterface|string|null $from = null,
        array &$params = [],
    ): string {
        $updateSets = $this->prepareUpdateSets($table, $columns, $params);

        $sql = 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' UPDATE '
            . implode(', ', $updateSets);

        $where = $this->queryBuilder->buildWhere($condition, $params);

        if ($where === '') {
            $where = 'WHERE 1';
        }

        return "$sql $where";
    }

    /**
     * Builds a DELETE statement using ALTER TABLE ... DELETE mutation syntax.
     *
     * Note: In ClickHouse, DELETE operations are mutations that are processed asynchronously.
     */
    public function delete(string $table, array|string $condition, array &$params): string
    {
        $sql = 'ALTER TABLE ' . $this->quoter->quoteTableName($table) . ' DELETE';

        $where = $this->queryBuilder->buildWhere($condition, $params);

        if ($where === '') {
            $where = 'WHERE 1';
        }

        return "$sql $where";
    }

    /**
     * @throws NotSupportedException
     */
    public function upsert(
        string $table,
        array|QueryInterface $insertColumns,
        array|bool $updateColumns = true,
        array &$params = [],
    ): string {
        // ClickHouse does not support traditional UPSERT.
        // Use ReplacingMergeTree or CollapsingMergeTree engines for similar behavior.
        // Falls back to regular INSERT.
        return $this->insert($table, $insertColumns, $params);
    }

    protected function prepareInsertValues(string $table, array|QueryInterface $columns, array $params = []): array
    {
        if (empty($columns)) {
            return [[], [], 'VALUES ()', []];
        }

        return parent::prepareInsertValues($table, $columns, $params);
    }
}
