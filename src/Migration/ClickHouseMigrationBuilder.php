<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Migration;

use Pushax\Db\ClickHouse\ClickHouseDataType;
use Pushax\Db\ClickHouse\TableEngine;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Migration\Informer\MigrationInformerInterface;
use Yiisoft\Db\Migration\MigrationBuilder;
use Yiisoft\Db\Query\QueryInterface;
use Yiisoft\Db\Schema\Column\ColumnInterface;

use function implode;
use function is_bool;
use function is_string;

/**
 * Wraps MigrationBuilder with ClickHouse-specific table creation helpers.
 *
 * Since MigrationBuilder is final, this class uses composition and delegates
 * all standard migration methods to the inner builder instance.
 *
 * Usage in migration:
 * ```php
 * use Pushax\Db\ClickHouse\ClickHouseDataType;
 * use Pushax\Db\ClickHouse\TableEngine;
 *
 * final class M240101_000000_CreateEventsTable implements ClickHouseMigrationInterface
 * {
 *     public function up(ClickHouseMigrationBuilder $b): void
 *     {
 *         $b->createMergeTreeTable('events',
 *             [
 *                 'event_date' => ClickHouseDataType::DATE,
 *                 'event_type' => ClickHouseDataType::STRING,
 *                 'user_id' => ClickHouseDataType::UINT64,
 *                 'value' => ClickHouseDataType::FLOAT64,
 *             ],
 *             orderBy: ['event_date', 'event_type', 'user_id'],
 *             partitionBy: 'toYYYYMM(event_date)');
 *     }
 *
 *     public function down(ClickHouseMigrationBuilder $b): void
 *     {
 *         $b->dropTable('events');
 *     }
 * }
 * ```
 */
final class ClickHouseMigrationBuilder
{
    private readonly MigrationBuilder $builder;

    public function __construct(
        private readonly ConnectionInterface $db,
        MigrationInformerInterface $informer,
        ?int $maxSqlOutputLength = null,
    ) {
        $this->builder = new MigrationBuilder($db, $informer, $maxSqlOutputLength);
    }

    // -------------------------------------------------------------------------
    // ClickHouse-specific methods
    // -------------------------------------------------------------------------

    /**
     * Creates a table with MergeTree family engine.
     *
     * @param string $table The table name.
     * @param array<string, ColumnInterface|string> $columns Column definitions.
     * @param string $engine The table engine. Default is MergeTree.
     * @param string|string[]|null $orderBy ORDER BY columns (required for MergeTree family).
     * @param string|string[]|null $primaryKey PRIMARY KEY columns (defaults to orderBy if not specified).
     * @param string|null $partitionBy PARTITION BY expression.
     * @param string|null $sampleBy SAMPLE BY expression.
     * @param string|null $ttl TTL expression.
     * @param array<string, mixed> $settings Engine settings.
     * @param string|null $comment Table comment.
     */
    public function createMergeTreeTable(
        string $table,
        array $columns,
        string $engine = TableEngine::MERGE_TREE,
        string|array|null $orderBy = null,
        string|array|null $primaryKey = null,
        ?string $partitionBy = null,
        ?string $sampleBy = null,
        ?string $ttl = null,
        array $settings = [],
        ?string $comment = null,
    ): void {
        $options = "ENGINE = $engine()";

        if ($partitionBy !== null) {
            $options .= " PARTITION BY $partitionBy";
        }

        if ($orderBy !== null) {
            $orderColumns = is_string($orderBy) ? $orderBy : implode(', ', $orderBy);
            $options .= " ORDER BY ($orderColumns)";
        }

        if ($primaryKey !== null) {
            $pkColumns = is_string($primaryKey) ? $primaryKey : implode(', ', $primaryKey);
            $options .= " PRIMARY KEY ($pkColumns)";
        }

        if ($sampleBy !== null) {
            $options .= " SAMPLE BY $sampleBy";
        }

        if ($ttl !== null) {
            $options .= " TTL $ttl";
        }

        if (!empty($settings)) {
            $parts = [];
            foreach ($settings as $key => $value) {
                $parts[] = "$key = " . (is_bool($value) ? ($value ? '1' : '0') : $value);
            }
            $options .= ' SETTINGS ' . implode(', ', $parts);
        }

        if ($comment !== null) {
            $options .= ' COMMENT ' . $this->db->getQuoter()->quoteValue($comment);
        }

        $this->builder->createTable($table, $columns, $options);
    }

    /**
     * Creates a Replicated MergeTree table.
     *
     * @param string $table The table name.
     * @param array<string, ColumnInterface|string> $columns Column definitions.
     * @param string $zookeeperPath The ZooKeeper path for the table.
     * @param string $replicaName The replica name.
     * @param string|string[]|null $orderBy ORDER BY columns.
     * @param string|string[]|null $primaryKey PRIMARY KEY columns.
     * @param string|null $partitionBy PARTITION BY expression.
     * @param array<string, mixed> $settings Engine settings.
     */
    public function createReplicatedTable(
        string $table,
        array $columns,
        string $zookeeperPath,
        string $replicaName,
        string|array|null $orderBy = null,
        string|array|null $primaryKey = null,
        ?string $partitionBy = null,
        array $settings = [],
    ): void {
        $quoter = $this->db->getQuoter();
        $engineParams = $quoter->quoteValue($zookeeperPath) . ', ' . $quoter->quoteValue($replicaName);
        $options = "ENGINE = ReplicatedMergeTree($engineParams)";

        if ($partitionBy !== null) {
            $options .= " PARTITION BY $partitionBy";
        }

        if ($orderBy !== null) {
            $orderColumns = is_string($orderBy) ? $orderBy : implode(', ', $orderBy);
            $options .= " ORDER BY ($orderColumns)";
        }

        if ($primaryKey !== null) {
            $pkColumns = is_string($primaryKey) ? $primaryKey : implode(', ', $primaryKey);
            $options .= " PRIMARY KEY ($pkColumns)";
        }

        if (!empty($settings)) {
            $parts = [];
            foreach ($settings as $key => $value) {
                $parts[] = "$key = $value";
            }
            $options .= ' SETTINGS ' . implode(', ', $parts);
        }

        $this->builder->createTable($table, $columns, $options);
    }

    /**
     * Creates a materialized view.
     *
     * @param string $viewName The view name.
     * @param string $toTable The target table (where data is stored).
     * @param string $selectQuery The SELECT query that populates the view.
     */
    public function createMaterializedView(string $viewName, string $toTable, string $selectQuery): void
    {
        $quoter = $this->db->getQuoter();
        $sql = 'CREATE MATERIALIZED VIEW ' . $quoter->quoteTableName($viewName)
            . ' TO ' . $quoter->quoteTableName($toTable)
            . ' AS ' . $selectQuery;
        $this->builder->execute($sql);
    }

    /**
     * Drops a materialized view.
     */
    public function dropMaterializedView(string $viewName): void
    {
        $sql = 'DROP VIEW IF EXISTS ' . $this->db->getQuoter()->quoteTableName($viewName);
        $this->builder->execute($sql);
    }

    /**
     * Creates a ClickHouse dictionary.
     *
     * @param string $name The dictionary name.
     * @param string $definition The full dictionary definition (structure, source, layout, lifetime).
     */
    public function createDictionary(string $name, string $definition): void
    {
        $sql = 'CREATE DICTIONARY ' . $this->db->getQuoter()->quoteTableName($name) . "\n" . $definition;
        $this->builder->execute($sql);
    }

    /**
     * Drops a dictionary.
     */
    public function dropDictionary(string $name): void
    {
        $sql = 'DROP DICTIONARY IF EXISTS ' . $this->db->getQuoter()->quoteTableName($name);
        $this->builder->execute($sql);
    }

    /**
     * Adds a data skipping index to a table.
     *
     * @param string $table The table name.
     * @param string $name The index name.
     * @param string $expression The expression to index.
     * @param string $type Index type (e.g., 'minmax', 'set(100)', 'bloom_filter').
     * @param int $granularity Index granularity.
     */
    public function addSkippingIndex(
        string $table,
        string $name,
        string $expression,
        string $type,
        int $granularity = 1,
    ): void {
        $quoter = $this->db->getQuoter();
        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' ADD INDEX ' . $quoter->quoteColumnName($name)
            . " ($expression) TYPE $type GRANULARITY $granularity";
        $this->builder->execute($sql);
    }

    /**
     * Drops a data skipping index from a table.
     */
    public function dropSkippingIndex(string $table, string $name): void
    {
        $quoter = $this->db->getQuoter();
        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' DROP INDEX ' . $quoter->quoteColumnName($name);
        $this->builder->execute($sql);
    }

    /**
     * Adds a projection to a table.
     *
     * @param string $table The table name.
     * @param string $name The projection name.
     * @param string $selectQuery The SELECT query for the projection.
     */
    public function addProjection(string $table, string $name, string $selectQuery): void
    {
        $quoter = $this->db->getQuoter();
        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' ADD PROJECTION ' . $quoter->quoteColumnName($name)
            . " ($selectQuery)";
        $this->builder->execute($sql);
    }

    /**
     * Drops a projection from a table.
     */
    public function dropProjection(string $table, string $name): void
    {
        $quoter = $this->db->getQuoter();
        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' DROP PROJECTION ' . $quoter->quoteColumnName($name);
        $this->builder->execute($sql);
    }

    /**
     * Modifies the ORDER BY clause of a MergeTree table.
     *
     * @param string $table The table name.
     * @param string|string[] $columns The new ORDER BY columns.
     */
    public function modifyOrderBy(string $table, string|array $columns): void
    {
        $orderColumns = is_string($columns) ? $columns : implode(', ', $columns);
        $sql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . " MODIFY ORDER BY ($orderColumns)";
        $this->builder->execute($sql);
    }

    /**
     * Modifies the TTL of a table.
     *
     * @param string $table The table name.
     * @param string $ttlExpression The TTL expression.
     */
    public function modifyTtl(string $table, string $ttlExpression): void
    {
        $sql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . " MODIFY TTL $ttlExpression";
        $this->builder->execute($sql);
    }

    /**
     * Removes TTL from a table.
     */
    public function removeTtl(string $table): void
    {
        $sql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table) . ' REMOVE TTL';
        $this->builder->execute($sql);
    }

    /**
     * Modifies a table setting.
     *
     * @param string $table The table name.
     * @param array<string, mixed> $settings The settings to modify.
     */
    public function modifySettings(string $table, array $settings): void
    {
        $parts = [];
        foreach ($settings as $key => $value) {
            $parts[] = "$key = " . (is_bool($value) ? ($value ? '1' : '0') : $value);
        }
        $sql = 'ALTER TABLE ' . $this->db->getQuoter()->quoteTableName($table)
            . ' MODIFY SETTING ' . implode(', ', $parts);
        $this->builder->execute($sql);
    }

    // -------------------------------------------------------------------------
    // Delegated standard MigrationBuilder methods
    // -------------------------------------------------------------------------

    public function getDb(): ConnectionInterface
    {
        return $this->builder->getDb();
    }

    public function execute(string $sql, array $params = []): void
    {
        $this->builder->execute($sql, $params);
    }

    public function insert(string $table, array $columns): void
    {
        $this->builder->insert($table, $columns);
    }

    public function batchInsert(string $table, array $columns, iterable $rows): void
    {
        $this->builder->batchInsert($table, $columns, $rows);
    }

    public function update(string $table, array $columns, array|string $condition = '', array $params = []): void
    {
        $this->builder->update($table, $columns, $condition, $params);
    }

    public function delete(string $table, array|string $condition = '', array $params = []): void
    {
        $this->builder->delete($table, $condition, $params);
    }

    public function createTable(string $table, array $columns, ?string $options = null): void
    {
        $this->builder->createTable($table, $columns, $options);
    }

    public function renameTable(string $table, string $newName): void
    {
        $this->builder->renameTable($table, $newName);
    }

    public function dropTable(string $table): void
    {
        $this->builder->dropTable($table);
    }

    public function truncateTable(string $table): void
    {
        $this->builder->truncateTable($table);
    }

    public function addColumn(string $table, string $column, ColumnInterface|string $type): void
    {
        $this->builder->addColumn($table, $column, $type);
    }

    public function dropColumn(string $table, string $column): void
    {
        $this->builder->dropColumn($table, $column);
    }

    public function renameColumn(string $table, string $name, string $newName): void
    {
        $this->builder->renameColumn($table, $name, $newName);
    }

    public function alterColumn(string $table, string $column, ColumnInterface|string $type): void
    {
        $this->builder->alterColumn($table, $column, $type);
    }

    public function addPrimaryKey(string $table, string $name, array|string $columns): void
    {
        $this->builder->addPrimaryKey($table, $name, $columns);
    }

    public function dropPrimaryKey(string $table, string $name): void
    {
        $this->builder->dropPrimaryKey($table, $name);
    }

    public function createIndex(
        string $table,
        string $name,
        array|string $columns,
        ?string $indexType = null,
        ?string $indexMethod = null,
    ): void {
        $this->builder->createIndex($table, $name, $columns, $indexType, $indexMethod);
    }

    public function dropIndex(string $table, string $name): void
    {
        $this->builder->dropIndex($table, $name);
    }

    public function createView(string $viewName, QueryInterface|string $subQuery): void
    {
        $this->builder->createView($viewName, $subQuery);
    }

    public function dropView(string $viewName): void
    {
        $this->builder->dropView($viewName);
    }

    public function addCommentOnColumn(string $table, string $column, string $comment): void
    {
        $this->builder->addCommentOnColumn($table, $column, $comment);
    }

    public function addCommentOnTable(string $table, string $comment): void
    {
        $this->builder->addCommentOnTable($table, $comment);
    }

    public function dropCommentFromColumn(string $table, string $column): void
    {
        $this->builder->dropCommentFromColumn($table, $column);
    }

    public function dropCommentFromTable(string $table): void
    {
        $this->builder->dropCommentFromTable($table);
    }
}
