<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Migration;

use Pushax\Db\ClickHouse\ClickHouseDataType;
use Pushax\Db\ClickHouse\TableEngine;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Migration\Informer\MigrationInformerInterface;
use Yiisoft\Db\Migration\MigrationBuilder;
use Yiisoft\Db\Schema\Column\ColumnInterface;

use function implode;
use function is_string;
use function microtime;
use function sprintf;

/**
 * Extends the standard MigrationBuilder with ClickHouse-specific table creation support.
 *
 * The standard `createTable()` method uses `$options` parameter to pass the ENGINE and other clauses.
 * This builder adds a `createMergeTreeTable()` convenience method for the most common use case.
 *
 * Usage in migration:
 * ```php
 * use Pushax\Db\ClickHouse\Column\ColumnBuilder;
 * use Pushax\Db\ClickHouse\TableEngine;
 *
 * final class M240101_000000_CreateEventsTable implements MigrationInterface
 * {
 *     public function up(MigrationBuilder $b): void
 *     {
 *         // If $b is a ClickHouseMigrationBuilder, use native methods:
 *         if ($b instanceof ClickHouseMigrationBuilder) {
 *             $b->createMergeTreeTable(
 *                 'events',
 *                 [
 *                     'event_date' => ClickHouseType::DATE,
 *                     'event_type' => ClickHouseType::STRING,
 *                     'user_id'    => ClickHouseType::UINT64,
 *                     'value'      => ClickHouseType::FLOAT64,
 *                 ],
 *                 orderBy: ['event_date', 'event_type', 'user_id'],
 *                 partitionBy: 'toYYYYMM(event_date)',
 *             );
 *         } else {
 *             // Standard approach also works:
 *             $b->createTable('events', [
 *                 'event_date' => ClickHouseType::DATE,
 *                 'event_type' => ClickHouseType::STRING,
 *                 'user_id'    => ClickHouseType::UINT64,
 *                 'value'      => ClickHouseType::FLOAT64,
 *             ], 'ENGINE = MergeTree() PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_type, user_id)');
 *         }
 *     }
 * }
 * ```
 */
final class ClickHouseMigrationBuilder extends MigrationBuilder
{
    public function __construct(
        private readonly ConnectionInterface $clickHouseDb,
        MigrationInformerInterface $informer,
        ?int $maxSqlOutputLength = null,
    ) {
        parent::__construct($clickHouseDb, $informer, $maxSqlOutputLength);
    }

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
            $orderColumns = is_array($orderBy) ? implode(', ', $orderBy) : $orderBy;
            $options .= " ORDER BY ($orderColumns)";
        }

        if ($primaryKey !== null) {
            $pkColumns = is_array($primaryKey) ? implode(', ', $primaryKey) : $primaryKey;
            $options .= " PRIMARY KEY ($pkColumns)";
        }

        if ($sampleBy !== null) {
            $options .= " SAMPLE BY $sampleBy";
        }

        if ($ttl !== null) {
            $options .= " TTL $ttl";
        }

        if (!empty($settings)) {
            $settingParts = [];
            foreach ($settings as $key => $value) {
                if (is_bool($value)) {
                    $settingParts[] = "$key = " . ($value ? '1' : '0');
                } else {
                    $settingParts[] = "$key = $value";
                }
            }
            $options .= ' SETTINGS ' . implode(', ', $settingParts);
        }

        if ($comment !== null) {
            $options .= ' COMMENT ' . $this->clickHouseDb->getQuoter()->quoteValue($comment);
        }

        $this->createTable($table, $columns, $options);
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
        $quoter = $this->clickHouseDb->getQuoter();
        $engineParams = $quoter->quoteValue($zookeeperPath) . ', ' . $quoter->quoteValue($replicaName);

        $options = "ENGINE = ReplicatedMergeTree($engineParams)";

        if ($partitionBy !== null) {
            $options .= " PARTITION BY $partitionBy";
        }

        if ($orderBy !== null) {
            $orderColumns = is_array($orderBy) ? implode(', ', $orderBy) : $orderBy;
            $options .= " ORDER BY ($orderColumns)";
        }

        if ($primaryKey !== null) {
            $pkColumns = is_array($primaryKey) ? implode(', ', $primaryKey) : $primaryKey;
            $options .= " PRIMARY KEY ($pkColumns)";
        }

        if (!empty($settings)) {
            $settingParts = [];
            foreach ($settings as $key => $value) {
                $settingParts[] = "$key = $value";
            }
            $options .= ' SETTINGS ' . implode(', ', $settingParts);
        }

        $this->createTable($table, $columns, $options);
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
        $quoter = $this->clickHouseDb->getQuoter();

        $sql = 'CREATE MATERIALIZED VIEW ' . $quoter->quoteTableName($viewName)
            . ' TO ' . $quoter->quoteTableName($toTable)
            . ' AS ' . $selectQuery;

        $this->execute($sql);
    }

    /**
     * Drops a materialized view.
     */
    public function dropMaterializedView(string $viewName): void
    {
        $sql = 'DROP VIEW IF EXISTS ' . $this->clickHouseDb->getQuoter()->quoteTableName($viewName);
        $this->execute($sql);
    }

    /**
     * Creates a ClickHouse dictionary.
     *
     * @param string $name The dictionary name.
     * @param string $definition The full dictionary definition (structure, source, layout, lifetime).
     */
    public function createDictionary(string $name, string $definition): void
    {
        $sql = 'CREATE DICTIONARY ' . $this->clickHouseDb->getQuoter()->quoteTableName($name)
            . "\n" . $definition;
        $this->execute($sql);
    }

    /**
     * Drops a dictionary.
     */
    public function dropDictionary(string $name): void
    {
        $sql = 'DROP DICTIONARY IF EXISTS ' . $this->clickHouseDb->getQuoter()->quoteTableName($name);
        $this->execute($sql);
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
        $quoter = $this->clickHouseDb->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' ADD INDEX ' . $quoter->quoteColumnName($name)
            . " ($expression) TYPE $type GRANULARITY $granularity";

        $this->execute($sql);
    }

    /**
     * Drops a data skipping index from a table.
     */
    public function dropSkippingIndex(string $table, string $name): void
    {
        $quoter = $this->clickHouseDb->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' DROP INDEX ' . $quoter->quoteColumnName($name);

        $this->execute($sql);
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
        $quoter = $this->clickHouseDb->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' ADD PROJECTION ' . $quoter->quoteColumnName($name)
            . " ($selectQuery)";

        $this->execute($sql);
    }

    /**
     * Drops a projection from a table.
     */
    public function dropProjection(string $table, string $name): void
    {
        $quoter = $this->clickHouseDb->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($table)
            . ' DROP PROJECTION ' . $quoter->quoteColumnName($name);

        $this->execute($sql);
    }

    /**
     * Modifies the ORDER BY clause of a MergeTree table.
     *
     * @param string $table The table name.
     * @param string|string[] $columns The new ORDER BY columns.
     */
    public function modifyOrderBy(string $table, string|array $columns): void
    {
        $orderColumns = is_array($columns) ? implode(', ', $columns) : $columns;
        $sql = 'ALTER TABLE ' . $this->clickHouseDb->getQuoter()->quoteTableName($table)
            . " MODIFY ORDER BY ($orderColumns)";

        $this->execute($sql);
    }

    /**
     * Modifies the TTL of a table.
     *
     * @param string $table The table name.
     * @param string $ttlExpression The TTL expression.
     */
    public function modifyTtl(string $table, string $ttlExpression): void
    {
        $sql = 'ALTER TABLE ' . $this->clickHouseDb->getQuoter()->quoteTableName($table)
            . " MODIFY TTL $ttlExpression";

        $this->execute($sql);
    }

    /**
     * Removes TTL from a table.
     */
    public function removeTtl(string $table): void
    {
        $sql = 'ALTER TABLE ' . $this->clickHouseDb->getQuoter()->quoteTableName($table)
            . ' REMOVE TTL';

        $this->execute($sql);
    }

    /**
     * Modifies a table setting.
     *
     * @param string $table The table name.
     * @param array<string, mixed> $settings The settings to modify.
     */
    public function modifySettings(string $table, array $settings): void
    {
        $settingParts = [];
        foreach ($settings as $key => $value) {
            if (is_bool($value)) {
                $settingParts[] = "$key = " . ($value ? '1' : '0');
            } else {
                $settingParts[] = "$key = $value";
            }
        }

        $sql = 'ALTER TABLE ' . $this->clickHouseDb->getQuoter()->quoteTableName($table)
            . ' MODIFY SETTING ' . implode(', ', $settingParts);

        $this->execute($sql);
    }
}
