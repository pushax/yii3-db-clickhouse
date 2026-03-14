<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Schema\Column\ColumnInterface;

/**
 * Provides a fluent interface for building ClickHouse CREATE TABLE statements.
 *
 * ClickHouse tables require specific clauses that are not found in traditional RDBMS:
 * - ENGINE: The table engine (MergeTree, ReplacingMergeTree, etc.)
 * - ORDER BY: The sorting key (required for MergeTree family)
 * - PARTITION BY: The partitioning expression
 * - PRIMARY KEY: The primary key (subset of ORDER BY)
 * - SAMPLE BY: The sampling expression
 * - TTL: Time-to-live expressions for data expiration
 * - SETTINGS: Engine-specific settings
 *
 * Usage example:
 * ```php
 * $builder = new TableBuilder($connection);
 * $sql = $builder
 *     ->table('events')
 *     ->column('event_date', 'Date')
 *     ->column('event_type', 'String')
 *     ->column('user_id', 'UInt64')
 *     ->column('value', 'Float64')
 *     ->engine(TableEngine::MERGE_TREE)
 *     ->partitionBy('toYYYYMM(event_date)')
 *     ->orderBy(['event_date', 'event_type', 'user_id'])
 *     ->primaryKey(['event_date', 'event_type'])
 *     ->sampleBy('user_id')
 *     ->ttl('event_date + INTERVAL 1 MONTH')
 *     ->settings(['index_granularity' => 8192])
 *     ->build();
 * ```
 */
final class TableBuilder
{
    private string $tableName = '';
    private string $engine = TableEngine::MERGE_TREE;
    private ?string $engineParams = null;
    private bool $ifNotExists = false;
    private ?string $database = null;
    private ?string $onCluster = null;

    /** @var array<string, string|ColumnInterface> */
    private array $columns = [];

    /** @var string[] */
    private array $orderBy = [];

    /** @var string[] */
    private array $primaryKey = [];

    private ?string $partitionBy = null;
    private ?string $sampleBy = null;

    /** @var string[] */
    private array $ttl = [];

    /** @var array<string, mixed> */
    private array $settings = [];

    private ?string $comment = null;

    /** @var string[] */
    private array $indices = [];

    public function __construct(
        private readonly ConnectionInterface $db,
    ) {}

    public function table(string $name): self
    {
        $this->tableName = $name;
        return $this;
    }

    public function ifNotExists(bool $ifNotExists = true): self
    {
        $this->ifNotExists = $ifNotExists;
        return $this;
    }

    public function database(string $database): self
    {
        $this->database = $database;
        return $this;
    }

    public function onCluster(string $cluster): self
    {
        $this->onCluster = $cluster;
        return $this;
    }

    /**
     * @param string|ColumnInterface $type The column type as a string or ColumnInterface instance.
     */
    public function column(string $name, string|ColumnInterface $type): self
    {
        $this->columns[$name] = $type;
        return $this;
    }

    /**
     * @param array<string, string|ColumnInterface> $columns
     */
    public function columns(array $columns): self
    {
        foreach ($columns as $name => $type) {
            $this->columns[$name] = $type;
        }
        return $this;
    }

    /**
     * Sets the table engine.
     *
     * @param string $engine One of {@see TableEngine} constants or any valid engine name.
     * @param string|null $params Engine parameters (e.g., for ReplicatedMergeTree).
     */
    public function engine(string $engine, ?string $params = null): self
    {
        $this->engine = $engine;
        $this->engineParams = $params;
        return $this;
    }

    /**
     * @param string|string[] $columns
     */
    public function orderBy(string|array $columns): self
    {
        $this->orderBy = (array) $columns;
        return $this;
    }

    /**
     * @param string|string[] $columns
     */
    public function primaryKey(string|array $columns): self
    {
        $this->primaryKey = (array) $columns;
        return $this;
    }

    public function partitionBy(string $expression): self
    {
        $this->partitionBy = $expression;
        return $this;
    }

    public function sampleBy(string $expression): self
    {
        $this->sampleBy = $expression;
        return $this;
    }

    /**
     * Adds a TTL expression.
     *
     * @param string $expression E.g., 'event_date + INTERVAL 1 MONTH'
     */
    public function ttl(string $expression): self
    {
        $this->ttl[] = $expression;
        return $this;
    }

    /**
     * @param array<string, mixed> $settings
     */
    public function settings(array $settings): self
    {
        $this->settings = array_merge($this->settings, $settings);
        return $this;
    }

    public function comment(string $comment): self
    {
        $this->comment = $comment;
        return $this;
    }

    /**
     * Add a data skipping index.
     *
     * @param string $name Index name.
     * @param string $expression The expression to index.
     * @param string $type Index type (e.g., 'minmax', 'set(100)', 'bloom_filter').
     * @param int $granularity Index granularity.
     */
    public function index(string $name, string $expression, string $type, int $granularity = 1): self
    {
        $quotedName = $this->db->getQuoter()->quoteColumnName($name);
        $this->indices[] = "INDEX $quotedName $expression TYPE $type GRANULARITY $granularity";
        return $this;
    }

    /**
     * Builds the CREATE TABLE SQL statement.
     */
    public function build(): string
    {
        $quoter = $this->db->getQuoter();

        $sql = 'CREATE TABLE';

        if ($this->ifNotExists) {
            $sql .= ' IF NOT EXISTS';
        }

        $tableName = $this->database !== null
            ? $quoter->quoteTableName($this->database) . '.' . $quoter->quoteTableName($this->tableName)
            : $quoter->quoteTableName($this->tableName);

        $sql .= ' ' . $tableName;

        if ($this->onCluster !== null) {
            $sql .= ' ON CLUSTER ' . $quoter->quoteValue($this->onCluster);
        }

        // Columns
        $columnDefs = [];
        $columnDefBuilder = $this->db->getQueryBuilder()->getColumnDefinitionBuilder();

        foreach ($this->columns as $name => $type) {
            $quotedName = $quoter->quoteColumnName($name);
            if ($type instanceof ColumnInterface) {
                $columnDefs[] = "    $quotedName " . $columnDefBuilder->build($type);
            } else {
                $columnDefs[] = "    $quotedName $type";
            }
        }

        // Add indices to column definitions block
        foreach ($this->indices as $index) {
            $columnDefs[] = "    $index";
        }

        if (!empty($columnDefs)) {
            $sql .= "\n(\n" . implode(",\n", $columnDefs) . "\n)";
        }

        // Engine
        $sql .= "\nENGINE = $this->engine";
        if ($this->engineParams !== null) {
            $sql .= "($this->engineParams)";
        } elseif (!in_array($this->engine, [
            TableEngine::MEMORY,
            TableEngine::NULL,
            TableEngine::TINY_LOG,
            TableEngine::STRIPE_LOG,
            TableEngine::LOG,
            TableEngine::SET,
        ], true)) {
            $sql .= '()';
        }

        // PARTITION BY
        if ($this->partitionBy !== null) {
            $sql .= "\nPARTITION BY $this->partitionBy";
        }

        // PRIMARY KEY
        if (!empty($this->primaryKey)) {
            $quotedPks = array_map($quoter->quoteColumnName(...), $this->primaryKey);
            $sql .= "\nPRIMARY KEY (" . implode(', ', $quotedPks) . ')';
        }

        // ORDER BY
        if (!empty($this->orderBy)) {
            $quotedOrder = array_map($quoter->quoteColumnName(...), $this->orderBy);
            $sql .= "\nORDER BY (" . implode(', ', $quotedOrder) . ')';
        } elseif (!empty($this->primaryKey)) {
            $quotedOrder = array_map($quoter->quoteColumnName(...), $this->primaryKey);
            $sql .= "\nORDER BY (" . implode(', ', $quotedOrder) . ')';
        }

        // SAMPLE BY
        if ($this->sampleBy !== null) {
            $sql .= "\nSAMPLE BY $this->sampleBy";
        }

        // TTL
        if (!empty($this->ttl)) {
            $sql .= "\nTTL " . implode(",\n    ", $this->ttl);
        }

        // SETTINGS
        if (!empty($this->settings)) {
            $settingParts = [];
            foreach ($this->settings as $key => $value) {
                if (is_bool($value)) {
                    $settingParts[] = "$key = " . ($value ? '1' : '0');
                } elseif (is_string($value)) {
                    $settingParts[] = "$key = " . $quoter->quoteValue($value);
                } else {
                    $settingParts[] = "$key = $value";
                }
            }
            $sql .= "\nSETTINGS " . implode(",\n    ", $settingParts);
        }

        // COMMENT
        if ($this->comment !== null) {
            $sql .= "\nCOMMENT " . $quoter->quoteValue($this->comment);
        }

        return $sql;
    }

    /**
     * Executes the CREATE TABLE statement.
     */
    public function execute(): void
    {
        $this->db->createCommand($this->build())->execute();
    }
}
