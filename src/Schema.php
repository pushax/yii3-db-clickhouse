<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Constant\ColumnInfoSource;
use Yiisoft\Db\Constant\ColumnType;
use Yiisoft\Db\Constraint\Index;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Schema\AbstractSchema;
use Yiisoft\Db\Schema\Column\ColumnInterface;
use Yiisoft\Db\Schema\SchemaInterface;
use Yiisoft\Db\Schema\TableSchema;
use Yiisoft\Db\Schema\TableSchemaInterface;

use function array_change_key_case;
use function array_column;
use function in_array;
use function is_string;
use function md5;
use function preg_match;
use function serialize;
use function str_contains;
use function strtolower;

/**
 * Implements ClickHouse specific schema, supporting ClickHouse 21.8 and higher.
 *
 * ClickHouse has a different approach to schema compared to traditional RDBMS:
 * - No foreign keys
 * - No traditional primary keys (uses ORDER BY for sorting)
 * - Supports various table engines (MergeTree, ReplacingMergeTree, etc.)
 * - No UPDATE/DELETE in the traditional sense
 *
 * @psalm-type ColumnArray = array{
 *   name: string,
 *   type: string,
 *   default_kind: string,
 *   default_expression: string,
 *   comment: string,
 *   is_in_partition_key: int,
 *   is_in_sorting_key: int,
 *   is_in_primary_key: int,
 *   is_in_sampling_key: int,
 *   database: string,
 *   table: string
 * }
 */
final class Schema extends AbstractSchema
{
    protected function getCacheKey(string $name): array
    {
        assert($this->db instanceof Connection);
        $driver = $this->db->getDriver();
        return [static::class, $driver->getDsn(), $driver->getUsername(), $name];
    }

    protected function getCacheTag(): string
    {
        assert($this->db instanceof Connection);
        $driver = $this->db->getDriver();
        return md5(serialize([static::class, $driver->getDsn(), $driver->getUsername()]));
    }

    protected function getResultColumnCacheKey(array $metadata): string
    {
        assert($this->db instanceof Connection);
        $driver = $this->db->getDriver();
        return md5(serialize([static::class . '::getResultColumn', $driver->getDsn(), ...$metadata]));
    }

    protected function loadResultColumn(array $metadata): ?ColumnInterface
    {
        $type = $metadata['type'] ?? $metadata['native_type'] ?? '';

        if (empty($type) || $type === 'NULL') {
            return null;
        }

        $columnInfo = ['source' => ColumnInfoSource::QUERY_RESULT];

        if (!empty($metadata['name'])) {
            $columnInfo['name'] = $metadata['name'];
        }

        return $this->db->getColumnFactory()->fromDbType(strtolower($type), $columnInfo);
    }

    protected function findConstraints(TableSchemaInterface $table): void
    {
        $tableName = $this->resolveFullName($table->getName(), $table->getSchemaName());

        $table->indexes(...$this->getTableMetadata($tableName, SchemaInterface::INDEXES));
    }

    /**
     * Collects the metadata of table columns.
     *
     * @param TableSchemaInterface $table The table metadata.
     *
     * @return bool Whether the table exists in the database.
     */
    protected function findColumns(TableSchemaInterface $table): bool
    {
        $schemaName = $table->getSchemaName();
        $tableName = $table->getName();

        $database = $schemaName ?: 'currentDatabase()';
        $databaseParam = $schemaName ?: null;

        $sql = <<<SQL
        SELECT
            `name`,
            `type`,
            `default_kind`,
            `default_expression`,
            `comment`,
            `is_in_partition_key`,
            `is_in_sorting_key`,
            `is_in_primary_key`,
            `is_in_sampling_key`
        FROM `system`.`columns`
        WHERE `database` = COALESCE(:schemaName, currentDatabase())
            AND `table` = :tableName
        ORDER BY `position`
        SQL;

        $columns = $this->db->createCommand($sql, [
            ':schemaName' => $databaseParam,
            ':tableName' => $tableName,
        ])->queryAll();

        if (empty($columns)) {
            return false;
        }

        foreach ($columns as $info) {
            $info = array_change_key_case($info);

            $info['database'] = $schemaName;
            $info['table'] = $tableName;

            /** @psalm-var ColumnArray $info */
            $column = $this->loadColumn($info);
            $table->column($info['name'], $column);
        }

        return true;
    }

    protected function findSchemaNames(): array
    {
        $sql = <<<SQL
        SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        SQL;

        /** @var string[] */
        return $this->db->createCommand($sql)->queryColumn();
    }

    protected function findTableComment(TableSchemaInterface $tableSchema): void
    {
        $sql = <<<SQL
        SELECT `comment`
        FROM `system`.`tables`
        WHERE
              `database` = COALESCE(:schemaName, currentDatabase()) AND
              `name` = :tableName
        SQL;

        $comment = $this->db->createCommand($sql, [
            ':schemaName' => $tableSchema->getSchemaName() ?: null,
            ':tableName' => $tableSchema->getName(),
        ])->queryScalar();

        $tableSchema->comment(is_string($comment) && $comment !== '' ? $comment : null);
    }

    protected function findTableNames(string $schema = ''): array
    {
        $sql = <<<SQL
        SELECT `name`
        FROM `system`.`tables`
        WHERE `database` = COALESCE(:schemaName, currentDatabase())
            AND `engine` NOT IN ('View', 'MaterializedView')
        ORDER BY `name`
        SQL;

        /** @var string[] */
        return $this->db->createCommand($sql, [
            ':schemaName' => $schema ?: null,
        ])->queryColumn();
    }

    protected function findViewNames(string $schema = ''): array
    {
        $sql = <<<SQL
        SELECT `name`
        FROM `system`.`tables`
        WHERE `database` = COALESCE(:schemaName, currentDatabase())
            AND `engine` IN ('View', 'MaterializedView')
        ORDER BY `name`
        SQL;

        /** @var string[] */
        return $this->db->createCommand($sql, [
            ':schemaName' => $schema ?: null,
        ])->queryColumn();
    }

    /**
     * Gets the `SHOW CREATE TABLE` SQL string.
     */
    protected function getCreateTableSql(TableSchemaInterface $table): string
    {
        $tableName = $table->getFullName();

        try {
            /** @psalm-var array<array-key, string> $row */
            $row = $this->db->createCommand(
                'SHOW CREATE TABLE ' . $this->db->getQuoter()->quoteTableName($tableName),
            )->queryOne();

            if (isset($row['statement'])) {
                $sql = $row['statement'];
            } else {
                $row = array_values($row);
                $sql = $row[0] ?? '';
            }
        } catch (Exception) {
            $sql = '';
        }

        return $sql;
    }

    /**
     * ClickHouse does not support CHECK constraints.
     *
     * @throws NotSupportedException
     */
    protected function loadTableChecks(string $tableName): array
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * ClickHouse does not support foreign keys.
     *
     * @throws NotSupportedException
     */
    protected function loadTableForeignKeys(string $tableName): array
    {
        return [];
    }

    /**
     * @throws NotSupportedException
     */
    protected function loadTableDefaultValues(string $tableName): array
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    protected function loadTableIndexes(string $tableName): array
    {
        $sql = <<<SQL
        SELECT
            `name`,
            `expr`,
            `type`
        FROM `system`.`data_skipping_indices`
        WHERE `database` = COALESCE(:schemaName, currentDatabase())
            AND `table` = :tableName
        SQL;

        $nameParts = $this->db->getQuoter()->getTableNameParts($tableName);
        $indexes = $this->db->createCommand($sql, [
            ':schemaName' => $nameParts['schemaName'] ?? null,
            ':tableName' => $nameParts['name'],
        ])->queryAll();

        $result = [];

        /** @psalm-var list<array{name: string, expr: string, type: string}> $indexes */
        foreach ($indexes as $index) {
            $result[$index['name']] = new Index(
                $index['name'],
                [$index['expr']],
                false,
                false,
            );
        }

        // Also get primary key columns from sorting key
        $sortingKeySql = <<<SQL
        SELECT `sorting_key`
        FROM `system`.`tables`
        WHERE `database` = COALESCE(:schemaName, currentDatabase())
            AND `name` = :tableName
        SQL;

        $sortingKey = $this->db->createCommand($sortingKeySql, [
            ':schemaName' => $nameParts['schemaName'] ?? null,
            ':tableName' => $nameParts['name'],
        ])->queryScalar();

        if (is_string($sortingKey) && $sortingKey !== '') {
            $sortingColumns = array_map('trim', explode(',', $sortingKey));
            $result['PRIMARY'] = new Index('PRIMARY', $sortingColumns, true, true);
        }

        return $result;
    }

    protected function loadTableSchema(string $name): ?TableSchemaInterface
    {
        $table = new TableSchema(...$this->db->getQuoter()->getTableNameParts($name));
        $this->resolveTableCreateSql($table);

        if ($this->findColumns($table)) {
            $this->findTableComment($table);
            $this->findConstraints($table);

            return $table;
        }

        return null;
    }

    protected function resolveTableCreateSql(TableSchemaInterface $table): void
    {
        $sql = $this->getCreateTableSql($table);
        $table->createSql($sql);
    }

    /**
     * Loads the column information into a {@see ColumnInterface} object.
     *
     * @psalm-param ColumnArray $info The column information.
     */
    private function loadColumn(array $info): ColumnInterface
    {
        $dbType = $info['type'];
        $notNull = true;

        // Handle Nullable type
        if (preg_match('/^Nullable\((.+)\)$/i', $dbType, $matches)) {
            $dbType = $matches[1];
            $notNull = false;
        }

        // Handle LowCardinality type
        if (preg_match('/^LowCardinality\((.+)\)$/i', $dbType, $matches)) {
            $dbType = $matches[1];

            // LowCardinality(Nullable(...))
            if (preg_match('/^Nullable\((.+)\)$/i', $dbType, $innerMatches)) {
                $dbType = $innerMatches[1];
                $notNull = false;
            }
        }

        $columnInfo = [
            'comment' => $info['comment'] === '' ? null : $info['comment'],
            'defaultValueRaw' => $info['default_expression'] === '' ? null : $info['default_expression'],
            'name' => $info['name'],
            'notNull' => $notNull,
            'primaryKey' => (bool) $info['is_in_primary_key'],
            'schema' => $info['database'],
            'source' => ColumnInfoSource::TABLE_SCHEMA,
            'table' => $info['table'],
        ];

        // Parse size from type definitions like FixedString(16), Decimal(10, 2)
        if (preg_match('/^(\w+)\((\d+)(?:,\s*(\d+))?\)$/', $dbType, $sizeMatches)) {
            $dbType = $sizeMatches[1];
            $columnInfo['size'] = (int) $sizeMatches[2];
            if (isset($sizeMatches[3])) {
                $columnInfo['scale'] = (int) $sizeMatches[3];
            }
        }

        // Parse Enum values
        if (preg_match('/^Enum(?:8|16)\((.+)\)$/i', $info['type'], $enumMatches)) {
            $dbType = 'Enum';
        }

        /** @psalm-suppress InvalidArgument */
        return $this->db->getColumnFactory()->fromDbType(strtolower($dbType), $columnInfo);
    }
}
