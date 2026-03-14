<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\QueryBuilder\AbstractDDLQueryBuilder;
use Yiisoft\Db\Schema\Column\ColumnInterface;

/**
 * Implements a (Data Definition Language) SQL statements for ClickHouse.
 *
 * ClickHouse DDL is different from traditional RDBMS:
 * - Tables require an ENGINE specification
 * - No ALTER COLUMN ... SET DATA TYPE (column type changes require recreating the table)
 * - No foreign keys
 * - No traditional primary keys (uses ORDER BY / PRIMARY KEY in table definition)
 * - Supports PARTITION BY, ORDER BY, SAMPLE BY clauses
 */
final class DDLQueryBuilder extends AbstractDDLQueryBuilder
{
    /**
     * @throws NotSupportedException
     */
    public function addCheck(string $table, string $name, string $expression): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    public function addCommentOnColumn(string $table, string $column, string $comment): string
    {
        return 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' COMMENT COLUMN '
            . $this->quoter->quoteColumnName($column)
            . ' '
            . $this->quoter->quoteValue($comment);
    }

    public function addCommentOnTable(string $table, string $comment): string
    {
        return 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' MODIFY COMMENT '
            . $this->quoter->quoteValue($comment);
    }

    /**
     * @throws NotSupportedException
     */
    public function addDefaultValue(string $table, string $name, string $column, mixed $value): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * @throws NotSupportedException
     */
    public function addForeignKey(
        string $table,
        string $name,
        array|string $columns,
        string $referenceTable,
        array|string $referenceColumns,
        ?string $delete = null,
        ?string $update = null,
    ): string {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * @throws NotSupportedException
     */
    public function addPrimaryKey(string $table, string $name, array|string $columns): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse. Use ORDER BY in CREATE TABLE.');
    }

    /**
     * @throws NotSupportedException
     */
    public function addUnique(string $table, string $name, array|string $columns): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    public function createIndex(
        string $table,
        string $name,
        array|string $columns,
        ?string $indexType = null,
        ?string $indexMethod = null,
    ): string {
        $sql = 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' ADD INDEX '
            . $this->quoter->quoteTableName($name)
            . ' (' . $this->queryBuilder->buildColumns($columns) . ')';

        if (!empty($indexType)) {
            $sql .= " TYPE $indexType";
        }

        if (!empty($indexMethod)) {
            $sql .= " GRANULARITY $indexMethod";
        }

        return $sql;
    }

    public function dropIndex(string $table, string $name): string
    {
        return 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' DROP INDEX '
            . $this->quoter->quoteTableName($name);
    }

    /**
     * @throws NotSupportedException
     */
    public function checkIntegrity(string $schema = '', string $table = '', bool $check = true): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * @throws NotSupportedException
     */
    public function dropCheck(string $table, string $name): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    public function dropCommentFromColumn(string $table, string $column): string
    {
        return $this->addCommentOnColumn($table, $column, '');
    }

    public function dropCommentFromTable(string $table): string
    {
        return $this->addCommentOnTable($table, '');
    }

    /**
     * @throws NotSupportedException
     */
    public function dropDefaultValue(string $table, string $name): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * @throws NotSupportedException
     */
    public function dropForeignKey(string $table, string $name): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * @throws NotSupportedException
     */
    public function dropPrimaryKey(string $table, string $name): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    /**
     * @throws NotSupportedException
     */
    public function dropUnique(string $table, string $name): string
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    public function alterColumn(string $table, string $column, ColumnInterface|string $type): string
    {
        return 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' MODIFY COLUMN '
            . $this->quoter->quoteColumnName($column)
            . ' '
            . $this->queryBuilder->buildColumnDefinition($type);
    }

    public function dropTable(string $table, bool $ifExists = false, bool $cascade = false): string
    {
        $sql = 'DROP TABLE';

        if ($ifExists) {
            $sql .= ' IF EXISTS';
        }

        $sql .= ' ' . $this->quoter->quoteTableName($table);

        return $sql;
    }

    public function renameColumn(string $table, string $oldName, string $newName): string
    {
        return 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' RENAME COLUMN '
            . $this->quoter->quoteColumnName($oldName)
            . ' TO '
            . $this->quoter->quoteColumnName($newName);
    }

    public function renameTable(string $oldName, string $newName): string
    {
        return 'RENAME TABLE '
            . $this->quoter->quoteTableName($oldName)
            . ' TO '
            . $this->quoter->quoteTableName($newName);
    }

    public function addColumn(string $table, string $column, ColumnInterface|string $type): string
    {
        return 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' ADD COLUMN '
            . $this->quoter->quoteColumnName($column)
            . ' '
            . $this->queryBuilder->buildColumnDefinition($type);
    }

    public function dropColumn(string $table, string $column): string
    {
        return 'ALTER TABLE '
            . $this->quoter->quoteTableName($table)
            . ' DROP COLUMN '
            . $this->quoter->quoteColumnName($column);
    }

    public function truncateTable(string $table): string
    {
        return 'TRUNCATE TABLE ' . $this->quoter->quoteTableName($table);
    }
}
