<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Column;

use Yiisoft\Db\Constant\ColumnType;
use Yiisoft\Db\QueryBuilder\AbstractColumnDefinitionBuilder;
use Yiisoft\Db\Schema\Column\ColumnInterface;

use function in_array;
use function strtolower;

/**
 * Builds column definitions for ClickHouse.
 *
 * ClickHouse column definitions differ from traditional RDBMS:
 * - Uses Nullable() wrapper for nullable columns
 * - No AUTO_INCREMENT (auto-increment is not a concept in ClickHouse)
 * - No UNSIGNED keyword (uses UInt* types instead)
 * - No PRIMARY KEY per column (primary key is defined at table level via ORDER BY)
 *
 * @link https://clickhouse.com/docs/en/sql-reference/data-types
 */
final class ColumnDefinitionBuilder extends AbstractColumnDefinitionBuilder
{
    protected const AUTO_INCREMENT_KEYWORD = '';

    protected const TYPES_WITH_SIZE = [
        'fixedstring',
        'decimal',
        'decimal32',
        'decimal64',
        'decimal128',
        'decimal256',
        'datetime64',
    ];

    protected const TYPES_WITH_SCALE = [
        'decimal',
        'decimal32',
        'decimal64',
        'decimal128',
        'decimal256',
    ];

    public function build(ColumnInterface $column): string
    {
        $type = $this->buildType($column);

        // Wrap with Nullable() if column allows NULL
        if ($column->isNotNull() === false) {
            $type = "Nullable($type)";
        }

        $definition = $type
            . $this->buildDefault($column)
            . $this->buildComment($column)
            . $this->buildExtra($column);

        return $definition;
    }

    public function buildType(ColumnInterface $column): string
    {
        $dbType = $this->getDbType($column);

        if (empty($dbType)
            || $dbType[-1] === ')'
            || !in_array(strtolower($dbType), static::TYPES_WITH_SIZE, true)
        ) {
            return $dbType;
        }

        $size = $column->getSize();

        if ($size === null) {
            return $dbType;
        }

        $scale = $column->getScale();

        if ($scale === null || !in_array(strtolower($dbType), static::TYPES_WITH_SCALE, true)) {
            return "$dbType($size)";
        }

        return "$dbType($size,$scale)";
    }

    protected function buildComment(ColumnInterface $column): string
    {
        $comment = $column->getComment();

        return $comment === null ? '' : ' COMMENT ' . $this->queryBuilder->getQuoter()->quoteValue($comment);
    }

    /**
     * ClickHouse does not support UNSIGNED keyword - it uses separate unsigned types (UInt8, UInt16, etc.)
     */
    protected function buildUnsigned(ColumnInterface $column): string
    {
        return '';
    }

    /**
     * ClickHouse does not support PRIMARY KEY per column.
     */
    protected function buildPrimaryKey(ColumnInterface $column): string
    {
        return '';
    }

    /**
     * ClickHouse does not support UNIQUE constraints.
     */
    protected function buildUnique(ColumnInterface $column): string
    {
        return '';
    }

    /**
     * ClickHouse does not support column-level CHECK constraints.
     */
    protected function buildCheck(ColumnInterface $column): string
    {
        return '';
    }

    /**
     * ClickHouse does not support REFERENCES.
     */
    protected function buildReferences(ColumnInterface $column): string
    {
        return '';
    }

    /**
     * ClickHouse does not support NOT NULL keyword in column definition.
     * Use Nullable() wrapper instead.
     */
    protected function buildNotNull(ColumnInterface $column): string
    {
        return '';
    }

    protected function getDbType(ColumnInterface $column): string
    {
        $isUnsigned = $column->isUnsigned();

        /** @psalm-suppress DocblockTypeContradiction */
        return $column->getDbType() ?? match ($column->getType()) {
            ColumnType::BOOLEAN => 'Bool',
            ColumnType::BIT => 'UInt64',
            ColumnType::TINYINT => $isUnsigned ? 'UInt8' : 'Int8',
            ColumnType::SMALLINT => $isUnsigned ? 'UInt16' : 'Int16',
            ColumnType::INTEGER => $isUnsigned ? 'UInt32' : 'Int32',
            ColumnType::BIGINT => $isUnsigned ? 'UInt64' : 'Int64',
            ColumnType::FLOAT => 'Float32',
            ColumnType::DOUBLE => 'Float64',
            ColumnType::DECIMAL => 'Decimal',
            ColumnType::MONEY => 'Decimal(18,4)',
            ColumnType::CHAR => 'FixedString(' . ($column->getSize() ?? 1) . ')',
            ColumnType::STRING => 'String',
            ColumnType::TEXT => 'String',
            ColumnType::BINARY => 'String',
            ColumnType::UUID => 'UUID',
            ColumnType::TIMESTAMP => 'DateTime',
            ColumnType::DATETIME => 'DateTime',
            ColumnType::DATETIMETZ => 'DateTime',
            ColumnType::TIME => 'String',
            ColumnType::TIMETZ => 'String',
            ColumnType::DATE => 'Date',
            ColumnType::ARRAY => 'Array(String)',
            ColumnType::STRUCTURED => 'String',
            ColumnType::JSON => 'String',
            ColumnType::ENUM => 'String',
            default => 'String',
        };
    }

    protected function getDefaultUuidExpression(): string
    {
        return 'generateUUIDv4()';
    }
}
