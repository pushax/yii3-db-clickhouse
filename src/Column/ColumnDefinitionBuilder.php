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

    /**
     * Maps lowercase type names (as stored by the parser) to proper ClickHouse type names.
     * The parser calls strtolower() on all types, but ClickHouse type names are case-sensitive.
     */
    private const CH_TYPE_CASE = [
        'uint8'    => 'UInt8',   'uint16'   => 'UInt16',  'uint32'  => 'UInt32',  'uint64'  => 'UInt64',
        'uint128'  => 'UInt128', 'uint256'  => 'UInt256',
        'int8'     => 'Int8',    'int16'    => 'Int16',   'int32'   => 'Int32',   'int64'   => 'Int64',
        'int128'   => 'Int128',  'int256'   => 'Int256',
        'float32'  => 'Float32', 'float64'  => 'Float64',
        'decimal'  => 'Decimal', 'decimal32' => 'Decimal32', 'decimal64' => 'Decimal64',
        'decimal128' => 'Decimal128', 'decimal256' => 'Decimal256',
        'bool'     => 'Bool',    'boolean'  => 'Bool',
        'string'   => 'String',  'fixedstring' => 'FixedString',
        'date'     => 'Date',    'date32'   => 'Date32',
        'datetime' => 'DateTime', 'datetime64' => 'DateTime64',
        'uuid'     => 'UUID',
        'enum8'    => 'Enum8',   'enum16'   => 'Enum16',
        'ipv4'     => 'IPv4',    'ipv6'     => 'IPv6',
        'json'     => 'JSON',
        'array'    => 'Array',   'tuple'    => 'Tuple',   'map'     => 'Map',
        'nested'   => 'Nested',
        'lowcardinality' => 'LowCardinality',
        'nullable'       => 'Nullable',
        'simpleaggregatefunction' => 'SimpleAggregateFunction',
        'aggregatefunction'       => 'AggregateFunction',
        'nothing'  => 'Nothing',
        'point'    => 'Point',   'ring'     => 'Ring',    'polygon' => 'Polygon',
        'multipolygon' => 'MultiPolygon',
    ];

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

        // The parser lowercases all type names, but ClickHouse types are case-sensitive.
        // Restore proper casing when the dbType came from a parsed definition.
        $dbType = $column->getDbType();
        if ($dbType !== null) {
            return self::CH_TYPE_CASE[strtolower($dbType)] ?? $dbType;
        }

        /** @psalm-suppress DocblockTypeContradiction */
        return match ($column->getType()) {
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
