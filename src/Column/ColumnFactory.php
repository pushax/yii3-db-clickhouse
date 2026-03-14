<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Column;

use Yiisoft\Db\Constant\ColumnType;
use Yiisoft\Db\Expression\Expression;
use Yiisoft\Db\Schema\Column\AbstractColumnFactory;
use Yiisoft\Db\Schema\Column\ColumnInterface;

use function str_starts_with;
use function strtolower;
use function substr;

/**
 * Column factory for ClickHouse database.
 *
 * Maps ClickHouse data types to abstract column types.
 *
 * @link https://clickhouse.com/docs/en/sql-reference/data-types
 */
final class ColumnFactory extends AbstractColumnFactory
{
    /**
     * Mapping from physical column types (keys) to abstract column types (values).
     *
     * @var string[]
     * @psalm-var array<string, ColumnType::*>
     */
    protected const TYPE_MAP = [
        // Integer types
        'int8' => ColumnType::TINYINT,
        'int16' => ColumnType::SMALLINT,
        'int32' => ColumnType::INTEGER,
        'int64' => ColumnType::BIGINT,
        'int128' => ColumnType::BIGINT,
        'int256' => ColumnType::BIGINT,
        'uint8' => ColumnType::TINYINT,
        'uint16' => ColumnType::SMALLINT,
        'uint32' => ColumnType::INTEGER,
        'uint64' => ColumnType::BIGINT,
        'uint128' => ColumnType::BIGINT,
        'uint256' => ColumnType::BIGINT,

        // Float types
        'float32' => ColumnType::FLOAT,
        'float64' => ColumnType::DOUBLE,

        // Decimal types
        'decimal' => ColumnType::DECIMAL,
        'decimal32' => ColumnType::DECIMAL,
        'decimal64' => ColumnType::DECIMAL,
        'decimal128' => ColumnType::DECIMAL,
        'decimal256' => ColumnType::DECIMAL,

        // Boolean
        'bool' => ColumnType::BOOLEAN,
        'boolean' => ColumnType::BOOLEAN,

        // String types
        'string' => ColumnType::TEXT,
        'fixedstring' => ColumnType::CHAR,

        // Date and time types
        'date' => ColumnType::DATE,
        'date32' => ColumnType::DATE,
        'datetime' => ColumnType::DATETIME,
        'datetime64' => ColumnType::DATETIME,

        // UUID
        'uuid' => ColumnType::UUID,

        // Enum types
        'enum' => ColumnType::STRING,
        'enum8' => ColumnType::STRING,
        'enum16' => ColumnType::STRING,

        // IP address types (stored as strings in abstraction)
        'ipv4' => ColumnType::STRING,
        'ipv6' => ColumnType::STRING,

        // JSON
        'json' => ColumnType::JSON,
        'object' => ColumnType::JSON,

        // Array (represented as JSON in abstraction)
        'array' => ColumnType::ARRAY,

        // Tuple (represented as structured in abstraction)
        'tuple' => ColumnType::STRUCTURED,

        // Map (represented as JSON in abstraction)
        'map' => ColumnType::JSON,

        // Nested (represented as structured in abstraction)
        'nested' => ColumnType::STRUCTURED,

        // Geo types (stored as strings in abstraction)
        'point' => ColumnType::STRING,
        'ring' => ColumnType::STRING,
        'polygon' => ColumnType::STRING,
        'multipolygon' => ColumnType::STRING,

        // Nothing type
        'nothing' => ColumnType::STRING,

        // SimpleAggregateFunction / AggregateFunction
        'simpleaggregatefunction' => ColumnType::STRING,
        'aggregatefunction' => ColumnType::BINARY,
    ];

    protected function columnDefinitionParser(): ColumnDefinitionParser
    {
        return new ColumnDefinitionParser();
    }

    protected function getColumnClass(string $type, array $info = []): string
    {
        return match ($type) {
            ColumnType::TIMESTAMP => DateTimeColumn::class,
            ColumnType::DATETIME => DateTimeColumn::class,
            ColumnType::DATETIMETZ => DateTimeColumn::class,
            ColumnType::TIME => DateTimeColumn::class,
            ColumnType::TIMETZ => DateTimeColumn::class,
            ColumnType::DATE => DateTimeColumn::class,
            default => parent::getColumnClass($type, $info),
        };
    }

    protected function normalizeDefaultValue(?string $defaultValue, ColumnInterface $column): mixed
    {
        if (
            $defaultValue === null
            || $column->isPrimaryKey()
            || $column->isComputed()
        ) {
            return null;
        }

        return $this->normalizeNotNullDefaultValue($defaultValue, $column);
    }

    protected function normalizeNotNullDefaultValue(string $defaultValue, ColumnInterface $column): mixed
    {
        if ($defaultValue === '') {
            return $column->phpTypecast($defaultValue);
        }

        if (strtolower($defaultValue) === 'now()') {
            return new Expression('now()');
        }

        if (strtolower($defaultValue) === 'today()') {
            return new Expression('today()');
        }

        if (strtolower($defaultValue) === 'generateuuidv4()') {
            return new Expression('generateUUIDv4()');
        }

        if ($defaultValue[0] === "'" && $defaultValue[-1] === "'") {
            $value = substr($defaultValue, 1, -1);
            $value = str_replace("''", "'", $value);
            return $column->phpTypecast($value);
        }

        return $column->phpTypecast($defaultValue);
    }
}
