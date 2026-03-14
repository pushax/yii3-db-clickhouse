<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Column;

use Yiisoft\Db\Constant\ColumnType;
use Yiisoft\Db\Schema\Column\BigIntColumn;
use Yiisoft\Db\Schema\Column\BooleanColumn;
use Yiisoft\Db\Schema\Column\DoubleColumn;
use Yiisoft\Db\Schema\Column\IntegerColumn;
use Yiisoft\Db\Schema\Column\StringColumn;

/**
 * Provides methods for creating column schema instances for ClickHouse.
 *
 * Includes ClickHouse-specific column types like UInt*, Int*, Float32, Float64, etc.
 */
final class ColumnBuilder extends \Yiisoft\Db\Schema\Column\ColumnBuilder
{
    public static function timestamp(?int $size = 0): DateTimeColumn
    {
        return new DateTimeColumn(ColumnType::TIMESTAMP, size: $size);
    }

    public static function datetime(?int $size = 0): DateTimeColumn
    {
        return new DateTimeColumn(ColumnType::DATETIME, size: $size);
    }

    public static function datetimeWithTimezone(?int $size = 0): DateTimeColumn
    {
        return new DateTimeColumn(ColumnType::DATETIMETZ, size: $size);
    }

    public static function time(?int $size = 0): DateTimeColumn
    {
        return new DateTimeColumn(ColumnType::TIME, size: $size);
    }

    public static function timeWithTimezone(?int $size = 0): DateTimeColumn
    {
        return new DateTimeColumn(ColumnType::TIMETZ, size: $size);
    }

    public static function date(): DateTimeColumn
    {
        return new DateTimeColumn(ColumnType::DATE);
    }

    public static function uuid(): StringColumn
    {
        return new StringColumn(ColumnType::UUID);
    }

    // ClickHouse-specific column types

    public static function int8(): IntegerColumn
    {
        $column = new IntegerColumn(ColumnType::TINYINT);
        $column->dbType('Int8');
        return $column;
    }

    public static function int16(): IntegerColumn
    {
        $column = new IntegerColumn(ColumnType::SMALLINT);
        $column->dbType('Int16');
        return $column;
    }

    public static function int32(): IntegerColumn
    {
        $column = new IntegerColumn(ColumnType::INTEGER);
        $column->dbType('Int32');
        return $column;
    }

    public static function int64(): BigIntColumn
    {
        $column = new BigIntColumn(ColumnType::BIGINT);
        $column->dbType('Int64');
        return $column;
    }

    public static function uint8(): IntegerColumn
    {
        $column = new IntegerColumn(ColumnType::TINYINT);
        $column->dbType('UInt8');
        $column->unsigned();
        return $column;
    }

    public static function uint16(): IntegerColumn
    {
        $column = new IntegerColumn(ColumnType::SMALLINT);
        $column->dbType('UInt16');
        $column->unsigned();
        return $column;
    }

    public static function uint32(): IntegerColumn
    {
        $column = new IntegerColumn(ColumnType::INTEGER);
        $column->dbType('UInt32');
        $column->unsigned();
        return $column;
    }

    public static function uint64(): BigIntColumn
    {
        $column = new BigIntColumn(ColumnType::BIGINT);
        $column->dbType('UInt64');
        $column->unsigned();
        return $column;
    }

    public static function float32(): DoubleColumn
    {
        $column = new DoubleColumn(ColumnType::FLOAT);
        $column->dbType('Float32');
        return $column;
    }

    public static function float64(): DoubleColumn
    {
        $column = new DoubleColumn(ColumnType::DOUBLE);
        $column->dbType('Float64');
        return $column;
    }

    /**
     * Creates a DateTime64 column with specified precision.
     */
    public static function datetime64(int $precision = 3): DateTimeColumn
    {
        $column = new DateTimeColumn(ColumnType::DATETIME, size: $precision);
        $column->dbType("DateTime64($precision)");
        return $column;
    }

    /**
     * Creates a Date32 column (extended date range).
     */
    public static function date32(): DateTimeColumn
    {
        $column = new DateTimeColumn(ColumnType::DATE);
        $column->dbType('Date32');
        return $column;
    }

    /**
     * Creates an IPv4 column.
     */
    public static function ipv4(): StringColumn
    {
        $column = new StringColumn(ColumnType::STRING);
        $column->dbType('IPv4');
        return $column;
    }

    /**
     * Creates an IPv6 column.
     */
    public static function ipv6(): StringColumn
    {
        $column = new StringColumn(ColumnType::STRING);
        $column->dbType('IPv6');
        return $column;
    }

    /**
     * Creates a LowCardinality(String) column.
     */
    public static function lowCardinalityString(): StringColumn
    {
        $column = new StringColumn(ColumnType::STRING);
        $column->dbType('LowCardinality(String)');
        return $column;
    }

    /**
     * Creates a FixedString column.
     */
    public static function fixedString(int $n): StringColumn
    {
        $column = new StringColumn(ColumnType::CHAR, size: $n);
        $column->dbType("FixedString($n)");
        return $column;
    }
}
