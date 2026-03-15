<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

/**
 * Constants for ClickHouse column type names.
 *
 * Use these instead of plain strings when defining table columns to avoid typos
 * and improve IDE support.
 *
 * @link https://clickhouse.com/docs/en/sql-reference/data-types
 */
final class ClickHouseDataType
{
    // Integer types
    public const INT8    = 'Int8';
    public const INT16   = 'Int16';
    public const INT32   = 'Int32';
    public const INT64   = 'Int64';
    public const INT128  = 'Int128';
    public const INT256  = 'Int256';
    public const UINT8   = 'UInt8';
    public const UINT16  = 'UInt16';
    public const UINT32  = 'UInt32';
    public const UINT64  = 'UInt64';
    public const UINT128 = 'UInt128';
    public const UINT256 = 'UInt256';

    // Float types
    public const FLOAT32 = 'Float32';
    public const FLOAT64 = 'Float64';

    // Decimal types
    public const DECIMAL    = 'Decimal';
    public const DECIMAL32  = 'Decimal32';
    public const DECIMAL64  = 'Decimal64';
    public const DECIMAL128 = 'Decimal128';
    public const DECIMAL256 = 'Decimal256';

    // Boolean
    public const BOOL = 'Bool';

    // String types
    public const STRING       = 'String';
    public const FIXED_STRING = 'FixedString';

    // Date and time types
    public const DATE        = 'Date';
    public const DATE32      = 'Date32';
    public const DATETIME    = 'DateTime';
    public const DATETIME64  = 'DateTime64';

    // UUID
    public const UUID = 'UUID';

    // Enum types
    public const ENUM8  = 'Enum8';
    public const ENUM16 = 'Enum16';

    // IP address types
    public const IPV4 = 'IPv4';
    public const IPV6 = 'IPv6';

    // JSON / semi-structured
    public const JSON = 'JSON';

    // Composite types (used as wrappers — combine with a subtype string)
    public const ARRAY          = 'Array';
    public const TUPLE          = 'Tuple';
    public const MAP            = 'Map';
    public const NESTED         = 'Nested';
    public const NULLABLE       = 'Nullable';
    public const LOW_CARDINALITY = 'LowCardinality';

    /** Returns `Nullable(<type>)`. */
    public static function nullable(string $type): string
    {
        return "Nullable($type)";
    }

    /** Returns `LowCardinality(<type>)`. */
    public static function lowCardinality(string $type): string
    {
        return "LowCardinality($type)";
    }

    /** Returns `Array(<type>)`. */
    public static function array(string $type): string
    {
        return "Array($type)";
    }

    /** Returns `FixedString(<n>)`. */
    public static function fixedString(int $n): string
    {
        return "FixedString($n)";
    }

    /** Returns `Decimal(<precision>, <scale>)`. */
    public static function decimal(int $precision, int $scale): string
    {
        return "Decimal($precision, $scale)";
    }

    /** Returns `DateTime64(<precision>)` or `DateTime64(<precision>, '<timezone>')`. */
    public static function dateTime64(int $precision, ?string $timezone = null): string
    {
        return $timezone !== null
            ? "DateTime64($precision, '$timezone')"
            : "DateTime64($precision)";
    }

    /** Returns `Enum8('label1' = 1, 'label2' = 2, ...)`. */
    public static function enum8(array $values): string
    {
        return 'Enum8(' . self::buildEnumValues($values) . ')';
    }

    /** Returns `Enum16('label1' = 1, 'label2' = 2, ...)`. */
    public static function enum16(array $values): string
    {
        return 'Enum16(' . self::buildEnumValues($values) . ')';
    }

    /** @param array<string,int> $values */
    private static function buildEnumValues(array $values): string
    {
        $parts = [];
        foreach ($values as $label => $value) {
            $parts[] = "'$label' = $value";
        }
        return implode(', ', $parts);
    }
}
