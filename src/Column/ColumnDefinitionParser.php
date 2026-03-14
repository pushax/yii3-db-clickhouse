<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Column;

/**
 * Parses column definition string for ClickHouse.
 *
 * Handles ClickHouse-specific type wrappers:
 * - Nullable(T)
 * - LowCardinality(T)
 * - Array(T)
 * - Tuple(T1, T2, ...)
 * - Map(K, V)
 */
final class ColumnDefinitionParser extends \Yiisoft\Db\Syntax\ColumnDefinitionParser
{
}
