<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Expression\ExpressionInterface;
use Pushax\Db\ClickHouse\Builder\LikeBuilder;
use Yiisoft\Db\QueryBuilder\AbstractDQLQueryBuilder;
use Yiisoft\Db\QueryBuilder\Condition\Like;
use Yiisoft\Db\QueryBuilder\Condition\NotLike;

use function ctype_digit;

/**
 * Implements a DQL (Data Query Language) SQL statements for ClickHouse.
 *
 * ClickHouse DQL specifics:
 * - LIMIT N, M syntax (or LIMIT N OFFSET M)
 * - Supports SAMPLE clause for approximate query processing
 * - Supports PREWHERE for filtering before reading data
 * - FINAL modifier for ReplacingMergeTree/CollapsingMergeTree
 * - Array functions (arrayJoin, arrayMap, etc.)
 * - Various aggregate functions (quantile, uniq, etc.)
 */
final class DQLQueryBuilder extends AbstractDQLQueryBuilder
{
    public function buildLimit(ExpressionInterface|int|null $limit, ExpressionInterface|int|null $offset): string
    {
        $sql = '';

        if ($this->hasLimit($limit)) {
            $sql = 'LIMIT ' . ($limit instanceof ExpressionInterface ? $this->buildExpression($limit) : (string) $limit);

            if ($this->hasOffset($offset)) {
                $sql .= ' OFFSET ' . ($offset instanceof ExpressionInterface ? $this->buildExpression($offset) : (string) $offset);
            }
        } elseif ($this->hasOffset($offset)) {
            // ClickHouse requires LIMIT when OFFSET is used
            $sql = 'LIMIT '
                . ($offset instanceof ExpressionInterface ? $this->buildExpression($offset) : (string) $offset)
                . ', 18446744073709551615';
        }

        return $sql;
    }

    /**
     * Checks to see if the given limit is effective.
     *
     * @param mixed $limit The given limit.
     *
     * @return bool Whether the limit is effective.
     */
    protected function hasLimit(mixed $limit): bool
    {
        return ctype_digit((string) $limit);
    }

    /**
     * Checks to see if the given offset is effective.
     *
     * @param mixed $offset The given offset.
     *
     * @return bool Whether the offset is effective.
     */
    protected function hasOffset(mixed $offset): bool
    {
        $offset = (string) $offset;
        return ctype_digit($offset) && $offset !== '0';
    }

    protected function defaultExpressionBuilders(): array
    {
        return [
            ...parent::defaultExpressionBuilders(),
            Like::class => LikeBuilder::class,
            NotLike::class => LikeBuilder::class,
        ];
    }
}
