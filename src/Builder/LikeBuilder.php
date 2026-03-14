<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Builder;

use Yiisoft\Db\QueryBuilder\Condition\Like;
use Yiisoft\Db\QueryBuilder\Condition\NotLike;

/**
 * Build an object of {@see Like} into SQL expressions for ClickHouse.
 *
 * ClickHouse supports LIKE and ILIKE (case-insensitive LIKE).
 */
final class LikeBuilder extends \Yiisoft\Db\QueryBuilder\Condition\Builder\LikeBuilder
{
    protected function prepareColumn(Like|NotLike $condition, array &$params): string
    {
        return parent::prepareColumn($condition, $params);
    }
}
