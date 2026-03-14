<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Schema\Quoter as BaseQuoter;

use function strtr;

/**
 * Implements ClickHouse quoting and unquoting methods.
 *
 * ClickHouse uses backticks (`) for identifier quoting, similar to MySQL.
 */
final class Quoter extends BaseQuoter
{
    public function quoteValue(string $value): string
    {
        return "'" . strtr($value, [
            '\\' => '\\\\',
            "'" => "\\'",
        ]) . "'";
    }
}
