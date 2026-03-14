<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Column;

use Yiisoft\Db\Constant\ColumnType;

/**
 * Represents the metadata for a datetime column in ClickHouse.
 *
 * ClickHouse datetime specifics:
 * - Date: stores date without time (YYYY-MM-DD)
 * - Date32: extended date range
 * - DateTime: stores datetime with seconds precision
 * - DateTime64: stores datetime with sub-second precision
 *
 * ClickHouse stores DateTime in UTC internally and converts based on server/session timezone.
 */
final class DateTimeColumn extends \Yiisoft\Db\Schema\Column\DateTimeColumn
{
    protected function getFormat(): string
    {
        return $this->format ??= match ($this->getType()) {
            ColumnType::DATETIMETZ => 'Y-m-d H:i:s' . $this->getMillisecondsFormat(),
            ColumnType::TIMETZ => 'H:i:s' . $this->getMillisecondsFormat(),
            default => parent::getFormat(),
        };
    }

    protected function shouldConvertTimezone(): bool
    {
        return $this->shouldConvertTimezone ??= !empty($this->dbTimezone) && $this->getType() !== ColumnType::DATE;
    }
}
