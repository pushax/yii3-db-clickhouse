<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Syntax\AbstractSqlParser;

/**
 * Implements ClickHouse specific SQL parsing.
 *
 * ClickHouse SQL syntax is similar to MySQL but with some differences:
 * - Uses backticks or double quotes for identifiers
 * - Supports `--` and `/* ... *​/` comments
 */
final class SqlParser extends AbstractSqlParser
{
    public function getNextPlaceholder(?int &$position = null): ?string
    {
        $result = null;
        $length = $this->length - 1;

        while ($this->position < $length) {
            $pos = $this->position++;

            match ($this->sql[$pos]) {
                ':' => ($word = $this->parseWord()) === ''
                    ? $this->skipChars(':')
                    : $result = ':' . $word,
                '"', "'", '`' => $this->skipQuotedWithEscape($this->sql[$pos]),
                '-' => $this->sql[$this->position] === '-'
                    ? ++$this->position && $this->skipToAfterChar("\n")
                    : null,
                '/' => $this->sql[$this->position] === '*'
                    ? ++$this->position && $this->skipToAfterString('*/')
                    : null,
                default => null,
            };

            if ($result !== null) {
                $position = $pos;

                return $result;
            }
        }

        return null;
    }
}
