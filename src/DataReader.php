<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Closure;
use Yiisoft\Db\Query\DataReaderInterface;
use Yiisoft\Db\Schema\Column\ColumnInterface;

/**
 * Implements a forward-only, array-backed data reader for ClickHouse HTTP query results.
 */
final class DataReader implements DataReaderInterface
{
    private int $index = 0;
    private Closure|string|null $indexBy = null;
    private ?Closure $resultCallback = null;
    /** @var ColumnInterface[] */
    private array $typecastColumns = [];

    public function __construct(private readonly array $data) {}

    public function current(): array|false
    {
        if (!isset($this->data[$this->index])) {
            return false;
        }

        $row = $this->data[$this->index];

        if ($this->resultCallback !== null) {
            $row = ($this->resultCallback)($row);
        }

        return $row;
    }

    public function key(): int|string|null
    {
        if (!isset($this->data[$this->index])) {
            return null;
        }

        if ($this->indexBy === null) {
            return $this->index;
        }

        $row = $this->data[$this->index];

        return is_string($this->indexBy)
            ? ($row[$this->indexBy] ?? $this->index)
            : ($this->indexBy)($row);
    }

    public function next(): void
    {
        $this->index++;
    }

    public function rewind(): void
    {
        $this->index = 0;
    }

    public function valid(): bool
    {
        return isset($this->data[$this->index]);
    }

    public function count(): int
    {
        return count($this->data);
    }

    public function indexBy(Closure|string|null $indexBy): static
    {
        $new = clone $this;
        $new->indexBy = $indexBy;
        return $new;
    }

    public function resultCallback(?Closure $resultCallback): static
    {
        $new = clone $this;
        $new->resultCallback = $resultCallback;
        return $new;
    }

    public function typecastColumns(array $typecastColumns): static
    {
        $new = clone $this;
        $new->typecastColumns = $typecastColumns;
        return $new;
    }
}
