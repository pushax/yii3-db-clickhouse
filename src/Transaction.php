<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Transaction\TransactionInterface;

/**
 * Implements the ClickHouse specific transaction.
 *
 * ClickHouse has no support for ACID transactions. This class satisfies the interface
 * by throwing {@see NotSupportedException} on all transaction operations.
 */
final class Transaction implements TransactionInterface
{
    public function __construct(private readonly Connection $db) {}

    /**
     * @throws NotSupportedException
     */
    public function begin(?string $isolationLevel = null): void
    {
        throw new NotSupportedException('ClickHouse does not support transactions.');
    }

    /**
     * @throws NotSupportedException
     */
    public function commit(): void
    {
        throw new NotSupportedException('ClickHouse does not support transactions.');
    }

    /**
     * @throws NotSupportedException
     */
    public function rollBack(): void
    {
        throw new NotSupportedException('ClickHouse does not support transactions.');
    }

    public function getLevel(): int
    {
        return 0;
    }

    public function isActive(): bool
    {
        return false;
    }

    /**
     * @throws NotSupportedException
     */
    public function setIsolationLevel(string $level): void
    {
        throw new NotSupportedException('ClickHouse does not support transaction isolation levels.');
    }

    /**
     * @throws NotSupportedException
     */
    public function createSavepoint(string $name): void
    {
        throw new NotSupportedException('ClickHouse does not support savepoints.');
    }

    /**
     * @throws NotSupportedException
     */
    public function rollBackSavepoint(string $name): void
    {
        throw new NotSupportedException('ClickHouse does not support savepoints.');
    }

    /**
     * @throws NotSupportedException
     */
    public function releaseSavepoint(string $name): void
    {
        throw new NotSupportedException('ClickHouse does not support savepoints.');
    }
}
