<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Pushax\Db\ClickHouse\Column\ColumnBuilder;
use Pushax\Db\ClickHouse\Column\ColumnFactory;
use Yiisoft\Db\Cache\SchemaCache;
use Yiisoft\Db\Command\CommandInterface;
use Yiisoft\Db\Connection\AbstractConnection;
use Yiisoft\Db\Connection\ServerInfoInterface;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\QueryBuilder\QueryBuilderInterface;
use Yiisoft\Db\Schema\Column\ColumnFactoryInterface;
use Yiisoft\Db\Schema\QuoterInterface;
use Yiisoft\Db\Schema\SchemaInterface;
use Yiisoft\Db\Transaction\TransactionInterface;

/**
 * Implements a connection to a ClickHouse database via HTTP interface.
 *
 * @link https://clickhouse.com/docs/en/interfaces/http
 */
final class Connection extends AbstractConnection
{
    private bool $active = false;
    private ?QueryBuilderInterface $queryBuilder = null;
    private ?QuoterInterface $quoter = null;
    private ?SchemaInterface $schema = null;
    private ?ServerInfoInterface $serverInfo = null;
    private ?ColumnFactoryInterface $columnFactory = null;

    public function __construct(
        private readonly Driver $driver,
        private readonly SchemaCache $schemaCache,
    ) {}

    public function open(): void
    {
        if ($this->active) {
            return;
        }

        $this->active = true;
    }

    public function close(): void
    {
        $this->active = false;
        $this->transaction = null;
    }

    public function isActive(): bool
    {
        return $this->active;
    }

    public function createCommand(?string $sql = null, array $params = []): CommandInterface
    {
        $command = new Command($this);

        if ($sql !== null) {
            $command->setSql($sql);
        }

        return $command->bindValues($params);
    }

    public function createTransaction(): TransactionInterface
    {
        return new Transaction($this);
    }

    public function getDriver(): Driver
    {
        return $this->driver;
    }

    public function getColumnBuilderClass(): string
    {
        return ColumnBuilder::class;
    }

    public function getColumnFactory(): ColumnFactoryInterface
    {
        return $this->columnFactory ??= new ColumnFactory();
    }

    public function getQueryBuilder(): QueryBuilderInterface
    {
        return $this->queryBuilder ??= new QueryBuilder($this);
    }

    public function getQuoter(): QuoterInterface
    {
        return $this->quoter ??= new Quoter('`', '`', $this->getTablePrefix());
    }

    public function getSchema(): SchemaInterface
    {
        return $this->schema ??= new Schema($this, $this->schemaCache);
    }

    public function getServerInfo(): ServerInfoInterface
    {
        return $this->serverInfo ??= new ServerInfo($this);
    }

    public function getLastInsertId(?string $sequenceName = null): string
    {
        throw new NotSupportedException('ClickHouse does not support getLastInsertId.');
    }

    public function quoteValue(mixed $value): string
    {
        return $this->getQuoter()->quoteValue((string) $value);
    }

    public function getDriverName(): string
    {
        return 'clickhouse';
    }
}
