<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Connection\ConnectionInterface;
use Pushax\Db\ClickHouse\Column\ColumnDefinitionBuilder;
use Yiisoft\Db\QueryBuilder\AbstractQueryBuilder;

/**
 * Implements ClickHouse specific query builder.
 */
final class QueryBuilder extends AbstractQueryBuilder
{
    public function __construct(ConnectionInterface $db)
    {
        $quoter = $db->getQuoter();
        $schema = $db->getSchema();

        parent::__construct(
            $db,
            new DDLQueryBuilder($this, $quoter, $schema),
            new DMLQueryBuilder($this, $quoter, $schema),
            new DQLQueryBuilder($this, $quoter),
            new ColumnDefinitionBuilder($this),
        );
    }

    protected function createSqlParser(string $sql): SqlParser
    {
        return new SqlParser($sql);
    }
}
