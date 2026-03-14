<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests\Integration;

use Pushax\Db\ClickHouse\TableBuilder;
use Pushax\Db\ClickHouse\TableEngine;

final class TableBuilderTest extends IntegrationTestCase
{
    public function testCreateMemoryTable(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        (new TableBuilder($this->db))
            ->table($table)
            ->column('id', 'UInt64')
            ->column('name', 'String')
            ->engine(TableEngine::MEMORY)
            ->execute();

        $schema = $this->db->getTableSchema($table);

        $this->assertNotNull($schema);
        $this->assertNotNull($schema->getColumn('id'));
        $this->assertNotNull($schema->getColumn('name'));
    }

    public function testCreateMergeTreeTable(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        (new TableBuilder($this->db))
            ->table($table)
            ->column('event_date', 'Date')
            ->column('user_id', 'UInt64')
            ->column('value', 'Float64')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy(['event_date', 'user_id'])
            ->partitionBy('toYYYYMM(event_date)')
            ->execute();

        $schema = $this->db->getTableSchema($table);

        $this->assertNotNull($schema);
        $this->assertNotNull($schema->getColumn('event_date'));
        $this->assertNotNull($schema->getColumn('user_id'));
        $this->assertNotNull($schema->getColumn('value'));
    }

    public function testIfNotExistsDoesNotThrow(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $builder = (new TableBuilder($this->db))
            ->table($table)
            ->column('id', 'UInt64')
            ->engine(TableEngine::MEMORY)
            ->ifNotExists();

        $builder->execute();
        $builder->execute(); // second call must not throw

        $this->assertNotNull($this->db->getTableSchema($table));
    }

    public function testCreateTableWithSettings(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        (new TableBuilder($this->db))
            ->table($table)
            ->column('ts', 'DateTime')
            ->column('val', 'UInt32')
            ->engine(TableEngine::MERGE_TREE)
            ->orderBy('ts')
            ->settings(['index_granularity' => 4096])
            ->execute();

        $this->assertNotNull($this->db->getTableSchema($table));
    }

    public function testCreateTableWithComment(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        (new TableBuilder($this->db))
            ->table($table)
            ->column('id', 'UInt64')
            ->engine(TableEngine::MEMORY)
            ->comment('Integration test table')
            ->execute();

        $this->db->getSchema()->refreshTableSchema($table);
        $schema = $this->db->getTableSchema($table);

        $this->assertNotNull($schema);
        $this->assertSame('Integration test table', $schema->getComment());
    }

    public function testBuildReturnsValidSql(): void
    {
        $sql = (new TableBuilder($this->db))
            ->table('dummy')
            ->column('id', 'UInt64')
            ->engine(TableEngine::MEMORY)
            ->build();

        $this->assertStringStartsWith('CREATE TABLE', $sql);
        $this->assertStringContainsString('ENGINE = Memory', $sql);
    }
}
