<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests\Integration;

use Pushax\Db\ClickHouse\ClickHouseDataType;
use Pushax\Db\ClickHouse\Migration\ClickHouseMigrationBuilder;
use Pushax\Db\ClickHouse\TableEngine;
use Yiisoft\Db\Migration\Informer\NullMigrationInformer;

final class MigrationBuilderTest extends IntegrationTestCase
{
    private ClickHouseMigrationBuilder $b;

    protected function setUp(): void
    {
        parent::setUp();
        $this->b = new ClickHouseMigrationBuilder($this->db, new NullMigrationInformer());
    }

    public function testCreateMergeTreeTable(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->b->createMergeTreeTable(
            $table,
            [
                'event_date' => ClickHouseDataType::DATE,
                'event_type' => ClickHouseDataType::STRING,
                'user_id'    => ClickHouseDataType::UINT64,
                'value'      => ClickHouseDataType::FLOAT64,
            ],
            orderBy: ['event_date', 'user_id'],
            partitionBy: 'toYYYYMM(event_date)',
        );

        $schema = $this->db->getTableSchema($table);
        $this->assertNotNull($schema);
        $this->assertNotNull($schema->getColumn('event_date'));
        $this->assertNotNull($schema->getColumn('event_type'));
        $this->assertNotNull($schema->getColumn('user_id'));
        $this->assertNotNull($schema->getColumn('value'));
    }

    public function testCreateMergeTreeTableWithSettings(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->b->createMergeTreeTable(
            $table,
            [
                'ts'  => ClickHouseDataType::DATETIME,
                'val' => ClickHouseDataType::UINT32,
            ],
            orderBy: 'ts',
            settings: ['index_granularity' => 4096],
        );

        $this->assertNotNull($this->db->getTableSchema($table));
    }

    public function testCreateMergeTreeTableWithComment(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->b->createMergeTreeTable(
            $table,
            ['id' => ClickHouseDataType::UINT64, 'ts' => ClickHouseDataType::DATE],
            orderBy: ['ts', 'id'],
            comment: 'Migration test table',
        );

        $this->db->getSchema()->refreshTableSchema($table);
        $schema = $this->db->getTableSchema($table);
        $this->assertNotNull($schema);
        $this->assertSame('Migration test table', $schema->getComment());
    }

    public function testCreateMaterializedView(): void
    {
        $source = $this->tmpTable('src');
        $target = $this->tmpTable('tgt');
        $view   = $this->tmpTable('mv');
        $this->dropAfterTest($source);
        $this->dropAfterTest($target);
        $this->dropAfterTest($view);

        // source table
        $this->b->createMergeTreeTable(
            $source,
            ['ts' => ClickHouseDataType::DATE, 'val' => ClickHouseDataType::UINT64],
            orderBy: 'ts',
        );

        // target table for the view
        $this->b->createMergeTreeTable(
            $target,
            ['ts' => ClickHouseDataType::DATE, 'val' => ClickHouseDataType::UINT64],
            orderBy: 'ts',
        );

        $this->b->createMaterializedView($view, $target, "SELECT ts, val FROM `$source`");

        // verify view exists via SHOW TABLES
        $tables = $this->db->createCommand('SHOW TABLES')->queryColumn();
        $this->assertContains($view, $tables);
    }

    public function testDropMaterializedView(): void
    {
        $source = $this->tmpTable('src');
        $target = $this->tmpTable('tgt');
        $view   = $this->tmpTable('mv');
        $this->dropAfterTest($source);
        $this->dropAfterTest($target);

        $this->b->createMergeTreeTable(
            $source,
            ['ts' => ClickHouseDataType::DATE, 'val' => ClickHouseDataType::UINT64],
            orderBy: 'ts',
        );
        $this->b->createMergeTreeTable(
            $target,
            ['ts' => ClickHouseDataType::DATE, 'val' => ClickHouseDataType::UINT64],
            orderBy: 'ts',
        );

        $this->b->createMaterializedView($view, $target, "SELECT ts, val FROM `$source`");
        $this->b->dropMaterializedView($view);

        $tables = $this->db->createCommand('SHOW TABLES')->queryColumn();
        $this->assertNotContains($view, $tables);
    }

    public function testAddAndDropSkippingIndex(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->b->createMergeTreeTable(
            $table,
            ['ts' => ClickHouseDataType::DATE, 'user_id' => ClickHouseDataType::UINT64],
            orderBy: ['ts', 'user_id'],
        );

        // Should not throw
        $this->b->addSkippingIndex($table, 'idx_user', 'user_id', 'minmax', 3);
        $this->b->dropSkippingIndex($table, 'idx_user');

        $this->assertNotNull($this->db->getTableSchema($table));
    }

    public function testModifyOrderBy(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->b->createMergeTreeTable(
            $table,
            ['ts' => ClickHouseDataType::DATE, 'user_id' => ClickHouseDataType::UINT64],
            orderBy: ['ts', 'user_id'],
        );

        // ClickHouse only allows adding ORDER BY columns in the same ALTER TABLE that adds the column.
        // Re-specifying the same ORDER BY is always valid and confirms the method works.
        $this->b->modifyOrderBy($table, ['ts', 'user_id']);

        $this->assertNotNull($this->db->getTableSchema($table));
    }

    public function testModifyTtlAndRemoveTtl(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->b->createMergeTreeTable(
            $table,
            ['ts' => ClickHouseDataType::DATE, 'val' => ClickHouseDataType::UINT32],
            orderBy: 'ts',
        );

        $this->b->modifyTtl($table, 'ts + INTERVAL 1 YEAR');
        $this->b->removeTtl($table);

        $this->assertNotNull($this->db->getTableSchema($table));
    }

    public function testModifySettings(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->b->createMergeTreeTable(
            $table,
            ['ts' => ClickHouseDataType::DATE, 'val' => ClickHouseDataType::UINT32],
            orderBy: 'ts',
        );

        // index_granularity is readonly after creation; use a writable setting instead
        $this->b->modifySettings($table, ['merge_with_ttl_timeout' => 3600]);

        $this->assertNotNull($this->db->getTableSchema($table));
    }

    public function testDelegatedDropTable(): void
    {
        $table = $this->tmpTable();

        $this->b->createMergeTreeTable(
            $table,
            ['id' => ClickHouseDataType::UINT64, 'ts' => ClickHouseDataType::DATE],
            orderBy: ['ts', 'id'],
        );

        $this->assertNotNull($this->db->getTableSchema($table));

        $this->b->dropTable($table);

        $this->db->getSchema()->refreshTableSchema($table);
        $this->assertNull($this->db->getTableSchema($table));
    }

    public function testDelegatedExecute(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->b->execute(
            "CREATE TABLE `$table` (id UInt64, name String) ENGINE = Memory"
        );

        $this->assertNotNull($this->db->getTableSchema($table));
    }
}
