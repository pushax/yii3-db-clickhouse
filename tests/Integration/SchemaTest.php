<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests\Integration;

final class SchemaTest extends IntegrationTestCase
{
    public function testGetTableNames(): void
    {
        $tables = $this->db->getSchema()->getTableNames();

        $this->assertIsArray($tables);
    }

    public function testTableExistsAfterCreation(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->db->createCommand("
            CREATE TABLE `$table`
            (
                `id`   UInt64,
                `name` String
            )
            ENGINE = Memory
        ")->execute();

        $tables = $this->db->getSchema()->getTableNames();

        $this->assertContains($table, $tables);
    }

    public function testGetTableSchema(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->db->createCommand("
            CREATE TABLE `$table`
            (
                `id`    UInt64,
                `name`  String,
                `score` Float64
            )
            ENGINE = Memory
        ")->execute();

        $schema = $this->db->getTableSchema($table);

        $this->assertNotNull($schema);
        $this->assertNotNull($schema->getColumn('id'));
        $this->assertNotNull($schema->getColumn('name'));
        $this->assertNotNull($schema->getColumn('score'));
    }

    public function testGetTableSchemaReturnsNullForMissingTable(): void
    {
        $schema = $this->db->getTableSchema('this_table_does_not_exist_xyz');

        $this->assertNull($schema);
    }

    public function testTableComment(): void
    {
        $table = $this->tmpTable();
        $this->dropAfterTest($table);

        $this->db->createCommand("
            CREATE TABLE `$table`
            (`id` UInt64)
            ENGINE = Memory
            COMMENT 'Test comment'
        ")->execute();

        // Refresh schema cache
        $this->db->getSchema()->refreshTableSchema($table);
        $schema = $this->db->getTableSchema($table);

        $this->assertNotNull($schema);
        $this->assertSame('Test comment', $schema->getComment());
    }

    public function testFindSchemaNames(): void
    {
        $schemas = $this->db->getSchema()->getSchemaNames();

        $this->assertIsArray($schemas);
        $this->assertNotEmpty($schemas);
    }
}
