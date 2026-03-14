<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests\Integration;

use Pushax\Db\ClickHouse\MutationBuilder;

final class MutationBuilderTest extends IntegrationTestCase
{
    private string $table;

    protected function setUp(): void
    {
        parent::setUp();

        // Mutations require a MergeTree table, not Memory
        $this->table = $this->tmpTable('mutations');
        $this->dropAfterTest($this->table);

        $this->db->createCommand("
            CREATE TABLE `{$this->table}`
            (
                `id`     UInt64,
                `status` String,
                `value`  Float64
            )
            ENGINE = MergeTree()
            ORDER BY id
        ")->execute();

        // Insert test rows
        $this->db->createCommand("
            INSERT INTO `{$this->table}` (id, status, value) VALUES
            (1, 'active', 10.0),
            (2, 'active', 20.0),
            (3, 'active', 30.0)
        ")->execute();
    }

    public function testUpdateMutation(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->update($this->table, ['status' => 'archived'], 'id = 1');

        // Wait for mutation to complete
        $mutation->waitForMutations($this->table, 30);

        $status = $this->db->createCommand(
            "SELECT status FROM `{$this->table}` WHERE id = 1"
        )->queryScalar();

        $this->assertSame('archived', $status);
    }

    public function testDeleteMutation(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->delete($this->table, 'id = 3');

        $mutation->waitForMutations($this->table, 30);

        $count = $this->db->createCommand(
            "SELECT count() FROM `{$this->table}` WHERE id = 3"
        )->queryScalar();

        $this->assertSame('0', $count);
    }

    public function testGetMutationStatus(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->delete($this->table, 'id = 2');

        $statuses = $mutation->getMutationStatus($this->table);

        $this->assertIsArray($statuses);
        $this->assertNotEmpty($statuses);
        $this->assertArrayHasKey('mutation_id', $statuses[0]);
        $this->assertArrayHasKey('is_done', $statuses[0]);
    }

    public function testWaitForMutationsReturnsTrueWhenDone(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->update($this->table, ['value' => 99.0], 'id = 1');

        $result = $mutation->waitForMutations($this->table, 30);

        $this->assertTrue($result);
    }

    public function testUpdateMultipleColumns(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->update(
            $this->table,
            ['status' => 'done', 'value' => 0.0],
            'id = 2',
        );

        $mutation->waitForMutations($this->table, 30);

        $row = $this->db->createCommand(
            "SELECT status, value FROM `{$this->table}` WHERE id = 2"
        )->queryOne();

        $this->assertSame('done', $row['status']);
        $this->assertSame('0', $row['value']);
    }
}
