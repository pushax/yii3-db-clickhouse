<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests\Integration;

use Pushax\Db\ClickHouse\BatchInsert;

final class BatchInsertTest extends IntegrationTestCase
{
    private string $table;

    protected function setUp(): void
    {
        parent::setUp();

        $this->table = $this->tmpTable('batch');
        $this->dropAfterTest($this->table);

        $this->db->createCommand("
            CREATE TABLE `{$this->table}`
            (
                `id`     UInt64,
                `name`   String,
                `score`  Float64,
                `active` UInt8
            )
            ENGINE = Memory
        ")->execute();
    }

    public function testInsertSingleRow(): void
    {
        $batch = new BatchInsert($this->db, $this->table, ['id', 'name', 'score', 'active']);
        $batch->addRow([1, 'Alice', 9.5, true]);
        $inserted = $batch->execute();

        $this->assertSame(1, $inserted);

        $count = $this->db->createCommand("SELECT count() FROM `{$this->table}`")->queryScalar();
        $this->assertSame('1', $count);
    }

    public function testInsertMultipleRows(): void
    {
        $batch = new BatchInsert($this->db, $this->table, ['id', 'name', 'score', 'active']);
        $batch->addRows([
            [1, 'Alice', 9.5, true],
            [2, 'Bob',   7.2, false],
            [3, 'Carol', 8.8, true],
        ]);

        $inserted = $batch->execute();

        $this->assertSame(3, $inserted);

        $count = $this->db->createCommand("SELECT count() FROM `{$this->table}`")->queryScalar();
        $this->assertSame('3', $count);
    }

    public function testDataIsCorrectlyStored(): void
    {
        $batch = new BatchInsert($this->db, $this->table, ['id', 'name', 'score', 'active']);
        $batch->addRow([42, "O'Brien", 3.14, false]);
        $batch->execute();

        $row = $this->db->createCommand(
            "SELECT * FROM `{$this->table}` WHERE id = 42"
        )->queryOne();

        $this->assertNotNull($row);
        $this->assertSame('42', $row['id']);
        $this->assertSame("O'Brien", $row['name']);
        $this->assertSame('3.14', $row['score']);
        $this->assertSame('0', $row['active']); // false → 0
    }

    public function testInsertWithBatchSize(): void
    {
        $rows = array_map(fn($i) => [$i, "user_$i", (float)$i, 1], range(1, 10));

        $batch = new BatchInsert($this->db, $this->table, ['id', 'name', 'score', 'active'], batchSize: 3);
        $batch->addRows($rows);
        $inserted = $batch->execute();

        $this->assertSame(10, $inserted);

        $count = $this->db->createCommand("SELECT count() FROM `{$this->table}`")->queryScalar();
        $this->assertSame('10', $count);
    }

    public function testEmptyExecuteInsertsNothing(): void
    {
        $batch = new BatchInsert($this->db, $this->table, ['id', 'name', 'score', 'active']);
        $inserted = $batch->execute();

        $this->assertSame(0, $inserted);

        $count = $this->db->createCommand("SELECT count() FROM `{$this->table}`")->queryScalar();
        $this->assertSame('0', $count);
    }

    public function testNullValueIsStored(): void
    {
        // Recreate table with nullable column
        $table = $this->tmpTable('nullable');
        $this->dropAfterTest($table);

        $this->db->createCommand("
            CREATE TABLE `$table`
            (`id` UInt64, `val` Nullable(String))
            ENGINE = Memory
        ")->execute();

        $batch = new BatchInsert($this->db, $table, ['id', 'val']);
        $batch->addRow([1, null]);
        $batch->execute();

        $row = $this->db->createCommand("SELECT val FROM `$table` WHERE id = 1")->queryOne();
        $this->assertNull($row['val']);
    }

    public function testExecuteClearsPendingRows(): void
    {
        $batch = new BatchInsert($this->db, $this->table, ['id', 'name', 'score', 'active']);
        $batch->addRow([1, 'Alice', 1.0, 1]);
        $batch->execute();

        $this->assertSame(0, $batch->getPendingCount());
    }
}
