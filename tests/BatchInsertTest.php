<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests;

use PHPUnit\Framework\TestCase;
use Pushax\Db\ClickHouse\BatchInsert;
use Pushax\Db\ClickHouse\Quoter;
use Yiisoft\Db\Command\CommandInterface;
use Yiisoft\Db\Connection\ConnectionInterface;

final class BatchInsertTest extends TestCase
{
    private ConnectionInterface $db;

    protected function setUp(): void
    {
        $quoter = new Quoter('`', '`');

        $command = $this->createMock(CommandInterface::class);
        $command->method('execute')->willReturn(0);

        $this->db = $this->createMock(ConnectionInterface::class);
        $this->db->method('getQuoter')->willReturn($quoter);
        $this->db->method('createCommand')->willReturn($command);
    }

    public function testInitialPendingCountIsZero(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);

        $this->assertSame(0, $batch->getPendingCount());
    }

    public function testAddRowIncrementsCount(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);
        $batch->addRow(['2024-01-01', 1]);

        $this->assertSame(1, $batch->getPendingCount());
    }

    public function testAddRowsIncrementsCount(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);
        $batch->addRows([
            ['2024-01-01', 1],
            ['2024-01-01', 2],
            ['2024-01-01', 3],
        ]);

        $this->assertSame(3, $batch->getPendingCount());
    }

    public function testAddRowReturnsSelf(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);

        $this->assertSame($batch, $batch->addRow(['2024-01-01', 1]));
    }

    public function testAddRowsReturnsSelf(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);

        $this->assertSame($batch, $batch->addRows([['2024-01-01', 1]]));
    }

    public function testClearResetsPendingCount(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);
        $batch->addRows([['2024-01-01', 1], ['2024-01-02', 2]]);
        $batch->clear();

        $this->assertSame(0, $batch->getPendingCount());
    }

    public function testClearReturnsSelf(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date']);

        $this->assertSame($batch, $batch->clear());
    }

    public function testExecuteEmptyBatchReturnsZero(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);

        $this->assertSame(0, $batch->execute());
    }

    public function testExecuteReturnsRowCount(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);
        $batch->addRows([
            ['2024-01-01', 1],
            ['2024-01-01', 2],
        ]);

        $this->assertSame(2, $batch->execute());
    }

    public function testExecuteClearsPendingRows(): void
    {
        $batch = new BatchInsert($this->db, 'events', ['date', 'user_id']);
        $batch->addRow(['2024-01-01', 1]);
        $batch->execute();

        $this->assertSame(0, $batch->getPendingCount());
    }

    public function testExecuteWithBatchSizeCallsCommandMultipleTimes(): void
    {
        $quoter = new Quoter('`', '`');

        $command = $this->createMock(CommandInterface::class);
        $command->expects($this->exactly(3))->method('execute')->willReturn(0);

        $db = $this->createMock(ConnectionInterface::class);
        $db->method('getQuoter')->willReturn($quoter);
        $db->method('createCommand')->willReturn($command);

        $batch = new BatchInsert($db, 'events', ['date', 'user_id'], batchSize: 2);
        $batch->addRows([
            ['2024-01-01', 1],
            ['2024-01-01', 2],
            ['2024-01-01', 3],
            ['2024-01-01', 4],
            ['2024-01-01', 5],
        ]);

        $inserted = $batch->execute();

        $this->assertSame(5, $inserted);
    }

    public function testSqlContainsTableAndColumns(): void
    {
        $quoter = new Quoter('`', '`');
        $capturedSql = null;

        $command = $this->createMock(CommandInterface::class);
        $command->method('execute')->willReturn(0);

        $db = $this->createMock(ConnectionInterface::class);
        $db->method('getQuoter')->willReturn($quoter);
        $db->method('createCommand')->willReturnCallback(
            function (string $sql) use ($command, &$capturedSql) {
                $capturedSql = $sql;
                return $command;
            }
        );

        $batch = new BatchInsert($db, 'events', ['event_date', 'user_id']);
        $batch->addRow(['2024-01-01', 42]);
        $batch->execute();

        $this->assertStringContainsString('INSERT INTO `events`', $capturedSql);
        $this->assertStringContainsString('`event_date`', $capturedSql);
        $this->assertStringContainsString('`user_id`', $capturedSql);
        $this->assertStringContainsString("'2024-01-01'", $capturedSql);
        $this->assertStringContainsString('42', $capturedSql);
    }

    public function testNullValueRenderedAsNull(): void
    {
        $quoter = new Quoter('`', '`');
        $capturedSql = null;

        $command = $this->createMock(CommandInterface::class);
        $command->method('execute')->willReturn(0);

        $db = $this->createMock(ConnectionInterface::class);
        $db->method('getQuoter')->willReturn($quoter);
        $db->method('createCommand')->willReturnCallback(
            function (string $sql) use ($command, &$capturedSql) {
                $capturedSql = $sql;
                return $command;
            }
        );

        $batch = new BatchInsert($db, 'events', ['user_id', 'value']);
        $batch->addRow([1, null]);
        $batch->execute();

        $this->assertStringContainsString('NULL', $capturedSql);
    }

    public function testBooleanValuesRendered(): void
    {
        $quoter = new Quoter('`', '`');
        $capturedSql = null;

        $command = $this->createMock(CommandInterface::class);
        $command->method('execute')->willReturn(0);

        $db = $this->createMock(ConnectionInterface::class);
        $db->method('getQuoter')->willReturn($quoter);
        $db->method('createCommand')->willReturnCallback(
            function (string $sql) use ($command, &$capturedSql) {
                $capturedSql = $sql;
                return $command;
            }
        );

        $batch = new BatchInsert($db, 'flags', ['active', 'deleted']);
        $batch->addRow([true, false]);
        $batch->execute();

        $this->assertStringContainsString('(1, 0)', $capturedSql);
    }
}
