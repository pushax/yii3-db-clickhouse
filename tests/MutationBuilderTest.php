<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests;

use PHPUnit\Framework\TestCase;
use Pushax\Db\ClickHouse\MutationBuilder;
use Pushax\Db\ClickHouse\Quoter;
use Yiisoft\Db\Command\CommandInterface;
use Yiisoft\Db\Connection\ConnectionInterface;

final class MutationBuilderTest extends TestCase
{
    private string $capturedSql = '';
    private ConnectionInterface $db;

    protected function setUp(): void
    {
        $quoter = new Quoter('`', '`');

        $command = $this->createMock(CommandInterface::class);
        $command->method('execute')->willReturn(0);
        $command->method('queryAll')->willReturn([]);
        $command->method('queryScalar')->willReturn('0');

        $this->db = $this->createMock(ConnectionInterface::class);
        $this->db->method('getQuoter')->willReturn($quoter);
        $this->db->method('createCommand')->willReturnCallback(
            function (string $sql, array $params = []) use ($command) {
                $this->capturedSql = $sql;
                return $command;
            }
        );
    }

    public function testUpdateGeneratesCorrectSql(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->update('events', ['status' => 'archived'], "event_date < today()");

        $this->assertStringContainsString('ALTER TABLE `events`', $this->capturedSql);
        $this->assertStringContainsString('UPDATE', $this->capturedSql);
        $this->assertStringContainsString("`status` = 'archived'", $this->capturedSql);
        $this->assertStringContainsString('WHERE event_date < today()', $this->capturedSql);
    }

    public function testUpdateWithIntegerValue(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->update('events', ['count' => 42], 'id = 1');

        $this->assertStringContainsString('`count` = 42', $this->capturedSql);
    }

    public function testUpdateWithFloatValue(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->update('events', ['score' => 3.14], 'id = 1');

        $this->assertStringContainsString('`score` = 3.14', $this->capturedSql);
    }

    public function testUpdateWithNullValue(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->update('events', ['deleted_at' => null], 'id = 1');

        $this->assertStringContainsString('`deleted_at` = NULL', $this->capturedSql);
    }

    public function testUpdateWithBooleanValues(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->update('events', ['active' => true, 'deleted' => false], 'id = 1');

        $this->assertStringContainsString('`active` = 1', $this->capturedSql);
        $this->assertStringContainsString('`deleted` = 0', $this->capturedSql);
    }

    public function testDeleteGeneratesCorrectSql(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->delete('events', "event_date < today() - 90");

        $this->assertStringContainsString('ALTER TABLE `events`', $this->capturedSql);
        $this->assertStringContainsString('DELETE WHERE event_date < today() - 90', $this->capturedSql);
    }

    public function testGetMutationStatusReturnsArray(): void
    {
        $mutation = new MutationBuilder($this->db);
        $result = $mutation->getMutationStatus('events');

        $this->assertIsArray($result);
    }

    public function testGetMutationStatusWithDatabase(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->getMutationStatus('events', 'analytics');

        $this->assertStringContainsString('system.mutations', $this->capturedSql);
    }

    public function testWaitForMutationsReturnsTrueWhenNoPending(): void
    {
        $mutation = new MutationBuilder($this->db);

        $result = $mutation->waitForMutations('events', timeoutSeconds: 5);

        $this->assertTrue($result);
    }

    public function testKillMutationGeneratesCorrectSql(): void
    {
        $mutation = new MutationBuilder($this->db);
        $mutation->killMutation('0000000001', 'events');

        $this->assertStringContainsString('KILL MUTATION', $this->capturedSql);
    }
}
