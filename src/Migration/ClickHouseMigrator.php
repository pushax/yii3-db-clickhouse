<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Migration;

use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Migration\Informer\MigrationInformerInterface;
use Yiisoft\Db\Migration\Informer\NullMigrationInformer;
use Yiisoft\Db\Migration\MigrationInterface;
use Yiisoft\Db\Migration\RevertibleMigrationInterface;
use Yiisoft\Db\Query\Query;

use const SORT_DESC;

/**
 * ClickHouse-specific migrator that handles ClickHouse limitations.
 *
 * Key differences from the standard Migrator:
 * - Does NOT wrap migrations in transactions (ClickHouse doesn't support transactions)
 * - Creates migration history table with MergeTree engine
 * - Uses `UInt32` for ID instead of auto-incrementing primary key
 * - Uses ALTER TABLE ... DELETE for removing migration history entries
 *
 * Usage:
 * ```php
 * $migrator = new ClickHouseMigrator($connection, $informer);
 * $migrator->up($migration);
 * ```
 */
final class ClickHouseMigrator
{
    private bool $checkMigrationHistoryTable = true;

    public function __construct(
        private readonly ConnectionInterface $db,
        private readonly MigrationInformerInterface $informer,
        private readonly string $historyTable = '{{%migration}}',
        private ?int $migrationNameLimit = 180,
        private readonly ?int $maxSqlOutputLength = null,
    ) {}

    /**
     * Applies a migration.
     *
     * Supports both standard Yii migrations ({@see MigrationInterface}) and
     * ClickHouse-native migrations ({@see ClickHouseMigrationInterface}).
     *
     * Note: ClickHouse does NOT support transactions. Migrations are executed directly
     * without transaction wrapping, regardless of whether the migration implements
     * TransactionalMigrationInterface.
     */
    public function up(MigrationInterface|ClickHouseMigrationInterface $migration): void
    {
        $this->checkMigrationHistoryTable();

        $builder = $this->createBuilder();

        if ($migration instanceof ClickHouseMigrationInterface) {
            $migration->up($builder);
        } else {
            $migration->up($builder);
        }

        $this->addMigrationToHistory($migration);
    }

    /**
     * Reverts a migration.
     */
    public function down(RevertibleMigrationInterface|RevertibleClickHouseMigrationInterface $migration): void
    {
        $this->checkMigrationHistoryTable();

        $builder = $this->createBuilder();

        if ($migration instanceof RevertibleClickHouseMigrationInterface) {
            $migration->down($builder);
        } else {
            $migration->down($builder);
        }

        $this->removeMigrationFromHistory($migration);
    }

    public function getMigrationNameLimit(): ?int
    {
        if ($this->migrationNameLimit !== null) {
            return $this->migrationNameLimit;
        }

        $tableSchema = $this->db->getSchema()->getTableSchema($this->historyTable);

        if ($tableSchema === null) {
            return null;
        }

        $limit = $tableSchema->getColumns()['name']->getSize();

        if ($limit === null) {
            return null;
        }

        return $this->migrationNameLimit = $limit;
    }

    /**
     * @psalm-return array<class-string, int|string>
     */
    public function getHistory(?int $limit = null): array
    {
        $this->checkMigrationHistoryTable();

        $query = (new Query($this->db))
            ->select(['apply_time', 'name'])
            ->from($this->historyTable)
            ->orderBy(['apply_time' => SORT_DESC, 'id' => SORT_DESC])
            ->indexBy('name');

        if ($limit > 0) {
            $query->limit($limit);
        }

        /** @psalm-var array<class-string, int|string> */
        return $query->column();
    }

    public function getHistoryTable(): string
    {
        return $this->historyTable;
    }

    private function addMigrationToHistory(MigrationInterface|ClickHouseMigrationInterface $migration): void
    {
        $this->db->createCommand()->insert(
            $this->historyTable,
            [
                'id' => time() * 1000 + random_int(0, 999),
                'name' => $this->getMigrationName($migration),
                'apply_time' => time(),
            ],
        )->execute();
    }

    private function removeMigrationFromHistory(
        MigrationInterface|ClickHouseMigrationInterface $migration,
    ): void {
        // ClickHouse uses ALTER TABLE ... DELETE for mutations
        $name = $this->getMigrationName($migration);
        $quoter = $this->db->getQuoter();

        $sql = 'ALTER TABLE ' . $quoter->quoteTableName($this->historyTable)
            . ' DELETE WHERE `name` = ' . $quoter->quoteValue($name);

        $this->db->createCommand($sql)->execute();
    }

    private function getMigrationName(MigrationInterface|ClickHouseMigrationInterface $migration): string
    {
        return $migration::class;
    }

    private function checkMigrationHistoryTable(): void
    {
        if (!$this->checkMigrationHistoryTable) {
            return;
        }

        if ($this->db->getSchema()->getTableSchema($this->historyTable, true) === null) {
            $this->createMigrationHistoryTable();
        }

        $this->checkMigrationHistoryTable = false;
    }

    /**
     * Creates the migration history table using ClickHouse MergeTree engine.
     *
     * Unlike standard RDBMS, ClickHouse:
     * - Uses MergeTree engine instead of InnoDB/etc.
     * - Uses UInt64 for ID (no auto-increment)
     * - ORDER BY is required for MergeTree
     */
    private function createMigrationHistoryTable(): void
    {
        /** @var string $tableName */
        $tableName = $this->db->getQuoter()->getRawTableName($this->historyTable);
        $this->informer->beginCreateHistoryTable('Creating migration history table "' . $tableName . '"...');

        $b = $this->createBuilder(new NullMigrationInformer());

        $b->createTable($this->historyTable, [
            'id' => 'UInt64',
            'name' => 'String',
            'apply_time' => 'UInt32',
        ], 'ENGINE = MergeTree() ORDER BY (id)');

        $this->informer->endCreateHistoryTable('Done.');
    }

    private function createBuilder(?MigrationInformerInterface $informer = null): ClickHouseMigrationBuilder
    {
        return new ClickHouseMigrationBuilder(
            $this->db,
            $informer ?? $this->informer,
            $this->maxSqlOutputLength,
        );
    }

}
