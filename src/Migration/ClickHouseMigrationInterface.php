<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Migration;

/**
 * Defines a ClickHouse-specific migration interface.
 *
 * Unlike the standard {@see \Yiisoft\Db\Migration\MigrationInterface}, this interface
 * receives a {@see ClickHouseMigrationBuilder} which provides ClickHouse-specific
 * methods like `createMergeTreeTable()`, `createReplicatedTable()`, etc.
 *
 * ClickHouse migrations are NEVER transactional - all operations are applied immediately.
 *
 * Usage:
 * ```php
 * final class M240101_000000_CreateEventsTable implements ClickHouseMigrationInterface
 * {
 *     public function up(ClickHouseMigrationBuilder $b): void
 *     {
 *         $b->createMergeTreeTable(
 *             'events',
 *             [
 *                 'event_date' => ClickHouseDataType::DATE,
 *                 'event_type' => ClickHouseDataType::lowCardinality(ClickHouseDataType::STRING),
 *                 'user_id'    => ClickHouseDataType::UINT64,
 *                 'value'      => ClickHouseDataType::FLOAT64,
 *                 'metadata'   => ClickHouseDataType::STRING,
 *             ],
 *             orderBy: ['event_date', 'event_type', 'user_id'],
 *             partitionBy: 'toYYYYMM(event_date)',
 *             settings: ['index_granularity' => 8192],
 *         );
 *
 *         $b->addSkippingIndex('events', 'idx_user', 'user_id', 'minmax', 3);
 *     }
 *
 *     public function down(ClickHouseMigrationBuilder $b): void
 *     {
 *         $b->dropTable('events');
 *     }
 * }
 * ```
 */
interface ClickHouseMigrationInterface
{
    /**
     * Contains the logic to be executed when applying this migration.
     */
    public function up(ClickHouseMigrationBuilder $b): void;
}
