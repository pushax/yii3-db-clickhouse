<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Migration;

/**
 * Defines a revertible ClickHouse migration interface.
 *
 * Provides both `up()` for applying and `down()` for reverting the migration.
 *
 * Note: Reverting ClickHouse schema changes can be complex because:
 * - Data cannot be easily restored after DROP TABLE
 * - ALTER TABLE ... DELETE is a mutation (asynchronous, heavy operation)
 * - Column type changes may lose data precision
 *
 * Consider carefully whether a migration should be revertible.
 */
interface RevertibleClickHouseMigrationInterface extends ClickHouseMigrationInterface
{
    /**
     * Contains the logic to be executed when reverting this migration.
     */
    public function down(ClickHouseMigrationBuilder $b): void;
}
