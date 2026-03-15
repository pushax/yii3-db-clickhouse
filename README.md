# ClickHouse Driver for Yii Database

A ClickHouse driver for [Yii Database](https://github.com/yiisoft/db), built as an independent package.
Uses the ClickHouse HTTP interface — no PDO extension required.

[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

## Requirements

- PHP 8.1–8.5
- Extensions: `curl`, `ctype`
- ClickHouse server with HTTP interface (default port 8123)

## Installation

```bash
composer require pushax/yii3-db-clickhouse
```

## Configuration

```php
use Pushax\Db\ClickHouse\Connection;
use Pushax\Db\ClickHouse\Driver;
use Pushax\Db\ClickHouse\Dsn;

$dsn = new Dsn(
    host: '127.0.0.1',
    databaseName: 'default',
    port: '8123',
);

$driver = new Driver($dsn, username: 'default', password: '');
$db = new Connection($driver, $schemaCache);
```

## Usage

### Querying

```php
// Raw command
$rows = $db->createCommand('SELECT * FROM events WHERE event_date >= :date', [
    ':date' => '2024-01-01',
])->queryAll();

// Query builder
$rows = $db->createQuery()
    ->from('events')
    ->where(['>=', 'event_date', '2024-01-01'])
    ->limit(100)
    ->all();
```

### Creating Tables

Use `TableBuilder` for a fluent interface to ClickHouse-specific `CREATE TABLE` syntax:

```php
use Pushax\Db\ClickHouse\ClickHouseDataType;
use Pushax\Db\ClickHouse\TableBuilder;
use Pushax\Db\ClickHouse\TableEngine;

$builder = new TableBuilder($db);

$builder
    ->table('events')
    ->ifNotExists()
    ->column('event_date', ClickHouseDataType::DATE)
    ->column('event_type', ClickHouseDataType::lowCardinality(ClickHouseDataType::STRING))
    ->column('user_id', ClickHouseDataType::UINT64)
    ->column('value', ClickHouseDataType::FLOAT64)
    ->engine(TableEngine::MERGE_TREE)
    ->partitionBy('toYYYYMM(event_date)')
    ->orderBy(['event_date', 'event_type', 'user_id'])
    ->primaryKey(['event_date', 'event_type'])
    ->sampleBy('user_id')
    ->ttl('event_date + INTERVAL 1 MONTH')
    ->settings(['index_granularity' => 8192])
    ->index('idx_user', 'user_id', 'minmax', 3)
    ->execute();
```

`TableBuilder` supports:

| Method | Description |
|---|---|
| `engine(string, ?string)` | Set the table engine and optional parameters |
| `partitionBy(string)` | Set the `PARTITION BY` expression |
| `orderBy(string\|array)` | Set the `ORDER BY` sorting key |
| `primaryKey(string\|array)` | Set the `PRIMARY KEY` |
| `sampleBy(string)` | Set the `SAMPLE BY` expression |
| `ttl(string)` | Add a `TTL` expression |
| `settings(array)` | Add engine-specific `SETTINGS` |
| `index(name, expr, type, granularity)` | Add a data skipping index |
| `onCluster(string)` | Add `ON CLUSTER` clause |
| `comment(string)` | Add a table comment |
| `build()` | Return the SQL string without executing |
| `execute()` | Execute the statement |

### Table Engines

All standard ClickHouse engines are available as constants on `TableEngine`:

```php
use Pushax\Db\ClickHouse\TableEngine;

// MergeTree family
TableEngine::MERGE_TREE
TableEngine::REPLACING_MERGE_TREE
TableEngine::SUMMING_MERGE_TREE
TableEngine::AGGREGATING_MERGE_TREE
TableEngine::COLLAPSING_MERGE_TREE
TableEngine::VERSIONED_COLLAPSING_MERGE_TREE

// Replicated variants
TableEngine::REPLICATED_MERGE_TREE
TableEngine::REPLICATED_REPLACING_MERGE_TREE
// ... and more

// Log family
TableEngine::TINY_LOG
TableEngine::STRIPE_LOG
TableEngine::LOG

// Special engines
TableEngine::DISTRIBUTED
TableEngine::MEMORY
TableEngine::MATERIALIZED_VIEW

// Integration engines
TableEngine::KAFKA
TableEngine::S3
TableEngine::MYSQL
TableEngine::HDFS
```

### Mutations (UPDATE / DELETE)

ClickHouse mutations are asynchronous `ALTER TABLE` operations:

```php
use Pushax\Db\ClickHouse\MutationBuilder;

$mutation = new MutationBuilder($db);

// Update rows matching a condition
$mutation->update('events', ['status' => 'archived'], "event_date < today() - 30");

// Delete rows
$mutation->delete('events', "event_date < today() - 90");

// Check mutation status
$statuses = $mutation->getMutationStatus('events');

// Wait for all mutations to finish (with timeout)
$completed = $mutation->waitForMutations('events', timeoutSeconds: 120);

// Kill a running mutation
$mutation->killMutation($mutationId, 'events');
```

> **Note:** Mutations are heavy, asynchronous operations that rewrite data parts. They are not suitable for frequent small updates.

### Batch Insert

```php
use Pushax\Db\ClickHouse\BatchInsert;
use Pushax\Db\ClickHouse\ClickHouseDataType;

$batch = new BatchInsert($db, 'events', ['event_date', 'user_id', 'value'], batchSize: 1000);

$batch->addRow(['2024-01-01', 1, 9.5]);
$batch->addRows([
    ['2024-01-02', 2, 7.2],
    ['2024-01-03', 3, 8.8],
]);

$inserted = $batch->execute(); // returns total rows inserted
```

### Partition Management

```php
use Pushax\Db\ClickHouse\PartitionManager;

$pm = new PartitionManager($db);

// List partitions with stats (rows, size, date range)
$partitions = $pm->getPartitions('events');

// Drop a partition
$pm->dropPartition('events', '202301');

// Detach / attach
$pm->detachPartition('events', '202301');
$pm->attachPartition('events', '202301');

// Move partition to another table (tables must share the same structure)
$pm->movePartition('events', 'events_archive', '202301');

// Replace a partition in the target from the source
$pm->replacePartition('events_new', 'events', '202301');

// Freeze partition (backup to shadow directory)
$pm->freezePartition('events', '202301');

// Clear a column within a partition
$pm->clearColumnInPartition('events', '202301', 'metadata');
```

### Migrations

Implement `ClickHouseMigrationInterface` to write ClickHouse-specific migrations:

```php
use Pushax\Db\ClickHouse\ClickHouseDataType;
use Pushax\Db\ClickHouse\Migration\ClickHouseMigrationInterface;
use Pushax\Db\ClickHouse\Migration\ClickHouseMigrationBuilder;

final class M240101_000000_CreateEventsTable implements ClickHouseMigrationInterface
{
    public function up(ClickHouseMigrationBuilder $b): void
    {
        $b->createMergeTreeTable(
            'events',
            [
                'event_date' => ClickHouseDataType::DATE,
                'event_type' => ClickHouseDataType::lowCardinality(ClickHouseDataType::STRING),
                'user_id'    => ClickHouseDataType::UINT64,
                'value'      => ClickHouseDataType::FLOAT64,
            ],
            orderBy: ['event_date', 'event_type', 'user_id'],
            partitionBy: 'toYYYYMM(event_date)',
            settings: ['index_granularity' => 8192],
        );

        $b->addSkippingIndex('events', 'idx_user', 'user_id', 'minmax', 3);
    }

    public function down(ClickHouseMigrationBuilder $b): void
    {
        $b->dropTable('events');
    }
}
```

> **Note:** ClickHouse migrations are never transactional — all operations are applied immediately.

## Column Types

All ClickHouse type names are available as constants on `ClickHouseDataType`:

```php
use Pushax\Db\ClickHouse\ClickHouseDataType;

// Integers
ClickHouseDataType::INT8 / INT16 / INT32 / INT64 / INT128 / INT256
ClickHouseDataType::UINT8 / UINT16 / UINT32 / UINT64 / UINT128 / UINT256

// Floats
ClickHouseDataType::FLOAT32
ClickHouseDataType::FLOAT64

// Decimal
ClickHouseDataType::decimal(10, 2)   // → 'Decimal(10, 2)'

// Boolean
ClickHouseDataType::BOOL

// Strings
ClickHouseDataType::STRING
ClickHouseDataType::fixedString(16)  // → 'FixedString(16)'

// Dates
ClickHouseDataType::DATE
ClickHouseDataType::DATE32
ClickHouseDataType::DATETIME
ClickHouseDataType::dateTime64(3)                    // → 'DateTime64(3)'
ClickHouseDataType::dateTime64(3, 'Europe/London')   // → 'DateTime64(3, 'Europe/London')'

// Other
ClickHouseDataType::UUID
ClickHouseDataType::IPV4
ClickHouseDataType::IPV6
ClickHouseDataType::JSON

// Enums
ClickHouseDataType::enum8(['active' => 1, 'archived' => 2])   // → "Enum8('active' = 1, 'archived' = 2)"
ClickHouseDataType::enum16(['active' => 1, 'archived' => 2])

// Wrappers
ClickHouseDataType::nullable(ClickHouseDataType::STRING)                      // → 'Nullable(String)'
ClickHouseDataType::lowCardinality(ClickHouseDataType::STRING)                // → 'LowCardinality(String)'
ClickHouseDataType::array(ClickHouseDataType::UINT64)                        // → 'Array(UInt64)'
```

## Testing

```bash
# Unit tests
composer test

# Integration tests (requires a running ClickHouse instance)
composer test:integration
```

## License

This project is released under the [BSD-3-Clause License](LICENSE).
