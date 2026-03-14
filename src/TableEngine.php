<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

/**
 * Defines the available table engines for ClickHouse.
 *
 * @link https://clickhouse.com/docs/en/engines/table-engines
 */
final class TableEngine
{
    // MergeTree Family
    public const MERGE_TREE = 'MergeTree';
    public const REPLACING_MERGE_TREE = 'ReplacingMergeTree';
    public const SUMMING_MERGE_TREE = 'SummingMergeTree';
    public const AGGREGATING_MERGE_TREE = 'AggregatingMergeTree';
    public const COLLAPSING_MERGE_TREE = 'CollapsingMergeTree';
    public const VERSIONED_COLLAPSING_MERGE_TREE = 'VersionedCollapsingMergeTree';
    public const GRAPHITE_MERGE_TREE = 'GraphiteMergeTree';

    // Replicated MergeTree Family
    public const REPLICATED_MERGE_TREE = 'ReplicatedMergeTree';
    public const REPLICATED_REPLACING_MERGE_TREE = 'ReplicatedReplacingMergeTree';
    public const REPLICATED_SUMMING_MERGE_TREE = 'ReplicatedSummingMergeTree';
    public const REPLICATED_AGGREGATING_MERGE_TREE = 'ReplicatedAggregatingMergeTree';
    public const REPLICATED_COLLAPSING_MERGE_TREE = 'ReplicatedCollapsingMergeTree';
    public const REPLICATED_VERSIONED_COLLAPSING_MERGE_TREE = 'ReplicatedVersionedCollapsingMergeTree';
    public const REPLICATED_GRAPHITE_MERGE_TREE = 'ReplicatedGraphiteMergeTree';

    // Log Family
    public const TINY_LOG = 'TinyLog';
    public const STRIPE_LOG = 'StripeLog';
    public const LOG = 'Log';

    // Integration Engines
    public const KAFKA = 'Kafka';
    public const MYSQL = 'MySQL';
    public const JDBC = 'JDBC';
    public const ODBC = 'ODBC';
    public const HDFS = 'HDFS';
    public const S3 = 'S3';
    public const URL = 'URL';

    // Special Engines
    public const DISTRIBUTED = 'Distributed';
    public const DICTIONARY = 'Dictionary';
    public const MEMORY = 'Memory';
    public const BUFFER = 'Buffer';
    public const FILE = 'File';
    public const NULL = 'Null';
    public const SET = 'Set';
    public const JOIN = 'Join';
    public const MATERIALIZED_VIEW = 'MaterializedView';
    public const LIVE_VIEW = 'LiveView';
}
