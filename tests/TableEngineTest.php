<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests;

use PHPUnit\Framework\TestCase;
use Pushax\Db\ClickHouse\TableEngine;

final class TableEngineTest extends TestCase
{
    public function testMergeTreeFamily(): void
    {
        $this->assertSame('MergeTree', TableEngine::MERGE_TREE);
        $this->assertSame('ReplacingMergeTree', TableEngine::REPLACING_MERGE_TREE);
        $this->assertSame('SummingMergeTree', TableEngine::SUMMING_MERGE_TREE);
        $this->assertSame('AggregatingMergeTree', TableEngine::AGGREGATING_MERGE_TREE);
        $this->assertSame('CollapsingMergeTree', TableEngine::COLLAPSING_MERGE_TREE);
        $this->assertSame('VersionedCollapsingMergeTree', TableEngine::VERSIONED_COLLAPSING_MERGE_TREE);
        $this->assertSame('GraphiteMergeTree', TableEngine::GRAPHITE_MERGE_TREE);
    }

    public function testReplicatedMergeTreeFamily(): void
    {
        $this->assertSame('ReplicatedMergeTree', TableEngine::REPLICATED_MERGE_TREE);
        $this->assertSame('ReplicatedReplacingMergeTree', TableEngine::REPLICATED_REPLACING_MERGE_TREE);
        $this->assertSame('ReplicatedSummingMergeTree', TableEngine::REPLICATED_SUMMING_MERGE_TREE);
        $this->assertSame('ReplicatedAggregatingMergeTree', TableEngine::REPLICATED_AGGREGATING_MERGE_TREE);
    }

    public function testLogFamily(): void
    {
        $this->assertSame('TinyLog', TableEngine::TINY_LOG);
        $this->assertSame('StripeLog', TableEngine::STRIPE_LOG);
        $this->assertSame('Log', TableEngine::LOG);
    }

    public function testSpecialEngines(): void
    {
        $this->assertSame('Distributed', TableEngine::DISTRIBUTED);
        $this->assertSame('Memory', TableEngine::MEMORY);
        $this->assertSame('Buffer', TableEngine::BUFFER);
        $this->assertSame('Null', TableEngine::NULL);
        $this->assertSame('MaterializedView', TableEngine::MATERIALIZED_VIEW);
    }

    public function testIntegrationEngines(): void
    {
        $this->assertSame('Kafka', TableEngine::KAFKA);
        $this->assertSame('MySQL', TableEngine::MYSQL);
        $this->assertSame('S3', TableEngine::S3);
        $this->assertSame('HDFS', TableEngine::HDFS);
    }
}
