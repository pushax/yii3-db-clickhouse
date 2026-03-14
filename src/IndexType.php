<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

/**
 * Defines the available data skipping index types for ClickHouse.
 *
 * @link https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes
 */
final class IndexType
{
    /**
     * Min/max index - stores min and max values of expression for each granule.
     */
    public const MIN_MAX = 'minmax';

    /**
     * Set index - stores unique values of expression for each granule.
     */
    public const SET = 'set';

    /**
     * Bloom filter index - probabilistic data structure for set membership testing.
     */
    public const BLOOM_FILTER = 'bloom_filter';

    /**
     * Token bloom filter for string columns tokenized by non-alphanumeric characters.
     */
    public const TOKEN_BLOOM_FILTER = 'tokenbf_v1';

    /**
     * N-gram bloom filter for string columns.
     */
    public const NGRAM_BLOOM_FILTER = 'ngrambf_v1';

    /**
     * Hypothesis index for approximate query processing.
     */
    public const HYPOTHESIS = 'hypothesis';

    /**
     * Annoy index for approximate nearest neighbor search.
     */
    public const ANNOY = 'annoy';
}
