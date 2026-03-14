<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Command\AbstractCommand;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Expression\Value\Param;
use Yiisoft\Db\Query\DataReaderInterface;
use Yiisoft\Db\Query\QueryInterface;
use Yiisoft\Db\QueryBuilder\QueryBuilderInterface;

use function array_column;
use function array_key_first;
use function curl_error;
use function curl_exec;
use function curl_getinfo;
use function curl_init;
use function curl_setopt_array;
use function json_decode;
use function preg_match;
use function reset;
use function urlencode;

use const CURLINFO_HTTP_CODE;
use const CURLOPT_HTTPHEADER;
use const CURLOPT_POST;
use const CURLOPT_POSTFIELDS;
use const CURLOPT_RETURNTRANSFER;
use const CURLOPT_URL;
use const CURLOPT_USERPWD;

/**
 * Implements a database command executed via the ClickHouse HTTP interface.
 *
 * ClickHouse characteristics:
 * - No traditional transactions
 * - INSERT is synchronous, UPDATE/DELETE use async ALTER TABLE mutations
 * - HTTP interface at port 8123
 */
final class Command extends AbstractCommand
{
    private ?string $rawResponse = null;
    private bool $forRead = false;

    public function prepare(?bool $forRead = null): void
    {
        $this->forRead = $forRead ?? false;
    }

    public function cancel(): void
    {
        $this->rawResponse = null;
        $this->forRead = false;
    }

    public function bindParam(
        int|string $name,
        mixed &$value,
        ?int $dataType = null,
        ?int $length = null,
        mixed $driverOptions = null,
    ): static {
        $this->params[$name] = new Param($value, $dataType ?? $this->db->getSchema()->getDataType($value));
        return $this;
    }

    public function bindValue(int|string $name, mixed $value, ?int $dataType = null): static
    {
        $this->params[$name] = new Param($value, $dataType ?? $this->db->getSchema()->getDataType($value));
        return $this;
    }

    public function bindValues(array $values): static
    {
        foreach ($values as $name => $value) {
            if ($value instanceof Param) {
                $this->params[$name] = $value;
            } else {
                $this->params[$name] = new Param($value, $this->db->getSchema()->getDataType($value));
            }
        }

        return $this;
    }

    public function insertReturningPks(string $table, array|QueryInterface $columns): array
    {
        $params = [];
        $insertSql = $this->db->getQueryBuilder()->insert($table, $columns, $params);
        $this->setSql($insertSql)->bindValues($params);
        $this->execute();

        return [];
    }

    /**
     * @throws NotSupportedException
     */
    public function upsertReturning(
        string $table,
        array|QueryInterface $insertColumns,
        array|bool $updateColumns = true,
        ?array $returnColumns = null,
    ): array {
        throw new NotSupportedException(__METHOD__ . ' is not supported by ClickHouse.');
    }

    public function showDatabases(): array
    {
        $sql = <<<SQL
        SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        SQL;

        return $this->setSql($sql)->queryColumn();
    }

    protected function getQueryBuilder(): QueryBuilderInterface
    {
        return $this->db->getQueryBuilder();
    }

    protected function internalExecute(): void
    {
        $sql = $this->getRawSql();

        if ($this->forRead && !preg_match('/\bFORMAT\s+\w+\s*$/i', $sql)) {
            $sql .= ' FORMAT JSON';
        }

        $driver = $this->getConnection()->getDriver();
        $url = $driver->getUrl() . '?database=' . urlencode($driver->getDatabase());

        $ch = curl_init();
        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => $sql,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_USERPWD => $driver->getUsername() . ':' . $driver->getPassword(),
            CURLOPT_HTTPHEADER => ['Content-Type: text/plain'],
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $curlError = curl_error($ch);

        if ($curlError !== '') {
            throw new Exception('ClickHouse connection error: ' . $curlError);
        }

        if ($httpCode !== 200) {
            throw new Exception('ClickHouse error [HTTP ' . $httpCode . ']: ' . $response);
        }

        $this->rawResponse = (string) $response;
    }

    protected function internalGetQueryResult(int $queryMode): mixed
    {
        if ($this->is($queryMode, self::QUERY_MODE_EXECUTE)) {
            return 0;
        }

        $decoded = json_decode((string) $this->rawResponse, true);
        $data = array_map(
            fn(array $row) => array_map(fn($v) => $v === null ? null : (string) $v, $row),
            $decoded['data'] ?? [],
        );

        if ($this->is($queryMode, self::QUERY_MODE_ALL)) {
            return $data;
        }

        if ($this->is($queryMode, self::QUERY_MODE_ROW)) {
            return $data[0] ?? null;
        }

        if ($this->is($queryMode, self::QUERY_MODE_COLUMN)) {
            if (empty($data)) {
                return [];
            }
            $firstKey = array_key_first($data[0]);
            return array_column($data, $firstKey);
        }

        if ($this->is($queryMode, self::QUERY_MODE_SCALAR)) {
            if (empty($data) || empty($data[0])) {
                return null;
            }
            return reset($data[0]);
        }

        if ($this->is($queryMode, self::QUERY_MODE_CURSOR)) {
            return new DataReader($data);
        }

        return null;
    }

    private function getConnection(): Connection
    {
        assert($this->db instanceof Connection);
        return $this->db;
    }
}
