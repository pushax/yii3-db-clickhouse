<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse;

use Yiisoft\Db\Connection\ServerInfoInterface;

/**
 * Provides information about the ClickHouse server via HTTP interface.
 */
final class ServerInfo implements ServerInfoInterface
{
    private ?string $version = null;
    private ?string $timezone = null;

    public function __construct(private readonly Connection $db) {}

    public function getVersion(bool $refresh = false): string
    {
        if ($this->version === null || $refresh) {
            /** @var string */
            $this->version = (string) $this->db->createCommand('SELECT version()')->queryScalar();
        }

        return $this->version;
    }

    public function getTimezone(bool $refresh = false): string
    {
        if ($this->timezone === null || $refresh) {
            /** @var string */
            $this->timezone = (string) $this->db->createCommand('SELECT timezone()')->queryScalar();
        }

        return $this->timezone;
    }
}
