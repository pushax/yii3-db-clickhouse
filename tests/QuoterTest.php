<?php

declare(strict_types=1);

namespace Pushax\Db\ClickHouse\Tests;

use PHPUnit\Framework\TestCase;
use Pushax\Db\ClickHouse\Quoter;

final class QuoterTest extends TestCase
{
    private Quoter $quoter;

    protected function setUp(): void
    {
        $this->quoter = new Quoter('`', '`');
    }

    public function testQuoteSimpleValue(): void
    {
        $this->assertSame("'hello'", $this->quoter->quoteValue('hello'));
    }

    public function testQuoteEmptyString(): void
    {
        $this->assertSame("''", $this->quoter->quoteValue(''));
    }

    public function testQuoteValueEscapesSingleQuote(): void
    {
        $this->assertSame("'it\\'s'", $this->quoter->quoteValue("it's"));
    }

    public function testQuoteValueEscapesBackslash(): void
    {
        $this->assertSame("'C:\\\\path'", $this->quoter->quoteValue('C:\\path'));
    }

    public function testQuoteValueEscapesBothSpecialChars(): void
    {
        $this->assertSame("'it\\'s a \\\\path'", $this->quoter->quoteValue("it's a \\path"));
    }

    public function testQuoteColumnName(): void
    {
        $this->assertSame('`user_id`', $this->quoter->quoteColumnName('user_id'));
    }

    public function testQuoteTableName(): void
    {
        $this->assertSame('`events`', $this->quoter->quoteTableName('events'));
    }
}
