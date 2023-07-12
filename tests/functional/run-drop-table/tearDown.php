<?php

declare(strict_types=1);

use Keboola\StorageApi\Client;
use PHPUnit\Framework\Assert;

return function (Client $client, string $testDir): void {
    $tables = $client->listTables('in.c-testCreateBucket');

    Assert::assertCount(0, $tables);
};
