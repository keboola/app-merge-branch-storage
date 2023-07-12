<?php

declare(strict_types=1);

use Keboola\StorageApi\Client;
use PHPUnit\Framework\Assert;

return function (Client $client, string $testDir): void {
    $buckets = $client->listBuckets();

    Assert::assertCount(0, $buckets);
};
