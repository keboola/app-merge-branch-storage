<?php

declare(strict_types=1);

use Keboola\MergeBrancheStorage\Configuration\Config;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use PHPUnit\Framework\Assert;

return function (Client $client, string $testDir): void {
    $table = $client->getTable('in.c-testCreateBucket.testTable');

    Assert::assertCount(4, $table['columns']);
    Assert::assertEquals([
        'column1',
        'column2',
        'column3',
        'column4',
    ], $table['columns']);
};
