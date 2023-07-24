<?php

declare(strict_types=1);

use Keboola\MergeBranchStorage\Configuration\Config;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use PHPUnit\Framework\Assert;

return function (Client $client, string $testDir): void {
    $table = $client->getTable('in.c-testCreateBucket.testTable');

    Assert::assertCount(2, $table['columnMetadata']);
    Assert::assertArrayHasKey('column1', $table['columnMetadata']);
    Assert::assertArrayHasKey('column2', $table['columnMetadata']);
};
