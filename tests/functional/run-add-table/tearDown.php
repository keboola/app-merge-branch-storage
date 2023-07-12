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
    $tables = $client->listTables('in.c-testCreateBucket');

    Assert::assertCount(2, $tables);

    foreach ($tables as $table) {
        switch ($table['name']) {
            case 'testTable':
                Assert::assertFalse($table['isTyped']);
                break;
            case 'testTableTypes':
                Assert::assertTrue($table['isTyped']);
                break;
            default:
                throw new Exception(sprintf('Unexpected table "%s"', $table['name']));
        }
    }
};
