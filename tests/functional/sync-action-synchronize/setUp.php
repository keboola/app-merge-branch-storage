<?php

declare(strict_types=1);

use Keboola\Csv\CsvFile;
use Keboola\MergeBrancheStorage\Configuration\Config;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;

return function (Client $client, string $testDir): string {
    $bucket = $client->createBucket('testCreateBucket', 'in');
    $table = $client->createTableAsync(
        $bucket,
        'testTable',
        new CsvFile($testDir . '/source/data/in/table.csv')
    );

    $components = new Components($client);

    $configuration = new Configuration();
    $configuration
        ->setComponentId(Config::COMPONENT_ID)
        ->setName('test')
    ;

    $newConfig = $components->addConfiguration($configuration);
    $configuration->setConfigurationId($newConfig['id']);

    $configurationRow = new ConfigurationRow($configuration);
    $configurationRow
        ->setConfiguration([
            'parameters' => [
                'action' => 'ADD_BUCKET',
                'values' => [
                    $bucket,
                ],
            ],
        ])
        ->setName('test add bucket')
    ;

    $components->addConfigurationRow($configurationRow);

    $configurationRow = new ConfigurationRow($configuration);
    $configurationRow
        ->setConfiguration([
            'parameters' => [
                'action' => 'ADD_TABLE',
                'values' => [
                    $table,
                ],
            ],
        ])
        ->setName('test add table')
    ;

    $components->addConfigurationRow($configurationRow);

    $configurationRow = new ConfigurationRow($configuration);
    $configurationRow
        ->setConfiguration([
            'parameters' => [
                'action' => 'ADD_COLUMN',
                'resourceId' => $table,
                'values' => [
                    'column1',
                ],
            ],
        ])
        ->setName('test add column')
    ;

    $components->addConfigurationRow($configurationRow);

    if (!is_string($configuration->getConfigurationId())) {
        throw new RuntimeException('Configuration ID must be string');
    }

    return (string) $configuration->getConfigurationId();
};
