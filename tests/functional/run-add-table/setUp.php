<?php

declare(strict_types=1);

use Keboola\Csv\CsvFile;
use Keboola\MergeBrancheStorage\Configuration\Config;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;

return function (Client $client, string $testDir): void {
    $client->createBucket('testCreateBucket', 'in');

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
        ->setConfiguration(
            json_decode(
                (string) file_get_contents($testDir . '/source/data/config.json'),
                true
            )
        )
        ->setName('test add table')
    ;

    $configRow = $components->addConfigurationRow($configurationRow);

    if (!is_string($configuration->getConfigurationId())) {
        throw new RuntimeException('Configuration ID must be string');
    }

    putenv(sprintf('KBC_CONFIGID=%s', $configuration->getConfigurationId()));
    putenv(sprintf('KBC_CONFIGROWID=%s', $configRow['id']));
};
