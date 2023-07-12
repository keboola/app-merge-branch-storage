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
    $components = new Components($client);

    $listComponentConfigurationsOptions = new ListComponentConfigurationsOptions();
    $listComponentConfigurationsOptions->setComponentId(Config::COMPONENT_ID);

    $listConfig = $components->listComponentConfigurations($listComponentConfigurationsOptions);

    foreach ($listConfig as $config) {
        $listConfigurationRowsOptions = new ListConfigurationRowsOptions();
        $listConfigurationRowsOptions
            ->setComponentId(Config::COMPONENT_ID)
            ->setConfigurationId($config['id'])
        ;

        $configRows = $components->listConfigurationRows($listConfigurationRowsOptions);
        foreach ($configRows as $configRow) {
            $params = $configRow['configuration']['parameters'];

            $filePath = sprintf(
                '%s/expected/data/out/storage/%s.json',
                $testDir,
                strtolower($params['action'])
            );

            if (!file_exists($filePath)) {
                throw new RuntimeException(sprintf('File "%s" not found!', $filePath));
            }

            Assert::assertEquals(
                json_decode((string) file_get_contents($filePath), true),
                $params['rawResourceJson']
            );
        }
    }
};
