<?php

declare(strict_types=1);

namespace Keboola\MergeBranchStorage;

use Exception;
use Keboola\Component\UserException;
use Keboola\Csv\CsvFile;
use Keboola\MergeBranchStorage\Actions\RunAction;
use Keboola\MergeBranchStorage\Actions\SynchronizeAction;
use Keboola\MergeBranchStorage\Configuration\Config;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Psr\Log\LoggerInterface;

class Application
{

    public function __construct(readonly Client $client, readonly LoggerInterface $logger, readonly string $datadir)
    {
    }

    public function run(Config $config): void
    {
        $runAction = new RunAction($this->client, $this->logger);
        switch ($config->getResourceAction()) {
            case Config::ACTION_ADD_BUCKET:
                $runAction->addBucket($config->getRawResourceJson());
                break;
            case Config::ACTION_ADD_TABLE:
                $runAction->addTable($config->getRawResourceJson(), $this->datadir);
                break;
            case Config::ACTION_ADD_COLUMN:
                if (!$config->getResourceId()) {
                    throw new UserException('Missing resourceId.');
                }
                $runAction->addColumn($config->getResourceId(), $config->getRawResourceJson());
                break;
            case Config::ACTION_ADD_PRIMARY_KEY:
                if (!$config->getResourceId()) {
                    throw new UserException('Missing resourceId.');
                }
                $runAction->addPrimaryKeys($config->getResourceId(), $config->getResourceValues());
                break;
            case Config::ACTION_DROP_BUCKET:
                $runAction->dropBucket($config->getResourceValues());
                break;
            case Config::ACTION_DROP_TABLE:
                $runAction->dropTable($config->getResourceValues());
                break;
            case Config::ACTION_DROP_COLUMN:
                if (!$config->getResourceId()) {
                    throw new UserException('Missing resourceId.');
                }
                $runAction->dropColumn($config->getResourceId(), $config->getResourceValues());
                break;
            case Config::ACTION_DROP_PRIMARY_KEY:
                $runAction->dropPrimaryKeys($config->getResourceValues());
                break;
            case Config::ACTION_EDIT_COLUMNS_METADATA:
                $runAction->editTablesColumnsMetadata($config->getRawResourceJson());
                break;
            default:
                throw new Exception(sprintf('Unknown action "%s"', $config->getResourceAction()));
        }

        $this->disableConfigurationRow($config);
    }

    public function synchronizeResources(string $configId): void
    {
        $components = new Components($this->client);

        $listConfigurationRowsOptions = new ListConfigurationRowsOptions();
        $listConfigurationRowsOptions->setComponentId(Config::COMPONENT_ID);
        $listConfigurationRowsOptions->setConfigurationId($configId);

        $configurationRows = $components->listConfigurationRows($listConfigurationRowsOptions);
        foreach ($configurationRows as $configurationRow) {
            if ($configurationRow['isDisabled']) {
                continue;
            }
            $configuration = $configurationRow['configuration'];

            $action = $configuration['parameters']['action'];
            $resourceId = $configuration['parameters']['resourceId'] ?? null;
            $resourceValues = $configuration['parameters']['values'];

            try {
                $synchronizeAction = new SynchronizeAction($this->client, $this->logger);
                $rawResource = $synchronizeAction->getRawResource($action, $resourceId, $resourceValues);
            } catch (ClientException $e) {
                throw new UserException(sprintf(
                    'Failed synchronizing config row "%s" (%s) with error message: %s.',
                    $configurationRow['name'],
                    $configurationRow['id'],
                    $e->getMessage()
                ));
            }
            $configuration['parameters']['rawResourceJson'] = $rawResource ?? [];

            $configOptions = new Configuration();
            $configOptions->setComponentId(Config::COMPONENT_ID);
            $configOptions->setConfigurationId($configId);

            $configRowOptions = new ConfigurationRow($configOptions);
            $configRowOptions->setRowId($configurationRow['id']);
            $configRowOptions->setConfiguration($configuration);

            $components->updateConfigurationRow($configRowOptions);
        }
    }

    private function disableConfigurationRow(Config $config): void
    {
        $this->logger->info(sprintf('Disabling configuration row "%s"', $config->getEnvKbcConfigRowId()));
        $components = new Components($this->client);
        $configOptions = new Configuration();
        $configOptions
            ->setComponentId(Config::COMPONENT_ID)
            ->setConfigurationId($config->getEnvKbcConfigId())
        ;
        $configRowOptions = new ConfigurationRow($configOptions);
        $configRowOptions
            ->setRowId($config->getEnvKbcConfigRowId())
            ->setIsDisabled(true)
        ;
        $components->updateConfigurationRow($configRowOptions);
        $this->logger->info(sprintf('Configuration row "%s" disabled', $config->getEnvKbcConfigRowId()));
    }
}
