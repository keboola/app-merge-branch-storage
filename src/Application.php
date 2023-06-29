<?php

declare(strict_types=1);

namespace Keboola\MergeBrancheStorage;

use Exception;
use Keboola\Component\UserException;
use Keboola\Csv\CsvFile;
use Keboola\MergeBrancheStorage\Configuration\Config;
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
    private const SKIP_COLUMN_METADATA_PROVIDER = ['system', 'storage'];

    public function __construct(readonly Client $client, readonly LoggerInterface $logger, readonly string $datadir)
    {
    }

    public function run(Config $config): void
    {
        switch ($config->getResourceAction()) {
            case Config::ACTION_ADD_BUCKET:
                $resources = (array) json_decode($config->getRawResourceJson(), true);
                $this->addBucket($resources);
                break;
            case Config::ACTION_ADD_TABLE:
                $resources = (array) json_decode($config->getRawResourceJson(), true);
                $this->addTable($resources);
                break;
            case Config::ACTION_ADD_COLUMN:
                if (!$config->getResourceId()) {
                    throw new UserException('Missing resourceId.');
                }
                $resources = (array) json_decode($config->getRawResourceJson(), true);
                $this->addColumn($config->getResourceId(), $resources);
                break;
            case Config::ACTION_ADD_PRIMARY_KEY:
                if (!$config->getResourceId()) {
                    throw new UserException('Missing resourceId.');
                }
                $this->addPrimaryKeys($config->getResourceId(), $config->getResourceValues());
                break;
            case Config::ACTION_DROP_BUCKET:
                $this->dropBucket($config->getResourceValues());
                break;
            case Config::ACTION_DROP_TABLE:
                $this->dropTable($config->getResourceValues());
                break;
            case Config::ACTION_DROP_COLUMN:
                if (!$config->getResourceId()) {
                    throw new UserException('Missing resourceId.');
                }
                $this->dropColumn($config->getResourceId(), $config->getResourceValues());
                break;
            case Config::ACTION_DROP_PRIMARY_KEY:
                $this->dropPrimaryKeys($config->getResourceValues());
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
            $configuration = $configurationRow['configuration'];

            $action = $configuration['parameters']['action'];
            $resourceId = $configuration['parameters']['resourceId'] ?? null;
            $resourceValues = $configuration['parameters']['values'];

            $rawResource = $this->getRawResource($action, $resourceId, $resourceValues);
            $configuration['parameters']['rawResourceJson'] = $rawResource ? json_encode($rawResource) : null;

            $configOptions = new Configuration();
            $configOptions->setComponentId(Config::COMPONENT_ID);
            $configOptions->setConfigurationId($configId);

            $configRowOptions = new ConfigurationRow($configOptions);
            $configRowOptions->setRowId($configurationRow['id']);
            $configRowOptions->setConfiguration($configuration);

            $components->updateConfigurationRow($configRowOptions);
        }
    }

    private function getRawResource(string $action, ?string $resourceId, array $resourceValues): ?array
    {
        switch ($action) {
            case Config::ACTION_ADD_BUCKET:
                return $this->listBuckets($resourceValues);
            case Config::ACTION_ADD_TABLE:
                return $this->listBucketTables($resourceValues);
            case Config::ACTION_ADD_COLUMN:
                if (!$resourceId) {
                    throw new UserException('Missing resourceId.');
                }
                return $this->listTableColumns($resourceId, $resourceValues);
            default:
                return null;
        }
    }

    private function listBuckets(array $resourceValues): array
    {
        $buckets = [];
        foreach ($resourceValues as $resourceValue) {
            $buckets[] = array_filter(
                $this->client->getBucket($resourceValue),
                fn($key) => in_array($key, ['id', 'name', 'stage', 'displayName', 'description', 'backend']),
                ARRAY_FILTER_USE_KEY
            );
        }
        return $buckets;
    }

    private function listBucketTables(array $resourceValues): array
    {
        return array_map(fn($table) => $this->client->getTable($table), $resourceValues);
    }

    private function listTableColumns(string $resourceId, array $resourceValues): array
    {
        $table = $this->client->getTable($resourceId);

        if ($table['isTyped'] === false) {
            return $resourceValues;
        } else {
            $filteredColumns = array_filter(
                $table['definition']['columns'],
                fn($column) => in_array($column['name'], $resourceValues)
            );

            return array_values($filteredColumns);
        }
    }

    private function addBucket(array $resources): void
    {
        foreach ($resources as $resource) {
            $bucketName = $resource['name'];
            $this->logger->info(sprintf('Creating bucket "%s"', $bucketName));
            if (str_starts_with($bucketName, 'c-')) {
                $bucketName = substr($bucketName, 2);
            }

            try {
                $this->client->createBucket(
                    $bucketName,
                    $resource['stage'],
                    $resource['description'],
                    $resource['backend'],
                    $resource['displayName']
                );
            } catch (ClientException $e) {
                $this->logger->warning(sprintf('Bucket "%s" already exists', $resource['name']));
            }
            $this->logger->info(sprintf('Bucket "%s" created', $bucketName));
        }
    }

    private function addTable(array $resources): void
    {
        foreach ($resources as $resource) {
            if ($resource['isTyped'] === true) {
                $this->logger->info(sprintf('Creating typed table "%s"', $resource['name']));
                $columns = array_map(function ($v) {
                    return array_filter($v, fn($key) => $key !== 'canBeFiltered', ARRAY_FILTER_USE_KEY);
                }, $resource['definition']['columns']);

                $options = [
                    'name' => $resource['name'],
                    'primaryKeysNames' => $resource['definition']['primaryKeysNames'] ?? [],
                    'columns' => $columns,
                ];

                if (!is_null($resource['distributionType'])) {
                    $options['distribution'] = [
                        'type' => $resource['distributionType'],
                        'distributionColumnsNames' => $resource['distributionKey'],
                    ];
                }

                if (!is_null($resource['indexType'])) {
                    $options['index'] = [
                        'type' => $resource['indexType'],
                        'indexColumnsNames' => $resource['indexKey'],
                    ];
                }

                try {
                    $this->client->createTableDefinition($resource['bucket']['id'], $options);
                } catch (ClientException $e) {
                    $this->logger->warning($e->getMessage());
                }
            } else {
                $this->logger->info(sprintf('Creating table "%s"', $resource['name']));
                $options = [
                    'bucketId' => $resource['bucket']['id'],
                    'name' => $resource['name'],
                    'primaryKey' => isset($resource['primaryKey']) ? implode(', ', $resource['primaryKey']) : null,
                    'distributionKey' => isset($resource['distributionKey']) ?
                        implode(', ', $resource['distributionKey']) :
                        null,
                    'transactional' => $resource['transactional'] ?? false,
                    'columns' => $resource['columns'] ?? null,
                    'syntheticPrimaryKeyEnabled' => $resource['syntheticPrimaryKeyEnabled'] ?? null,
                ];

                $filename = $this->datadir . '/emptyTableFile.csv';
                $file = new CsvFile($filename);
                $file->writeRow($resource['columns']);
                try {
                    $tableId = $this->client->createTableAsync(
                        $resource['bucket']['id'],
                        $resource['name'],
                        $file,
                        $options
                    );
                } catch (ClientException $e) {
                    $this->logger->warning($e->getMessage());
                    continue;
                } finally {
                    unlink($filename);
                }

                if (!empty($resource['columnMetadata'])) {
                    $columnsMetadata = [];
                    foreach ($resource['columnMetadata'] as $column => $metadatas) {
                        foreach ($metadatas as $metadata) {
                            if (in_array($metadata['provider'], self::SKIP_COLUMN_METADATA_PROVIDER)) {
                                continue;
                            }
                            $columnsMetadata[$metadata['provider']][$column][] = [
                                'key' => $metadata['key'],
                                'value' => $metadata['value'],
                            ];
                        }
                    }

                    $clientMetadata = new Metadata($this->client);
                    foreach ($columnsMetadata as $provider => $columnMetadata) {
                        $tableMetadataOptions = new TableMetadataUpdateOptions(
                            $tableId,
                            (string) $provider,
                            null,
                            $columnMetadata
                        );
                        $clientMetadata->postTableMetadataWithColumns($tableMetadataOptions);
                    }
                }
            }
            $this->logger->info(sprintf('Table "%s" created', $resource['name']));
        }
    }

    private function addColumn(string $tableId, array $resources): void
    {
        $table = $this->client->getTable($tableId);

        foreach ($resources as $resource) {
            $this->logger->info(sprintf(
                'Creating column "%s" in table "%s"',
                $resource['name'],
                $table['name']
            ));
            try {
                if ($table['isTyped'] === true) {
                    $this->client->addTableColumn(
                        $tableId,
                        $resource['name'],
                        $resource['definition'],
                        $resource['basetype']
                    );
                } else {
                    $this->client->addTableColumn($tableId, $resource);
                }
            } catch (ClientException $e) {
                $this->logger->warning(sprintf('Column "%s" already exists', $resource['name']));
            }
            $this->logger->info(sprintf('Column "%s" created', $resource['name']));
        }
    }

    private function addPrimaryKeys(string $resourceId, array $resourceValues): void
    {
        $this->logger->info(sprintf('Creating primary keys for table "%s"', $resourceId));
        try {
            $this->client->createTablePrimaryKey($resourceId, $resourceValues);
        } catch (ClientException $e) {
            $this->logger->warning($e->getMessage());
        }
        $this->logger->info(sprintf('Primary keys for table "%s" created', $resourceId));
    }

    private function dropPrimaryKeys(array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->logger->info(sprintf('Dropping primary keys for table "%s"', $resourceValue));
            try {
                $this->client->removeTablePrimaryKey($resourceValue);
            } catch (ClientException $e) {
                $this->logger->warning($e->getMessage());
            }
            $this->logger->info(sprintf('Primary keys for table "%s" dropped', $resourceValue));
        }
    }

    private function dropColumn(string $resourceId, array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->logger->info(sprintf('Dropping column "%s" from table "%s"', $resourceValue, $resourceId));
            try {
                $this->client->deleteTableColumn($resourceId, $resourceValue, ['force' => true]);
            } catch (ClientException $e) {
                $this->logger->warning($e->getMessage());
            }
            $this->logger->info(sprintf('Column "%s" from table "%s" dropped', $resourceValue, $resourceId));
        }
    }

    private function dropTable(array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->logger->info(sprintf('Dropping table "%s"', $resourceValue));
            try {
                $this->client->dropTable($resourceValue, ['force' => true]);
            } catch (ClientException $e) {
                $this->logger->warning($e->getMessage());
            }
            $this->logger->info(sprintf('Table "%s" dropped', $resourceValue));
        }
    }

    private function dropBucket(array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->logger->info(sprintf('Dropping bucket "%s"', $resourceValue));
            try {
                $this->client->dropBucket($resourceValue, ['force' => true, 'async' => true]);
            } catch (ClientException $e) {
                $this->logger->warning($e->getMessage());
            }
            $this->logger->info(sprintf('Bucket "%s" dropped', $resourceValue));
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
