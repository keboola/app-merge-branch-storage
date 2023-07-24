<?php

declare(strict_types=1);

namespace Keboola\MergeBranchStorage\Actions;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Psr\Log\LoggerInterface;

class RunAction
{
    private const SKIP_COLUMN_METADATA_PROVIDER = ['system', 'storage'];

    public function __construct(readonly Client $client, readonly LoggerInterface $logger)
    {
    }

    public function addBucket(array $resources): void
    {
        foreach ($resources as $resource) {
            $bucketName = $resource['name'];
            $this->logger->info(sprintf('Creating bucket "%s"', $bucketName));
            if (str_starts_with($bucketName, 'c-')) {
                $bucketName = substr($bucketName, 2);
            }

            $this->client->createBucket(
                $bucketName,
                $resource['stage'],
                $resource['description'],
                $resource['backend'],
                $resource['displayName']
            );
            $this->logger->info(sprintf('Bucket "%s" created', $bucketName));
        }
    }

    public function addTable(array $resources, string $datadir): void
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

                $this->client->createTableDefinition($resource['bucket']['id'], $options);
            } else {
                $this->logger->info(sprintf('Creating table "%s"', $resource['name']));
                $options = [
                    'bucketId' => $resource['bucket']['id'],
                    'name' => $resource['name'],
                    'primaryKey' => isset($resource['primaryKey']) ? implode(',', $resource['primaryKey']) : null,
                    'distributionKey' => isset($resource['distributionKey']) ?
                        implode(',', $resource['distributionKey']) :
                        null,
                    'transactional' => $resource['transactional'] ?? false,
                    'columns' => $resource['columns'] ?? null,
                    'syntheticPrimaryKeyEnabled' => $resource['syntheticPrimaryKeyEnabled'] ?? null,
                ];

                $filename = $datadir . '/emptyTableFile.csv';
                $file = new CsvFile($filename);
                $file->writeRow($resource['columns']);
                $tableId = $this->client->createTableAsync(
                    $resource['bucket']['id'],
                    $resource['name'],
                    $file,
                    $options
                );

                $resource['id'] = $tableId;
                $this->editTableColumnsMetadata($resource);
            }
            $this->logger->info(sprintf('Table "%s" created', $resource['name']));
        }
    }

    public function addColumn(string $tableId, array $resources): void
    {
        $table = $this->client->getTable($tableId);

        foreach ($resources as $resource) {
            if ($table['isTyped'] === true) {
                $this->logger->info(sprintf(
                    'Creating column "%s" in table "%s"',
                    $resource['name'],
                    $table['name']
                ));
                $this->client->addTableColumn(
                    $tableId,
                    $resource['name'],
                    $resource['definition'],
                    $resource['basetype']
                );
                $this->logger->info(sprintf('Column "%s" created', $resource['name']));
            } else {
                $this->logger->info(sprintf(
                    'Creating column "%s" in table "%s"',
                    $resource,
                    $table['name']
                ));
                $this->client->addTableColumn($tableId, $resource);
                $this->logger->info(sprintf('Column "%s" created', $resource));
            }
        }
    }

    public function addPrimaryKeys(string $resourceId, array $resourceValues): void
    {
        $this->logger->info(sprintf('Creating primary keys for table "%s"', $resourceId));
        $this->client->createTablePrimaryKey($resourceId, array_map(fn($v) => trim($v), $resourceValues));
        $this->logger->info(sprintf('Primary keys for table "%s" created', $resourceId));
    }

    public function dropPrimaryKeys(array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->logger->info(sprintf('Dropping primary keys for table "%s"', $resourceValue));
            $this->client->removeTablePrimaryKey($resourceValue);
            $this->logger->info(sprintf('Primary keys for table "%s" dropped', $resourceValue));
        }
    }

    public function dropColumn(string $resourceId, array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->logger->info(sprintf('Dropping column "%s" from table "%s"', $resourceValue, $resourceId));
            $this->client->deleteTableColumn($resourceId, $resourceValue, ['force' => true]);
            $this->logger->info(sprintf('Column "%s" from table "%s" dropped', $resourceValue, $resourceId));
        }
    }

    public function dropTable(array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->logger->info(sprintf('Dropping table "%s"', $resourceValue));
            $this->client->dropTable($resourceValue, ['force' => true]);
            $this->logger->info(sprintf('Table "%s" dropped', $resourceValue));
        }
    }

    public function dropBucket(array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->logger->info(sprintf('Dropping bucket "%s"', $resourceValue));
            $this->client->dropBucket($resourceValue, ['force' => true, 'async' => true]);
            $this->logger->info(sprintf('Bucket "%s" dropped', $resourceValue));
        }
    }

    public function editTablesColumnsMetadata(array $resourceValues): void
    {
        foreach ($resourceValues as $resourceValue) {
            $this->editTableColumnsMetadata($resourceValue);
        }
    }

    private function editTableColumnsMetadata(array $resourceValue): void
    {
        if (empty($resourceValue['columnMetadata'])) {
            return;
        }

        $this->logger->info(sprintf('Editing columns metadata for table "%s"', $resourceValue['id']));
        $columnsMetadata = [];
        foreach ($resourceValue['columnMetadata'] as $column => $metadatas) {
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
                $resourceValue['id'],
                (string) $provider,
                null,
                $columnMetadata
            );
            $clientMetadata->postTableMetadataWithColumns($tableMetadataOptions);
        }
        $this->logger->info(sprintf('Columns metadata for table "%s" edited', $resourceValue['id']));
    }
}
