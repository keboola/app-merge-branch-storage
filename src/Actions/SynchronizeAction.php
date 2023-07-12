<?php

declare(strict_types=1);

namespace Keboola\MergeBranchStorage\Actions;

use Keboola\Component\UserException;
use Keboola\MergeBranchStorage\Configuration\Config;
use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;

class SynchronizeAction
{

    public function __construct(readonly Client $client, readonly LoggerInterface $logger)
    {
    }

    public function getRawResource(string $action, ?string $resourceId, array $resourceValues): ?array
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
        $tables = [];
        foreach ($resourceValues as $resourceValue) {
            array_map(fn($table) => $this->client->getTable($table), $resourceValues);
            $table = array_filter(
                $this->client->getTable($resourceValue),
                fn($key) => in_array($key, [
                    'isTyped',
                    'name',
                    'definition',
                    'distributionType',
                    'distributionKey',
                    'indexType',
                    'indexKey',
                    'bucket',
                    'primaryKey',
                    'transactional',
                    'columns',
                    'syntheticPrimaryKeyEnabled',
                    'columnMetadata',
                ]),
                ARRAY_FILTER_USE_KEY
            );

            $table['bucket'] = array_filter($table['bucket'], fn($key) => $key === 'id', ARRAY_FILTER_USE_KEY);

            $tables[] = $table;
        }
        return $tables;
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
}
