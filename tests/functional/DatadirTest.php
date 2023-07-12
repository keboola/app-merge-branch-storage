<?php

declare(strict_types=1);

namespace Keboola\MergeBrancheStorage\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\MergeBrancheStorage\Configuration\Config;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use RuntimeException;
use Symfony\Component\Process\Process;

class DatadirTest extends DatadirTestCase
{
    protected string $testProjectDir;

    protected function assertMatchesSpecification(
        DatadirTestSpecificationInterface $specification,
        Process $runProcess,
        string $tempDatadir
    ): void {
        if ($specification->getExpectedReturnCode() !== null) {
            $this->assertProcessReturnCode($specification->getExpectedReturnCode(), $runProcess);
        } else {
            $this->assertNotSame(0, $runProcess->getExitCode(), 'Exit code should have been non-zero');
        }
        if ($specification->getExpectedStdout() !== null) {
            $this->assertStringMatchesFormat(
                trim($specification->getExpectedStdout()),
                trim($runProcess->getOutput()),
                'Failed asserting stdout output'
            );
        }
        if ($specification->getExpectedStderr() !== null) {
            $this->assertStringMatchesFormat(
                trim($specification->getExpectedStderr()),
                trim($runProcess->getErrorOutput()),
                'Failed asserting stderr output'
            );
        }
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();

        $apiClient = $this->getStorageApiClient();
        $this->cleanupProject($apiClient);

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $configId = $initCallback($apiClient, $this->testProjectDir);
            putenv(sprintf('CONFIG_ID=%s', $configId));
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $apiClient = $this->getStorageApiClient();

        $setUpPhpFile = $this->testProjectDir . '/tearDown.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }
            $initCallback($apiClient, $this->testProjectDir);
        }

        $this->cleanupProject($apiClient);
    }

    private function getStorageApiClient(): Client
    {
        return new Client([
            'url' => (string) getenv('KBC_URL'),
            'token' => (string) getenv('KBC_TOKEN'),
        ]);
    }

    private function cleanupProject(Client $client): void
    {
        foreach ($client->listBuckets() as $bucket) {
            $client->dropBucket($bucket['id'], ['force' => true, 'async' => true]);
        }

        $components = new Components($client);
        $listConfigurations = new ListComponentConfigurationsOptions();
        $listConfigurations->setComponentId(Config::COMPONENT_ID);

        foreach ($components->listComponentConfigurations($listConfigurations) as $configuration) {
            $components->deleteConfiguration(Config::COMPONENT_ID, $configuration['id']);
        }
    }
}
