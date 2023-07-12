<?php

declare(strict_types=1);

namespace Keboola\MergeBranchStorage;

use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;
use Keboola\MergeBranchStorage\Configuration\Config;
use Keboola\MergeBranchStorage\Configuration\ConfigDefinition;
use Keboola\MergeBranchStorage\Configuration\SynchronizeConfigDefinition;
use Keboola\StorageApi\ClientException;

class Component extends BaseComponent
{
    private const ACTION_SYNCHRONIZE_RESOURCES = 'synchronize_resources';

    protected function run(): void
    {
        $clientFactory = new ClientFactory($this->getConfig());
        $application = new Application($clientFactory->getClient(), $this->getLogger(), $this->getDataDir());
        try {
            $application->run($this->getConfig());
        } catch (ClientException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function runSynchronizeResources(): array
    {
        $clientFactory = new ClientFactory($this->getConfig());
        $application = new Application($clientFactory->getClient(), $this->getLogger(), $this->getDataDir());

        try {
            $application->synchronizeResources($this->getConfig()->getConfigId());
        } catch (UserException $e) {
            return [
                'status' => 'error',
                'message' => $e->getMessage(),
            ];
        }

        return [
            'status' => 'success',
            'message' => 'Synchronized',
        ];
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        $rawConfig = $this->getRawConfig();
        $action = $rawConfig['action'] ?? 'run';
        if ($action === self::ACTION_SYNCHRONIZE_RESOURCES) {
            return SynchronizeConfigDefinition::class;
        }
        return ConfigDefinition::class;
    }

    protected function getSyncActions(): array
    {
        return [
            self::ACTION_SYNCHRONIZE_RESOURCES => 'runSynchronizeResources',
        ];
    }
}
