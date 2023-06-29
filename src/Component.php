<?php

declare(strict_types=1);

namespace Keboola\MergeBrancheStorage;

use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;
use Keboola\MergeBrancheStorage\Configuration\Config;
use Keboola\MergeBrancheStorage\Configuration\ConfigDefinition;
use Keboola\MergeBrancheStorage\Configuration\SynchronizeConfigDefinition;

class Component extends BaseComponent
{
    private const ACTION_SYNCHRONIZE_RESOURCES = 'synchronize_resources';

    protected function run(): void
    {
        $this->getLogger()->info('Starting application.');
        $clientFactory = new ClientFactory($this->getConfig());
        $application = new Application($clientFactory->getClient(), $this->getLogger(), $this->getDataDir());
        $this->getLogger()->info('Application started.');
        $application->run($this->getConfig());
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
