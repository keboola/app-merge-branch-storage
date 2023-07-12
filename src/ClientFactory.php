<?php

declare(strict_types=1);

namespace Keboola\MergeBranchStorage;

use Keboola\MergeBranchStorage\Configuration\Config;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;

class ClientFactory
{
    public function __construct(readonly Config $config)
    {
    }

    public function getClient(): Client
    {
        $clientConfig = [
            'url' => $this->config->getEnvKbcUrl(),
            'token' => $this->config->getEnvKbcToken(),
        ];
        if (!empty($this->config->getEnvKbcBranchId())) {
            return new BranchAwareClient($this->config->getEnvKbcBranchId(), $clientConfig);
        }

        return new Client($clientConfig);
    }
}
