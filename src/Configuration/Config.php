<?php

declare(strict_types=1);

namespace Keboola\MergeBrancheStorage\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public const COMPONENT_ID = 'keboola.app-merge-branche-storage';

    public const ACTION_ADD_BUCKET = 'ADD_BUCKET';

    public const ACTION_ADD_TABLE = 'ADD_TABLE';

    public const ACTION_ADD_COLUMN = 'ADD_COLUMN';

    public const ACTION_ADD_PRIMARY_KEY = 'ADD_PRIMARY_KEY';

    public const ACTION_DROP_BUCKET = 'DROP_BUCKET';

    public const ACTION_DROP_TABLE = 'DROP_TABLE';

    public const ACTION_DROP_COLUMN = 'DROP_COLUMN';

    public const ACTION_DROP_PRIMARY_KEY = 'DROP_PRIMARY_KEY';


    public function getConfigId(): string
    {
        $configId = self::getEnvKbcConfigId();
        if (!$configId) {
            $configId = $this->getStringValue(['configId']);
        }
        return $configId;
    }

    public function getResourceAction(): string
    {
        return $this->getStringValue(['parameters', 'action']);
    }

    public function getResourceId(): ?string
    {
        $value = $this->getStringValue(['parameters', 'resourceId'], false);
        if (!$value) {
            return null;
        }
        return $value;
    }

    public function getResourceValues(): array
    {
        return $this->getArrayValue(['parameters', 'values']);
    }

    public function getRawResourceJson(): array
    {
        return $this->getArrayValue(['parameters', 'rawResourceJson']);
    }
}
