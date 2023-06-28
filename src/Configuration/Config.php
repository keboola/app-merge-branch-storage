<?php

declare(strict_types=1);

namespace Keboola\MergeBrancheStorage\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public const COMPONENT_ID = 'keboola.app-merge-branche-storage';

    public function getBranchId(): string
    {
        return $this->getStringValue(['parameters', 'branchId']);
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

    public function getRawResourceJson(): string
    {
        return $this->getStringValue(['parameters', 'rawResourceJson']);
    }
}
