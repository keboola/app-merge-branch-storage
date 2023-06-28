<?php

declare(strict_types=1);

namespace Keboola\MergeBrancheStorage\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class SynchronizeConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode->ignoreExtraKeys();
        // @formatter:on
        return $parametersNode;
    }
}
