<?php

declare(strict_types=1);

namespace Keboola\MergeBrancheStorage\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys()
            ->children()
                ->enumNode('action')->values([
                    'ADD_BUCKET',
                    'ADD_TABLE',
                    'ADD_COLUMN',
                    'ADD_PRIMARY_KEY',
                    'DROP_BUCKET',
                    'DROP_TABLE',
                    'DROP_COLUMN',
                    'DROP_PRIMARY_KEY',
                ])->end()
                ->scalarNode('resourceId')->defaultNull()->end()
                ->arrayNode('values')
                    ->scalarPrototype()->end()
                ->end()
                ->scalarNode('rawResourceJson')->isRequired()->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
