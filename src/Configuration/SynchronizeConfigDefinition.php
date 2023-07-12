<?php

declare(strict_types=1);

namespace Keboola\MergeBranchStorage\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class SynchronizeConfigDefinition extends BaseConfigDefinition
{
    protected function getRootDefinition(TreeBuilder $treeBuilder): ArrayNodeDefinition
    {
        $rootNode = parent::getRootDefinition($treeBuilder);
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $rootNode
            ->children()
                ->scalarNode('configId')->isRequired()->cannotBeEmpty()->end()
            ->end()
        ;
        // @formatter:on
        return $rootNode;
    }
}
