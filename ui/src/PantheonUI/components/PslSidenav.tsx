import { Node, Tree } from "@contiamo/code-editor/lib/psl/psl-treeAnalyser";
import { styled, useURLState } from "@operational/components";
import React from "react";
import NakedCard from "./NakedCard";
import { PslSidenavInventory } from "./PslSidenavInventory";
import { PslSidenavStructure } from "./PslSidenavStructure";
import { TableResponse } from "./Query/types";

export const TreeContainer = styled("div")`
  margin: -${props => props.theme.space.medium}px;
`;

export interface PslSidenavProps {
  tree: Tree | {};
  onStructureNodeClick: (startPosition: Node["startPosition"]) => void;
  onInventoryNodeClick: (textToInsert: string) => void;
  getDataSources: () => Promise<string[]>;
  getTables: (dataSourceName: string) => Promise<TableResponse[]>;
  catalogId: string;
}

export const PslSidenav: React.FC<PslSidenavProps> = ({
  tree,
  onStructureNodeClick,
  onInventoryNodeClick,
  getDataSources,
  getTables,
  catalogId,
}) => {
  const [tabName, setTabName] = useURLState("tab", "structure", val =>
    ["structure", "inventory"].includes(val) ? val : undefined,
  );

  return (
    <NakedCard
      fullHeight
      activeTabName={tabName}
      onTabChange={setTabName}
      tabs={[
        {
          name: "structure",
          children: <PslSidenavStructure tree={tree} onStructureNodeClick={onStructureNodeClick} />,
        },
        {
          name: "inventory",
          children: (
            <PslSidenavInventory
              catalogId={catalogId}
              onInventoryNodeClick={onInventoryNodeClick}
              getDataSources={getDataSources}
              getTables={getTables}
            />
          ),
        },
      ]}
    />
  );
};
