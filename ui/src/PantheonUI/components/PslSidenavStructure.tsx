import { Tree } from "@contiamo/code-editor/lib/psl/psl-treeAnalyser";
import { Message, Tree as OpTree } from "@operational/components";
import React from "react";
import { PslColor } from "../utils";
import { PslSidenavProps, TreeContainer } from "./PslSidenav";
import { PslSidenavSection } from "./PslSidenavSection";

const isTree = (i: any): i is Tree => typeof i.measures === "object";

export type PslSidenavStructureProps = Pick<PslSidenavProps, "tree" | "onStructureNodeClick">;

export const PslSidenavStructure: React.SFC<PslSidenavStructureProps> = ({
  tree,
  onStructureNodeClick: onNodeClick,
}) => (
  <>
    <Message type="info" body="Get a quick overview of the structure of your schema below." />
    {isTree(tree) ? (
      <TreeContainer>
        <PslSidenavSection title="Measures">
          <OpTree
            trees={tree.measures.map(m => ({
              label: m.name,
              onClick: () => onNodeClick(m.startPosition),
              tag: "M",
              color: PslColor.measure,
              highlight: m.hasCursor,
              childNodes: [],
            }))}
          />
        </PslSidenavSection>
        <PslSidenavSection title="Dimensions">
          <OpTree
            key={Math.random() /* Recreate the tree to have the updated `initiallyOpen` */}
            trees={tree.dimensions.map(d => ({
              label: d.name,
              highlight: d.hasCursor,
              initiallyOpen: d.shouldBeOpen,
              childNodes: [
                ...d.attributes.map(a => ({
                  label: a.name,
                  tag: "A",
                  highlight: a.hasCursor,
                  color: PslColor.attribute,
                  onClick: () => onNodeClick(a.startPosition),
                  childNodes: [],
                })),
                ...d.levels.map(l => ({
                  label: l.name,
                  initiallyOpen: l.shouldBeOpen,
                  highlight: l.hasCursor,
                  tag: l.attributes.length === 0 ? "L" : undefined,
                  color: PslColor.level,
                  childNodes: l.attributes.map(a => ({
                    label: a.name,
                    highlight: a.hasCursor,
                    onClick: () => onNodeClick(a.startPosition),
                    tag: "A",
                    color: PslColor.attribute,
                    childNodes: [],
                  })),
                })),
                ...d.hierarchies.map(h => ({
                  label: h.name || "hierarchy (no name)",
                  highlight: h.hasCursor,
                  initiallyOpen: h.shouldBeOpen,
                  childNodes: h.levels.map(l => ({
                    label: l.name,
                    highlight: l.hasCursor,
                    initiallyOpen: l.shouldBeOpen,
                    onClick: () => onNodeClick(l.startPosition),
                    tag: l.attributes.length === 0 ? "L" : undefined,
                    color: PslColor.level,
                    childNodes: l.attributes.map(a => ({
                      label: a.name,
                      highlight: a.hasCursor,
                      tag: "A",
                      color: PslColor.attribute,
                      childNodes: [],
                    })),
                  })),
                })),
              ],
            }))}
          />
        </PslSidenavSection>
        <PslSidenavSection title="Schema tables">
          <OpTree
            key={Math.random() /* Recreate the tree to have the updated `initiallyOpen` */}
            trees={tree.tables.map(t => ({
              label: t.name,
              highlight: t.hasCursor,
              initiallyOpen: t.shouldBeOpen,
              tag: t.columns.length === 0 ? "T" : undefined,
              color: PslColor.table,
              childNodes: t.columns.map(c => ({
                label: c.name,
                highlight: c.hasCursor,
                tag: "C",
                color: PslColor.column,
                onClick: () => onNodeClick(c.startPosition),
                childNodes: [],
              })),
            }))}
          />
        </PslSidenavSection>
      </TreeContainer>
    ) : (
      "No schema"
    )}
  </>
);
