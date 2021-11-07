import { Message, Spinner, Tree } from "@operational/components";
import React, { useEffect, useState } from "react";
import { PslColor } from "../utils";
import { PslSidenavProps, TreeContainer } from "./PslSidenav";
import { PslSidenavSection } from "./PslSidenavSection";
import { ListSchemas } from "./queries";
import { TableResponse } from "./Query/types";

export type PslSidenavInventoryProps = Pick<
  PslSidenavProps,
  "onInventoryNodeClick" | "getDataSources" | "getTables" | "catalogId"
>;

interface DataSource {
  name: string;
  tables: TableResponse[] | null; // null is not loaded
}

export const PslSidenavInventory: React.FC<PslSidenavInventoryProps> = ({
  getDataSources,
  onInventoryNodeClick,
  getTables,
  catalogId,
}) => {
  const [dataSources, setDataSources] = useState<DataSource[] | null>(null);
  const [initiallyOpen, setInitiallyOpen] = useState("");

  useEffect(() => {
    // Retrieve schemas on the first load
    getDataSources().then(res => {
      setDataSources(res.map(r => ({ name: r, tables: null })));
    });
  }, []);

  return (
    <>
      <Message type="info" body="Use these assets in your schema to connect to the physical data." />
      <TreeContainer>
        <PslSidenavSection title="Available Schemas">
          <ListSchemas catalogId={catalogId}>
            {schemas =>
              schemas === null ? (
                <Spinner />
              ) : (
                <Tree
                  trees={schemas.data.map(schema => ({
                    label: schema.name,
                    tag: "S",
                    onClick() {
                      onInventoryNodeClick(schema.name);
                    },
                  }))}
                />
              )
            }
          </ListSchemas>
        </PslSidenavSection>
        <PslSidenavSection title="Available Data Sources">
          {dataSources === null ? (
            <Spinner />
          ) : (
            <Tree
              key={initiallyOpen} // Reset the tree only if initiallyOpen change
              trees={dataSources.map(ds => ({
                label: ds.name,
                initiallyOpen: initiallyOpen === ds.name,
                tag: "D",
                onClick() {
                  if (ds.tables === null) {
                    getTables(ds.name).then(tables => {
                      setDataSources(dataSources.map(i => (i.name === ds.name ? { ...i, tables } : i)));
                      setInitiallyOpen(ds.name);
                    });
                  }
                },
                childNodes:
                  ds.tables === null
                    ? [{ label: "Loading…" }]
                    : Array.isArray(ds.tables)
                    ? ds.tables.map(t => ({
                        label: t.name,
                        childNodes: t.columns.map(c => ({
                          label: c,
                          tag: "C",
                          color: PslColor.column,
                          onClick() {
                            onInventoryNodeClick(c);
                          },
                        })),
                      }))
                    : [{ label: "Error!" }],
              }))}
            />
          )}
        </PslSidenavSection>
      </TreeContainer>
    </>
  );
};
