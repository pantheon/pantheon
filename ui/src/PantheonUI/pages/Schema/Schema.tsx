import { Editor } from "@contiamo/code-editor";
import { Tree } from "@contiamo/code-editor/lib/psl/psl-treeAnalyser";
import { Card, Chip, PageContent, styled, useOperationalContext } from "@operational/components";
import memoize from "lodash/memoize";
import React, { useEffect, useRef, useState } from "react";
import { Prompt } from "react-router";
import { GetMethod } from "restful-react";

import { CodeEditor, EditButton, EditSchemaActions } from "../../components";
import { PslSidenav } from "../../components/PslSidenav";
import { TableResponse } from "../../components/Query/types";
import { DataSource } from "../../data/dataSource";
import { Schema } from "../../data/schema";
import { customFetch } from "../../utils";

const Layout = styled("div")`
  display: grid;
  grid-template-columns: 1fr 315px;
  grid-column-gap: 2px; /* Avoid shadow cut on the header */
`;

export interface SchemaTabProps {
  schema: Schema;
  refetch: GetMethod<Schema>;
}

export type EditStatus = "readonly" | "editing" | "saving";

/**
 * Get datasources, in a memoize version.
 *
 * This fetch is shared between code-editor and sidenav to permit a better ux (less loading spinners)
 */
const getDataSources = memoize((catalogId: string) =>
  customFetch(`/catalogs/${catalogId}/dataSources?pageSize=100&page=1`)
    .then(res => res.json())
    .then(res => res.data.map((d: DataSource) => d.name)),
);

/**
 * Get datasource tables, in a memoize version.
 */
const getTables = memoize(
  (catalogId: string, dataSourceName: string) =>
    customFetch(`/catalogs/${catalogId}/dataSourcesByName/${dataSourceName}`)
      .then(res => res.json())
      .then((ds: DataSource) => {
        if (ds) {
          return customFetch(`/catalogs/${catalogId}/dataSources/${ds.id}/tables`)
            .then(res => res.json())
            .then((res: TableResponse[]) => res);
        } else {
          return Promise.reject();
        }
      }),
  (...args) => JSON.stringify(args), // Deal with memoize multiple args
);

export const SchemaTab: React.SFC<SchemaTabProps> = ({ schema, refetch }) => {
  const [editStatus, setEditStatus] = useState<EditStatus>("readonly");
  const [psl, setPsl] = useState(schema.psl);
  const [pslTree, setPslTree] = useState<{} | Tree>({});
  const [hasChanges, setHasChanges] = useState(false);
  const isReadOnly = editStatus === "readonly";
  const editor = useRef<Editor | null>(null);
  const { clearMessages } = useOperationalContext();

  useEffect(() => {
    // Clear the network cache on load
    if (getTables.cache.clear) {
      getTables.cache.clear();
    }
    if (getDataSources.cache.clear) {
      getDataSources.cache.clear();
    }
  }, [getTables, getDataSources]);

  useEffect(() => setHasChanges(psl !== schema.psl), [psl]);

  useEffect(() => {
    if (hasChanges) {
      const handleOnClose = (e: Event) => {
        e.returnValue = true;
      };
      window.addEventListener("beforeunload", handleOnClose);
      return () => window.removeEventListener("beforeunload", handleOnClose);
    }

    return;
  }, [hasChanges]);

  return (
    <PageContent fill padding="small">
      <Prompt
        message="You have unsaved changes in your PSL. Are you sure you would like to navigate away without saving?"
        when={hasChanges}
      />
      <Layout>
        <Card
          title={<>Schema PSL {isReadOnly && <Chip>view mode</Chip>}</>}
          {...{ style: { marginBottom: 0 } }}
          action={
            isReadOnly ? (
              <EditButton
                data-cy="pantheon--schema__edit-button"
                onClick={() => {
                  setEditStatus("editing");
                  if (editor.current) {
                    editor.current.focus();
                  }
                }}
              />
            ) : (
              <EditSchemaActions
                schema={schema}
                draftSchema={{ ...schema, psl }}
                onCancelClick={() => {
                  setEditStatus("readonly");
                  setPsl(schema.psl);
                  clearMessages();
                }}
                onSaveBegin={() => {
                  setEditStatus("saving");
                  clearMessages();
                }}
                onSaveSuccess={() => {
                  setEditStatus("readonly");
                  setHasChanges(false);
                  refetch();
                }}
                onSaveFailed={() => setEditStatus("editing")}
              />
            )
          }
        >
          <CodeEditor
            disabled={editStatus !== "editing"}
            height={windowHeight => windowHeight - 190 /* header size + pagecontent padding */}
            value={psl}
            onChange={setPsl}
            onPslTreeChange={setPslTree}
            codeEditorRef={e => (editor.current = e)}
            getPslDataSources={() => getDataSources(schema.catalogId)}
            getPslTables={(dataSourceName: string) => getTables(schema.catalogId, dataSourceName)}
          />
        </Card>
        <PslSidenav
          catalogId={schema.catalogId}
          tree={pslTree}
          onStructureNodeClick={startPosition => {
            if (editor.current) {
              editor.current.setPosition({ column: startPosition.column + 1, lineNumber: startPosition.lineNumber });
              editor.current.focus();
            }
          }}
          onInventoryNodeClick={textToInsert => {
            if (editor.current && editStatus === "editing") {
              editor.current.insertAtCurrentPosition(textToInsert);
              editor.current.focus();
            }
          }}
          getDataSources={() => getDataSources(schema.catalogId)}
          getTables={dataSourceName => getTables(schema.catalogId, dataSourceName)}
        />
      </Layout>
    </PageContent>
  );
};

export default SchemaTab;
