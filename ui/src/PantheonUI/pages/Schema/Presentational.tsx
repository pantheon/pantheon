import { Button, Page, Progress } from "@operational/components";
import { OperationalRouter } from "@operational/router";
import { kebab, title } from "case";
import React, { Suspense } from "react";
import { GetMethod } from "restful-react";

import { GetSchemaData, Schema } from "../../data/schema";

const SchemaTab = React.lazy(() => import(/* webpackChunkName: "schema-schema" */ "./Schema"));
const SettingsTab = React.lazy(() => import(/* webpackChunkName: "schema-settings" */ "./Settings"));

export interface SchemaPresentationalProps {
  schema: GetSchemaData;
  tabName: string;
  refetch: GetMethod<Schema>;
}

const SchemaPresentational: React.FC<SchemaPresentationalProps> = ({ schema, tabName, refetch }) => (
  <OperationalRouter>
    {({ pushState }) =>
      schema && (
        <Page
          title={`Schema: ${schema ? schema.name : "-"}`}
          condensedTitle
          activeTabName={title(tabName)}
          onTabChange={name => pushState(`schemas/${schema.id}/${kebab(name)}`)}
          actions={
            <Button
              data-cy="pantheon--schema__query-editor-button"
              condensed
              icon="Document"
              color="ghost"
              onClick={() => pushState(`schemas/${schema.id}/query`)}
            >
              Query Editor
            </Button>
          }
          tabs={[
            {
              name: "Schema",
              children: schema && (
                <Suspense fallback={<Progress />}>
                  <SchemaTab schema={schema} refetch={refetch} />
                </Suspense>
              ),
            },
            {
              name: "Settings",
              children: schema && (
                <Suspense fallback={<Progress />}>
                  <SettingsTab schema={schema} refetch={refetch} />
                </Suspense>
              ),
            },
          ]}
        />
      )
    }
  </OperationalRouter>
);

export default SchemaPresentational;
