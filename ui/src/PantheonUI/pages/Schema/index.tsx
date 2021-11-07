import React from "react";
import { GetSchema } from "../../data/schema";
import SchemaPresentational from "./Presentational";

export interface SchemaPageProps {
  catalogId: string;
  schemaId: string;
  tabName: string; // from react-router
}

export const SchemaPage: React.SFC<SchemaPageProps> = ({ catalogId, schemaId, tabName }) => (
  <GetSchema catalogId={catalogId} schemaId={schemaId}>
    {(schema, _, { refetch }) => schema && <SchemaPresentational schema={schema} refetch={refetch} tabName={tabName} />}
  </GetSchema>
);

export default SchemaPage;
