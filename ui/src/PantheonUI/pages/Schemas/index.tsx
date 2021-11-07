import React from "react";

import { GetSchemas } from "../../data/schema";
import SchemasPresentational from "./Presentational";

export interface SchemasProps {
  catalogId: string;
  page: number;
}

export interface State {
  permissionsEditedSchemaId?: string;
}

const Schemas: React.SFC<SchemasProps> = ({ catalogId, page }) => {
  return (
    <GetSchemas catalogId={catalogId} page={page}>
      {(schemas, { loading }) => (
        <>
          {schemas && (
            <SchemasPresentational
              isLoading={loading}
              page={page}
              schemas={schemas}
            />
          )}
        </>
      )}
    </GetSchemas>
  );
};

export default Schemas;
