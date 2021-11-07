import React from "react";
import { Get } from "restful-react";

import { Query } from "../components";
import { getConfig } from "../components/Config";
import { Paginated } from "../data/paginated";
import { Schema } from "../data/schema";

const QueryEditor = React.lazy(() => import("../components/QueryEditor"));

export interface QuerySchemaProps {
  catalogId: string;
  schemaId: string;
}

export const QuerySchema: React.SFC<QuerySchemaProps> = ({ catalogId, schemaId }) => (
  <Get<Paginated<Schema[]>> path={`/catalogs/${catalogId}/schemas`}>
    {getResponse =>
      getConfig("useNewQueryEditor") ? (
        <QueryEditor
          catalogId={catalogId}
          schemas={getResponse ? getResponse.data : []}
          schemaId={schemaId}
          key={schemaId}
        />
      ) : (
        <Query catalogId={catalogId} schemas={getResponse ? getResponse.data : []} schemaId={schemaId} />
      )
    }
  </Get>
);

export default QuerySchema;
