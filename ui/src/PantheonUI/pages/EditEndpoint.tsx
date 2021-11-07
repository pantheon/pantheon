import React from "react";
import { Get } from "restful-react";

import { Page, Progress } from "@operational/components";
import { Query } from "../components";
import { Endpoint } from "../data/endpoint";
import { GetSchemas, Schema } from "../data/schema";

export interface Props {
  catalogId: string;
  endpointId: string;
}

export interface State {
  isEditorUnmounted: boolean;
  schemas?: Schema[];
}

const EditEndpoint: React.SFC<Props> = ({ catalogId, endpointId }) => {
  return (
    <GetSchemas catalogId={catalogId}>
      {getResponse => (
        <Get<Endpoint> path={`/catalogs/${catalogId}/endpoints/${endpointId}`}>
          {endpoint =>
            endpoint ? (
              <Page fill title={`Endpoint: ${endpoint.name}`} noPadding>
                {getResponse ? (
                  <Query
                    catalogId={catalogId}
                    schemas={getResponse ? getResponse.data : []}
                    schemaId={endpoint ? endpoint.schemaId : ""}
                    endpoint={endpoint}
                  />
                ) : (
                  <Progress />
                )}
              </Page>
            ) : (
              <Progress />
            )
          }
        </Get>
      )}
    </GetSchemas>
  );
};

export default EditEndpoint;
