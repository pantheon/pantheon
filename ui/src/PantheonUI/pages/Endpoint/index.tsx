import React from "react";

import { GetEndpoint } from "../../data/endpoint";
import EndpointPresentational from "./Presentational";

export interface EndpointComponentProps {
  catalogId: string;
  endpointId: string;
  tabName: string;
}

const EndpointComponent: React.SFC<EndpointComponentProps> = ({ catalogId, endpointId, tabName }) => (
  <GetEndpoint catalogId={catalogId} endpointId={endpointId}>
    {(endpoint, { loading }, { refetch }) =>
      endpoint && (
        <EndpointPresentational
          tabName={tabName}
          refetch={refetch}
          isLoading={loading}
          endpoint={endpoint}
          endpointId={endpointId}
          catalogId={catalogId}
        />
      )
    }
  </GetEndpoint>
);

export default EndpointComponent;
