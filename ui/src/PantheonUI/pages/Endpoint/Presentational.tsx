import { Page } from "@operational/components";
import { OperationalRouter } from "@operational/router";
import { kebab, title } from "case";
import React from "react";
import { GetMethod } from "restful-react";
import { Endpoint } from "../../data/endpoint";

const Overview = React.lazy(() => import(/* webpackChunkName: "endpoint-overview"*/ "./Overview"));
const TestEndpoint = React.lazy(() => import(/* webpackChunkName: "endpoint-test-connection"*/ "./TestEndpoint"));

export interface EndpointPresentationalProps {
  endpointId: string;
  refetch: GetMethod<Endpoint>;
  catalogId: string;
  tabName: string;
  endpoint: Endpoint;
  isLoading: boolean;
}

const EndpointPresentational: React.FC<EndpointPresentationalProps> = ({
  endpointId,
  refetch,
  catalogId,
  tabName,
  endpoint,
  isLoading,
}) => (
  <OperationalRouter>
    {({ pushState }) => (
      <Page
        condensedTitle
        onTabChange={name => pushState(`endpoints/${endpointId}/${kebab(name)}`)}
        activeTabName={title(tabName)}
        title={`Endpoint${endpoint ? `: ${endpoint.name}` : ""}`}
        loading={isLoading}
        tabs={[
          {
            name: "Overview",
            children: <Overview refetchEndpoint={refetch} catalogId={catalogId} endpoint={endpoint} />,
          },
          {
            name: "Test Endpoint",
            children: endpoint && <TestEndpoint endpoint={endpoint} catalogId={catalogId} />,
          },
        ]}
      />
    )}
  </OperationalRouter>
);

export default EndpointPresentational;
