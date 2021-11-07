import {
  Button,
  Card,
  CardColumn,
  CardColumns,
  Code,
  Form,
  Input,
  PageArea,
  PageContent,
} from "@operational/components";

import omitBy from "lodash/omitBy";
import React, { useCallback, useEffect, useRef, useState } from "react";

import { EndpointComponentProps } from ".";
import { getConfig } from "../../components/Config";
import { Endpoint, PantheonSchemaObject } from "../../data/endpoint";
import { customFetch } from "../../utils";

export interface TestConnectionProps {
  catalogId: EndpointComponentProps["catalogId"];
  endpoint: Endpoint;
}

const TestEndpoint: React.SFC<TestConnectionProps> = ({ catalogId, endpoint }) => {
  const properties =
    endpoint.requestSchema && endpoint.requestSchema.properties
      ? Object.entries(endpoint.requestSchema.properties)
      : [];

  /**
   * Make a simple object of params: { [key]: defaultValue }
   * based on the backend response
   */
  const initialState = properties
    .map(([property, value]: [string, PantheonSchemaObject]) => ({
      [property]: value.type === "string" ? "" : value.minimum || 0,
    }))
    .reduce((acc, val) => ({ ...acc, ...val }), {});

  const [params, updateParams] = useState(initialState);
  const [playgroundBodyType, updatePlaygroundBodyType] = useState("CURL");
  const [endpointOutput, setEndpointOutput] = useState<unknown>({});
  const [isRequestInFlight, setIsRequestInFlight] = useState(false);
  const requestController = useRef<AbortController | null>(null);

  const querySettings = properties.filter(([_, field]: [string, PantheonSchemaObject]) => field.system);
  const queryParameters = properties.filter(([_, field]: [string, PantheonSchemaObject]) => !field.system);

  const endpointUrl = `/catalogs/${catalogId}/endpoints/${endpoint.id}/execute`;

  useEffect(() => {
    requestController.current = new AbortController();
    return () => {
      if (requestController.current) {
        requestController.current.abort();
      }
    };
  }, []);

  const runQuery = useCallback(() => {
    const { _queryId, _customReference, ...requestBody } = params;
    setIsRequestInFlight(true);
    setEndpointOutput({});
    customFetch(endpointUrl, {
      method: "POST",
      body: JSON.stringify({
        ...requestBody,
        _customReference: `ui-test-endpoint${_customReference ? ` / ${_customReference}` : ""}`,
      }),
      headers: { "Content-Type": "application/json" },
      signal: requestController.current ? requestController.current.signal : undefined,
    })
      .then(resp => resp.json())
      .then(setEndpointOutput)
      .then(() => setIsRequestInFlight(false));
  }, [requestController.current, params]);

  return (
    <PageContent>
      <PageArea>
        <Card title="Endpoint Information">
          <CardColumns>
            <CardColumn title="URL">
              <Input
                data-cy="pantheon--endpoints__endpoint-url"
                value={`${getConfig("backend")}${endpointUrl}`}
                fullWidth
                copy
              />
            </CardColumn>
          </CardColumns>
        </Card>
        <Card title="Playground">
          <CardColumns>
            <CardColumn>
              <CardColumns>
                {queryParameters && (
                  <CardColumn title="Parameters">
                    <Form>
                      {queryParameters.map(([key]) => (
                        <Input
                          onChange={newValue => updateParams({ ...params, [key]: newValue })}
                          key={key}
                          label={key}
                          value={String(params[key])}
                        />
                      ))}
                    </Form>
                  </CardColumn>
                )}
              </CardColumns>
              <br />
              <br />
              <CardColumns>
                {querySettings && (
                  <CardColumn title="Query Settings">
                    <Form>
                      {querySettings.map(([key, value]: [string, PantheonSchemaObject]) => (
                        <Input
                          type={value.type === "string" ? "text" : "number"}
                          onChange={newValue =>
                            updateParams({
                              ...params,
                              [key]: value.type === "string" ? newValue : Number(newValue),
                            })
                          }
                          key={key}
                          label={key}
                          value={String(params[key])}
                        />
                      ))}
                    </Form>
                  </CardColumn>
                )}
              </CardColumns>
              <br />
              <div>
                <Button
                  loading={isRequestInFlight}
                  disabled={isRequestInFlight}
                  color="primary"
                  onClick={() => !isRequestInFlight && runQuery()}
                >
                  Send Request
                </Button>
              </div>
            </CardColumn>
            <CardColumn>
              <CardColumns>
                <CardColumn
                  title="Request Body"
                  activeTabName={playgroundBodyType}
                  onTabChange={updatePlaygroundBodyType}
                  tabs={[
                    {
                      name: "CURL",
                      children: (
                        <Code syntax="bash">{`curl \\
    '${getConfig("backend")}${endpointUrl}' \\
    -H 'content-type: application/json' \\
    -H "Authorization: Bearer $ACCOUNT_TOKEN" \\
    -H 'accept: */*'${
      params
        ? ` \\
    --data-binary '${JSON.stringify(omitBy(params, value => value === ""), null, 2)}'`
        : ""
    }`}</Code>
                      ),
                    },
                    {
                      name: "JSON",
                      children: (
                        <Code syntax="javascript">
                          {JSON.stringify(omitBy(params, value => value === ""), null, 2)}
                        </Code>
                      ),
                    },
                    {
                      name: "SCHEMA",
                      children: endpoint.requestSchema ? (
                        <Code syntax="javascript">{JSON.stringify(endpoint.requestSchema.properties, null, 2)}</Code>
                      ) : (
                        "Schema Not Available"
                      ),
                    },
                    {
                      name: "NODE.JS",
                      children: (
                        <Code syntax="javascript">{`const fetch = require("node-fetch");
const token = "MY_TOKEN_HERE";
                      
fetch("${getConfig("backend")}${endpointUrl}", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Authorization: \`Bearer \${token}\`,
    accept: "*/*",
  },
  body: JSON.stringify(${JSON.stringify(omitBy(params, value => value === ""), null, 4).slice(0, -2)}
  })
})
  .then(response => response.json())
  .then(result => {
    // Do something with the result
    console.log(JSON.stringify(result, null, 2));
  });
                      `}</Code>
                      ),
                    },
                  ]}
                />
              </CardColumns>
            </CardColumn>
          </CardColumns>
        </Card>
        <Card title="Result">
          <CardColumns>
            <CardColumn title="Response">
              <Code syntax="javascript">{JSON.stringify(endpointOutput, null, 2)}</Code>
            </CardColumn>
          </CardColumns>
        </Card>
      </PageArea>
    </PageContent>
  );
};

export default TestEndpoint;
