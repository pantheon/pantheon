import {
  Button,
  Card,
  CardColumn,
  CardColumns,
  CardItem,
  Code,
  FinePrint,
  Input,
  OperationalContext,
  PageArea,
  PageContent,
  ResourceName,
  Textarea,
} from "@operational/components";

import React, { useState } from "react";
import { GetMethod } from "restful-react";

import { OperationalRouter } from "@operational/router";
import { getConfig } from "../../components/Config";
import { QueryTypeIcon } from "../../components/QueryTypeIcon";
import { DeleteEndpoint, Endpoint, UpdateEndpoint } from "../../data/endpoint";
import { encodeForUrl } from "../../utils";

import get from "lodash/get";
import isEmpty from "lodash/isEmpty";
import { getNonSystemParams } from "../../utils/getNonSystemParams";

export interface OverviewProps {
  catalogId: string;
  endpoint: Endpoint | null;
  refetchEndpoint: GetMethod<Endpoint>;
}

const Overview: React.SFC<OverviewProps> = ({ catalogId, refetchEndpoint, endpoint }) => {
  const [isEditing, setIsEditing] = useState(false);
  const [metadata, setMetadata] = useState({
    description: endpoint ? endpoint.description : "",
    name: endpoint ? endpoint.name : "",
  });
  const params = getNonSystemParams(get(endpoint, "requestSchema.properties", {}));

  return (
    <OperationalRouter>
      {({ pushState }) => (
        <PageContent>
          {({ confirm }) => (
            <PageArea>
              <Card
                title="Details"
                action={
                  isEditing ? (
                    <>
                      {endpoint && (
                        <Button
                          condensed
                          onClick={() => {
                            setIsEditing(false);
                            setMetadata({ description: endpoint.description, name: endpoint.name });
                          }}
                        >
                          Cancel
                        </Button>
                      )}

                      {endpoint && (
                        <UpdateEndpoint catalogId={catalogId} endpointId={endpoint.id}>
                          {(update, { loading: updateEndpointLoading }) =>
                            endpoint && (
                              <Button
                                disabled={!(metadata.name || metadata.description)}
                                onClick={() => {
                                  update({
                                    ...endpoint,
                                    name: metadata.name || endpoint.name,
                                    description: metadata.description || endpoint.description,
                                  })
                                    .then(() => refetchEndpoint())
                                    .then(() => setIsEditing(false));
                                }}
                                loading={updateEndpointLoading}
                                condensed
                                color="primary"
                              >
                                Save
                              </Button>
                            )
                          }
                        </UpdateEndpoint>
                      )}
                    </>
                  ) : (
                    <Button onClick={() => setIsEditing(true)} condensed color="primary">
                      Edit
                    </Button>
                  )
                }
              >
                {endpoint && (
                  <>
                    <CardItem title="ID" value={<code>{endpoint.id}</code> || ""} />
                    <CardItem
                      title="Name"
                      value={
                        isEditing ? (
                          <Input
                            onChange={name => setMetadata({ ...metadata, name })}
                            value={metadata.name || endpoint.name}
                          />
                        ) : (
                          metadata.name || endpoint.name
                        )
                      }
                    />
                    <CardItem
                      title="Description"
                      value={
                        isEditing ? (
                          <Textarea
                            onChange={description => setMetadata({ ...metadata, description })}
                            value={metadata.description || endpoint.description || ""}
                          />
                        ) : (
                          metadata.description || endpoint.description
                        )
                      }
                    />
                    <br />
                    <CardColumns>
                      <CardColumn title="Execution URL">
                        <Input
                          data-cy="pantheon--endpoints__execution-url"
                          value={`${getConfig("backend")}/catalogs/${catalogId}/endpoints/${endpoint.name}/execute`}
                          fullWidth
                          copy
                        />
                      </CardColumn>
                    </CardColumns>
                  </>
                )}
              </Card>
              <Card
                title="Query"
                action={
                  <Button
                    onClick={() =>
                      endpoint
                        ? pushState(`/endpoints/${endpoint.id}/edit?querydata=${encodeForUrl(endpoint.query)}`)
                        : undefined
                    }
                    condensed
                    color="primary"
                  >
                    Edit in Query Editor
                  </Button>
                }
              >
                {endpoint && (
                  <>
                    <CardItem title="Type" value={<QueryTypeIcon endpoint={endpoint} />} />
                    <CardItem title="Schema" value={<code>{endpoint.schemaName || endpoint.schemaId}</code>} />
                    {!isEmpty(params) && (
                      <>
                        <br />
                        <CardColumns>
                          <CardColumn title="Query Parameters">
                            <Code syntax="javascript">{JSON.stringify(params, null, 2)}</Code>
                          </CardColumn>
                        </CardColumns>
                        <br />
                      </>
                    )}

                    <br />
                    <CardColumns>
                      <CardColumn title="Query Definition">
                        <Code syntax="javascript" children={JSON.stringify(endpoint.query, null, 2)} />
                      </CardColumn>
                    </CardColumns>
                  </>
                )}
              </Card>
              <Card title="Danger zone">
                {endpoint && (
                  <DeleteEndpoint catalogId={catalogId} endpointId={endpoint.id}>
                    {(deleteEndpoint, { loading: loadingDeleteEndpoint }) => (
                      <OperationalContext>
                        {({ pushMessage }) => (
                          <Button
                            color="error"
                            loading={loadingDeleteEndpoint}
                            onClick={() => {
                              confirm({
                                title: "Confirm Endpoint Deletion",
                                body: (
                                  <>
                                    Are you sure you would like to delete the endpoint{" "}
                                    <ResourceName>{endpoint.name}</ResourceName>?
                                  </>
                                ),
                                onConfirm: () =>
                                  deleteEndpoint().then(() => {
                                    pushMessage({ body: "Endpoint Deleted", type: "success" });
                                    pushState("endpoints");
                                  }),
                              });
                            }}
                          >
                            Delete Endpoint
                          </Button>
                        )}
                      </OperationalContext>
                    )}
                  </DeleteEndpoint>
                )}
                <FinePrint>Consumers will no longer be able to use this endpoint.</FinePrint>
              </Card>
            </PageArea>
          )}
        </PageContent>
      )}
    </OperationalRouter>
  );
};

export default Overview;
