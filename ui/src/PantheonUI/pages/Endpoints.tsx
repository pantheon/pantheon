import { Card, Page, Paginator, ResourceName, Table } from "@operational/components";
import { OperationalRouter } from "@operational/router";
import React from "react";

import { QueryTypeIcon } from "../components/QueryTypeIcon";
import { GetEndpoints } from "../data/endpoint";
import { sliceTextIfNeeded } from "../utils/sliceTextIfNeeded";

export interface EndpointsProps {
  catalogId: string;
  page: number;
}

const Endpoints: React.SFC<EndpointsProps> = ({ catalogId, page }) => {
  return (
    <GetEndpoints catalogId={catalogId} page={page}>
      {endpoints => (
        <Page title="Endpoints" loading={!endpoints}>
          {endpoints && (
            <Card
              title={
                <>
                  Endpoints <b>({endpoints ? endpoints.page.itemCount : "-"})</b>
                </>
              }
            >
              <OperationalRouter>
                {({ pushState }) => (
                  <>
                    <Table
                      data={endpoints.data}
                      onRowClick={endpoint => {
                        pushState(`endpoints/${endpoint.id}/overview`);
                      }}
                      columns={[
                        {
                          heading: "Type",
                          cell: endpoint => <QueryTypeIcon endpoint={endpoint} />,
                        },
                        { heading: "Name", cell: endpoint => <ResourceName>{endpoint.name}</ResourceName> },
                        {
                          heading: "Schema",
                          cell: endpoint => <code>{endpoint.schemaName || endpoint.schemaId}</code>,
                        },
                        { heading: "Description", cell: endpoint => sliceTextIfNeeded(endpoint.description) },
                      ]}
                      rowActions={endpoint => [
                        {
                          label: "Details",
                          onClick: () => {
                            pushState(`endpoints/${endpoint.id}`);
                          },
                        }
                      ]}
                    />
                    <Paginator
                      page={page}
                      itemCount={endpoints.page.itemCount}
                      itemsPerPage={endpoints.page.itemsPerPage}
                      onChange={newPage => {
                        pushState(`endpoints?page=${newPage}`);
                      }}
                    />
                  </>
                )}
              </OperationalRouter>
            </Card>
          )}
        </Page>
      )}
    </GetEndpoints>
  );
};

export default Endpoints;
