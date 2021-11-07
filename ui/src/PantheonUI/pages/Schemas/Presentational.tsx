import { Button, Card, Page, Paginator, Table } from "@operational/components";
import { OperationalRouter } from "@operational/router";
import React from "react";
import { GetSchemasData } from "../../data/schema";

export interface SchemasPresentationalProps {
  isLoading: boolean;
  schemas: GetSchemasData;
  page: number;
}

const SchemasPresentational: React.FC<SchemasPresentationalProps> = ({
  isLoading,
  schemas,
  page,
}) => (
  <Page title="Schemas" loading={isLoading}>
    <Card
      title={
        <>
          Schemas <b>({schemas ? schemas.page.itemCount : "-"})</b>
        </>
      }
      action={
        <OperationalRouter>
          {({ basePath }) => (
            <Button
              data-cy="pantheon--schemas__create-schema"
              condensed
              color="primary"
              to={`${basePath}/schemas/new`}
              icon="Add"
            >
              Create Schema
            </Button>
          )}
        </OperationalRouter>
      }
    >
      <OperationalRouter>
        {({ pushState }) => (
          <>
            <Table
              data={schemas.data}
              onRowClick={schema => {
                pushState(`schemas/${schema.id}`);
              }}
              icon={() => "Schema"}
              columns={[
                { heading: "Name", cell: schema => <code>{schema.name}</code> },
                {
                  heading: "Description",
                  cell: schema =>
                    schema.description.length > 30 ? schema.description.slice(0, 30) + "…" : schema.description,
                },
              ]}
              rowActions={schema => [
                {
                  label: "Details",
                  onClick: () => pushState(`schemas/${schema.id}`),
                },
                {
                  label: "Build query",
                  onClick: () => pushState(`schemas/${schema.id}/query`),
                }
              ]}
            />
            <Paginator
              page={page}
              itemCount={schemas.page.itemCount}
              itemsPerPage={schemas.page.itemsPerPage}
              onChange={newPage => {
                pushState(`schemas/?page=${newPage}`);
              }}
            />
          </>
        )}
      </OperationalRouter>
    </Card>
  </Page>
);

export default SchemasPresentational;
