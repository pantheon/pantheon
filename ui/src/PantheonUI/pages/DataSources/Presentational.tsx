import { Button, Card, NameTag, Page, Paginator, Table } from "@operational/components";
import { OperationalRouterProps } from "@operational/router";
import React from "react";

import { DataSource, GetDataSourcesData } from "../../data/dataSource";
import { sliceTextIfNeeded } from "../../utils/sliceTextIfNeeded";

export interface DataSourcesPresentationalProps {
  dataSources: GetDataSourcesData;
  page: number;
  pushState: OperationalRouterProps["pushState"];
  isLoading: boolean;
}

const DataSourcesPresentational: React.FC<DataSourcesPresentationalProps> = ({
  dataSources,
  page,
  pushState,
  isLoading,
}) => (
  <Page title="Data Sources" loading={isLoading}>
    <Card
      title={
        <>
          Data sources <b>({dataSources ? dataSources.page.itemCount : "-"})</b>
        </>
      }
      action={
        <Button
          data-cy="pantheon--data-sources__create-datasource-button"
          condensed
          color="primary"
          onClick={() => pushState("data-sources/new")}
          icon="Add"
        >
          Add a data source
        </Button>
      }
    >
      {dataSources && (
        <>
          <Table<DataSource>
            data={dataSources.data}
            onRowClick={dataSource => pushState(`data-sources/${dataSource.id}`)}
            icon={() => "Database"}
            columns={[
              { heading: "Name", cell: dataSource => <code>{dataSource.name}</code> },
              {
                heading: "Description",
                cell: dataSource => sliceTextIfNeeded(dataSource.description),
              },
              {
                heading: "Source type",
                cell: dataSource => (
                  <>
                    <NameTag left>{dataSource.dataSourceProductName.slice(0, 2).toUpperCase()}</NameTag>
                    {dataSource.dataSourceProductName}
                  </>
                ),
              },
            ]}
          />
          <Paginator
            page={page}
            itemCount={dataSources.page.itemCount}
            itemsPerPage={dataSources.page.itemsPerPage}
            onChange={newPage => {
              pushState(`data-sources?page=${newPage}`);
            }}
          />
        </>
      )}
    </Card>
  </Page>
);

export default DataSourcesPresentational;
