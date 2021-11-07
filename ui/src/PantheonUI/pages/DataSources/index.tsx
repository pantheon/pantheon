import { OperationalRouter } from "@operational/router";
import React from "react";

import { Route } from "react-router";
import { DataSourceWizard } from "../../components";
import { DataSource, GetDataSources } from "../../data/dataSource";
import { DataSourceProduct } from "../../data/dataSourceProduct";
import { decodeFromUrl, getQueryField } from "../../utils";
import * as result from "../../utils/result";
import DataSourcesPresentational from "./Presentational";

export interface DataSourcesProps {
  catalogId: string;
  page: number;
}

export interface DataSourcesState {
  permissionsEditedDataSourceId?: string;
  dataSourceProduct?: DataSourceProduct;
  dataSource?: DataSource;
  connectionTestResult?: result.Result<string, null>;
}

export interface DataSourceAndProduct {
  dataSource: DataSource;
  product: DataSourceProduct;
}

export interface DataSourceWizardState {
  dataSourceProduct?: DataSourceProduct;
  dataSource: DataSource;
}

const DataSources: React.SFC<DataSourcesProps> = ({ catalogId, page }) => {
  return (
    <GetDataSources page={page} catalogId={catalogId}>
      {(dataSources, { loading }) => (
        <OperationalRouter>
          {({ basePath, pushState }) => (
            <>
              {dataSources && (
                <DataSourcesPresentational
                  pushState={pushState}
                  dataSources={dataSources}
                  page={page}
                  isLoading={loading}
                />
              )}
              <Route
                path={`${basePath}/data-sources/new/:product?`}
                render={routerProps => {
                  return (
                    <DataSourceWizard
                      confirm={confirm}
                      dataSourceProduct={decodeFromUrl(getQueryField("properties")(routerProps.location.search) || "")}
                      productName={routerProps.match.params.product}
                      catalogId={catalogId}
                    />
                  );
                }}
              />
            </>
          )}
        </OperationalRouter>
      )}
    </GetDataSources>
  );
};

export default DataSources;
