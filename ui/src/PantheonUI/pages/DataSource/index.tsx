import { Page, PageContent, Progress } from "@operational/components";
import { OperationalRouter } from "@operational/router";
import { kebab, title } from "case";
import React, { Suspense } from "react";

import { GetDataSource } from "../../data/dataSource";

const NativeQuery = React.lazy(() => import("./NativeQuery"));
const Overview = React.lazy(() => import("./Overview"));

export interface DataSourceProps {
  catalogId: string;
  dataSourceId: string;
  tab?: string;
}

const DataSourceComponent: React.SFC<DataSourceProps> = ({ catalogId, dataSourceId, tab }) => (
  <GetDataSource catalogId={catalogId} dataSourceId={dataSourceId}>
    {(dataSource, { loading }, { refetch }) =>
      !dataSource ? (
        <Progress />
      ) : (
        <OperationalRouter>
          {({ pushState }) => (
            <Page
              title={`Data source: ${dataSource ? dataSource.name : ""}`}
              loading={loading}
              activeTabName={title(tab || "Overview")}
              onTabChange={newTabTitle => {
                pushState(`data-sources/${dataSource.id}/${kebab(newTabTitle)}${window.location.search}`);
              }}
              condensedTitle
              tabs={[
                {
                  name: "Overview",
                  children: (
                    <Suspense fallback={<PageContent />}>
                      <Overview dataSource={dataSource} catalogId={catalogId} refetch={refetch} />
                    </Suspense>
                  ),
                },
                {
                  name: "Native Query",
                  children: (
                    <Suspense fallback={<PageContent />}>
                      <NativeQuery catalogId={catalogId} dataSourceId={dataSource.id} />
                    </Suspense>
                  ),
                },
              ]}
            />
          )}
        </OperationalRouter>
      )
    }
  </GetDataSource>
);

export default DataSourceComponent;
