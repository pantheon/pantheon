import {
  Button,
  Card,
  FinePrint,
  ModalConfirmContext,
  OperationalContext,
  PageArea,
  PageContent,
} from "@operational/components";

import React, { useState } from "react";

import { OperationalRouter } from "@operational/router";
import { DataSourceConnectionEditor, DataSourceInfoEditor } from "../../components/DataSourceEditor";
import DataSourceFields from "../../components/DataSourceFields";
import { DataSource, DeleteDataSource } from "../../data/dataSource";
import { GetDataSourceProduct } from "../../data/dataSourceProduct";
import { Draft } from "../../types";

export interface DataSourceOverviewProps {
  dataSource: DataSource;
  refetch: () => void;
  catalogId: string;
}

const Overview: React.SFC<DataSourceOverviewProps> = ({ dataSource, refetch, catalogId }) => {
  const [infoDraft, setInfoDraft] = useState<Draft<Partial<DataSource>> | null>(null);
  const [configDraft, setConfigDraft] = useState<Draft<Partial<DataSource>> | null>(null);

  return (
    <PageContent>
      {({ confirm }: ModalConfirmContext) =>
        dataSource !== null && (
          <PageArea>
            <DataSourceFields
              title="Data source information"
              dataSource={dataSource}
              draft={infoDraft}
              setDraft={setInfoDraft}
              refetch={refetch}
              onEdit={() => setConfigDraft(null)}
              showFields={baseProps => <DataSourceInfoEditor {...baseProps} />}
            />
            <GetDataSourceProduct
              id={dataSource.dataSourceProductId}
              children={product => (
                <DataSourceFields
                  title="Configuration"
                  dataSource={dataSource}
                  draft={configDraft}
                  setDraft={setConfigDraft}
                  refetch={refetch}
                  onEdit={() => setInfoDraft(null)}
                  showFields={baseProps => (
                    <DataSourceConnectionEditor {...baseProps} properties={product ? product.properties : []} />
                  )}
                />
              )}
            />
            <Card title="Danger zone">
              <DeleteDataSource dataSourceId={dataSource.id} catalogId={catalogId}>
                {(deleteDataSource, { loading: loadingDeleteDataSource }) => (
                  <OperationalContext>
                    {({ pushMessage }) => (
                      <OperationalRouter>
                        {({ pushState }) => (
                          <Button
                            color="error"
                            loading={loadingDeleteDataSource}
                            onClick={() => {
                              confirm({
                                title: "Danger zone",
                                body:
                                  "Schemas and queries using this data source will no longer be able to query it. Are you sure you want to delete this data source?",
                                actionButton: <Button color="error">Delete</Button>,
                                onConfirm: () => {
                                  deleteDataSource().then(() => {
                                    pushMessage({
                                      body: "Data source deleted successfully",
                                      type: "info",
                                    });
                                    pushState("data-sources");
                                  });
                                },
                              });
                            }}
                          >
                            Delete
                          </Button>
                        )}
                      </OperationalRouter>
                    )}
                  </OperationalContext>
                )}
              </DeleteDataSource>
              <FinePrint>Schemas and queries using this data source will no longer be able to query it.</FinePrint>
            </Card>
          </PageArea>
        )
      }
    </PageContent>
  );
};

export default Overview;
