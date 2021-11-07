import {
  Actions,
  Button,
  CardColumn,
  CardColumns,
  ControlledModalContent,
  Message,
  styled,
} from "@operational/components";

import ControlledModal from "@operational/components/lib/Internals/ControlledModal";
import { OperationalRouter } from "@operational/router";
import { kebab } from "case";
import get from "lodash/get";
import isArray from "lodash/isArray";
import isEqual from "lodash/isEqual";
import omitBy from "lodash/omitBy";
import React, { useState } from "react";

import { ProductBox } from ".";
import { CreateDataSource, DataSource, TestDataSourceConnection } from "../data/dataSource";
import { DataSourceProduct, GetDataSourceProducts } from "../data/dataSourceProduct";
import { encodeForUrl } from "../utils";
import { getConfig } from "./Config";
import { DataSourceEditor } from "./DataSourceEditor";
import Loader from "./Loader";

export interface DataSourceWizardProps {
  catalogId: string;
  dataSourceProduct?: DataSourceProduct;
  productName: DataSourceProduct["name"];
  dataSource?: DataSource;
  children?: React.ReactNode;
  confirm: any;
}

const DataSourceGrid = styled("div")`
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  grid-column-gap: ${props => props.theme.space.content}px;
  grid-row-gap: ${props => props.theme.space.content}px;
`;

const makeProductImageUrl = (dataSourceProductId: string) =>
  `${getConfig("backend")}/dataSourceProducts/${dataSourceProductId}/icon.png`;

const TestConnectionContainer = styled("div")<{ singleChild: boolean }>`
  display: flex;
  align-items: center;
  justify-content: ${props => (props.singleChild ? "flex-end" : "space-between")};
  margin: ${props => props.theme.space.medium}px 0px;
  & > *:last-child {
    margin-right: 0px;
  }
`;

const ResultView = styled("div")<{ status: "success" | "error" }>`
  font-weight: bold;
  color: ${props => (props.status === "success" ? props.theme.color.success : props.theme.color.error)};
`;

export const DataSourceWizard: React.SFC<DataSourceWizardProps> = ({
  catalogId,
  productName,
  dataSource,
  dataSourceProduct,
  confirm,
}) => {
  const [connectionTestResult, setConnectionTestResult] = useState<{ type: "error" | "success"; value: string } | null>(
    null,
  );
  const [editingDataSource, setEditingDataSource] = useState<Partial<DataSource>>(dataSource || { properties: {} });
  const [fieldErrors, setFieldErrors] = useState<{ [key: string]: string }>({});
  const [error, setError] = useState<string | null>(null);

  return (
    <OperationalRouter>
      {({ pushState }) => (
        <ControlledModal
          onClose={() => pushState("data-sources")}
          closeOnOverlayClick={false}
          fullSize
          title={dataSourceProduct ? `Add a ${dataSourceProduct.name} data source` : "Add data source"}
        >
          <ControlledModalContent fullSize>
            {productName && dataSourceProduct ? (
              <>
                <DataSourceEditor
                  productProps={dataSourceProduct.properties}
                  dataSource={editingDataSource}
                  onChange={val => {
                    setEditingDataSource(val);

                    // Reset errors
                    if (val.name !== editingDataSource.name) {
                      setFieldErrors({ ...fieldErrors, name: "" });
                    }
                    if (!isEqual(val.properties, editingDataSource.properties)) {
                      setConnectionTestResult(null);
                    }
                    setError(null);
                  }}
                  productName={dataSourceProduct.name}
                  productImgUrl={makeProductImageUrl(dataSourceProduct.id)}
                  fieldErrors={fieldErrors}
                >
                  <TestConnectionContainer singleChild={!Boolean(connectionTestResult)}>
                    {connectionTestResult && (
                      <ResultView
                        data-cy={`pantheon--data-source-wizard--connection-test__${connectionTestResult.type}`}
                        status={connectionTestResult.type}
                      >
                        {connectionTestResult.type === "success"
                          ? "Connection tested successfully"
                          : "Connection test failed"}
                      </ResultView>
                    )}
                    <TestDataSourceConnection productId={dataSourceProduct.id}>
                      {(test, { loading }) => (
                        <Button
                          data-cy={"pantheon--data-source-wizard__test-connection"}
                          textColor="primary"
                          loading={loading}
                          onClick={() => {
                            if (!editingDataSource) {
                              setError(null);
                              setConnectionTestResult({ type: "error", value: "An unexpected error occurred" });
                            } else {
                              test(omitBy(editingDataSource.properties, val => val === "")).then(checkResult => {
                                setConnectionTestResult(
                                  checkResult.valid
                                    ? { type: "success", value: checkResult.message || "Successful" }
                                    : { type: "error", value: checkResult.message || "An unexpected error occurred" },
                                );
                              });
                            }
                          }}
                        >
                          Test Connection
                        </Button>
                      )}
                    </TestDataSourceConnection>
                  </TestConnectionContainer>
                </DataSourceEditor>
              </>
            ) : (
              <GetDataSourceProducts>
                {productsPage => (
                  <CardColumns>
                    <CardColumn title="Select your data source type">
                      <DataSourceGrid>
                        {productsPage ? (
                          productsPage.data.map(product => (
                            <ProductBox
                              key={product.id}
                              onClick={() =>
                                pushState(`data-sources/new/${kebab(product.name)}?properties=${encodeForUrl(product)}`)
                              }
                              imageUrl={makeProductImageUrl(product.id)}
                              label={product.name}
                            />
                          ))
                        ) : (
                          <Loader />
                        )}
                      </DataSourceGrid>
                    </CardColumn>
                  </CardColumns>
                )}
              </GetDataSourceProducts>
            )}
            {(error && <Message type="error" body={error} />) ||
              (connectionTestResult && connectionTestResult.type === "error" && (
                <Message type="error" body={connectionTestResult.value} />
              ))}
          </ControlledModalContent>
          <Actions>
            <Button data-cy="pantheon--data-source-wizard__cancel-button" onClick={() => pushState("data-sources")}>
              Cancel
            </Button>
            {dataSourceProduct && (
              <CreateDataSource catalogId={catalogId} localErrorOnly>
                {(create, { loading: loadingCreateDataSource }) => (
                  <Button
                    data-cy="pantheon--data-source-wizard__create-button"
                    color="primary"
                    loading={loadingCreateDataSource}
                    onClick={() => {
                      const createDataSource = () =>
                        create({ dataSourceProductId: dataSourceProduct.id, ...editingDataSource })
                          .then(data => pushState(`data-sources/${data.id}`))
                          .catch(e => {
                            setFieldErrors(get(e, "data.fieldErrors", {}));
                            if (isArray(e.data.errors)) {
                              setConnectionTestResult(null); // Avoid to have two errors on the screen
                              setError(e.data.errors.join(", "));
                            }
                          });

                      if (connectionTestResult === null) {
                        confirm({
                          title: "Do you want to save this data source without testing the connection?",
                          body: "Connection to this data source might not be valid",
                          actionButton: <Button textColor="primary">Save without test</Button>,
                          onConfirm: createDataSource,
                        });
                      } else if (connectionTestResult.type === "error") {
                        confirm({
                          title: "Do you want to save this data source despite the failed connection test?",
                          body: "Connection to this data source might not be valid",
                          actionButton: <Button textColor="primary">Save without test</Button>,
                          onConfirm: createDataSource,
                        });
                      } else {
                        createDataSource();
                      }
                    }}
                  >
                    Add a Data Source
                  </Button>
                )}
              </CreateDataSource>
            )}
          </Actions>
        </ControlledModal>
      )}
    </OperationalRouter>
  );
};
