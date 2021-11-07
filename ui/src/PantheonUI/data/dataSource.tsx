import React from "react";
import { Get, GetProps, Mutate, MutateProps } from "restful-react";
import * as tucson from "tucson-decode";
import uuidV1 from "uuid/v1";
import { getConfig } from "../components/Config";
import { pageQueryParams, Paginated, paginatedDecoder } from "./paginated";

export type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>;

export interface DataSource {
  id: string;
  catalogId: string;
  description?: string;
  name: string;
  dataSourceProductId: string;
  dataSourceProductName: string;
  properties: {
    [key: string]: string;
  };
}

export const makeDataSource = (catalogId: string): DataSource => {
  return {
    // providing UUID here not to separate response and request types (id is optional in request but mandatory in response)
    id: uuidV1(),
    catalogId,
    description: "New data source",
    name: "data-src2",
    properties: {},
    // to be checked on validation along with properties (id normally cannot be an empty string).
    dataSourceProductId: "",
    dataSourceProductName: "",
  };
};

export const dataSourceDecoder: tucson.Decoder<DataSource> = tucson.object({
  id: tucson.string,
  catalogId: tucson.string,
  description: tucson.optional(tucson.string),
  name: tucson.string,
  properties: tucson.dictionary(tucson.string),
  dataSourceProductId: tucson.string,
  dataSourceProductName: tucson.string,
});

/**
 * GetDataSources
 */

export type GetDataSourcesData = Paginated<DataSource[]>;

export interface GetDataSourcesProps {
  catalogId: string;
  page?: number;
  children: GetProps<GetDataSourcesData, {}, {}>["children"];
}

export const GetDataSources: React.SFC<GetDataSourcesProps> = props => (
  <Get<GetDataSourcesData>
    base={getConfig("backend")}
    path={`/catalogs/${props.catalogId}/dataSources`}
    queryParams={pageQueryParams(props.page)}
    resolve={data => {
      const decoded = paginatedDecoder(tucson.array(dataSourceDecoder))(data);
      if (decoded.type === "error") {
        console.warn("Error decoding data sources", decoded.value);
        throw new Error("Unable to retrieve data sources");
      }
      return decoded.value;
    }}
  >
    {props.children}
  </Get>
);

/**
 * GetDataSource
 */

export type GetDataSourceData = DataSource;

export interface GetDataSourceProps {
  catalogId: string;
  dataSourceId: string;
  children: GetProps<GetDataSourceData, {}, {}>["children"];
}

export const GetDataSource: React.SFC<GetDataSourceProps> = props => (
  <Get<GetDataSourceData>
    path={`/catalogs/${props.catalogId}/dataSources/${props.dataSourceId}`}
    base={getConfig("backend")}
    resolve={data => {
      const decoded = dataSourceDecoder(data);
      if (decoded.type === "error") {
        console.warn("Error decoding data source", decoded.value);
        throw new Error("Unable to retrieve data source");
      }
      return decoded.value;
    }}
  >
    {props.children}
  </Get>
);

/**
 * UpdateDataSource
 */

export type UpdateDataSourceData = DataSource;

export interface UpdateDataSourceProps {
  catalogId: string;
  dataSourceId: string;
  children: MutateProps<UpdateDataSourceData, {}, {}, {}>["children"];
}

export const UpdateDataSource: React.SFC<UpdateDataSourceProps> = props => (
  <Mutate<UpdateDataSourceData, {}>
    base={getConfig("backend")}
    verb="PUT"
    path={`/catalogs/${props.catalogId}/dataSources/${props.dataSourceId}`}
  >
    {props.children}
  </Mutate>
);

/**
 * CreateDataSource
 */

export type CreateDataSourceData = DataSource;

export type CreateDataSourceProps = Omit<MutateProps<CreateDataSourceData, {}, {}, {}>, "path" | "verb"> & {
  catalogId: string;
};

export const CreateDataSource: React.SFC<CreateDataSourceProps> = props => (
  <Mutate<CreateDataSourceData, {}>
    base={getConfig("backend")}
    verb="POST"
    path={`/catalogs/${props.catalogId}/dataSources`}
    {...props}
  />
);

/**
 * DeleteDataSource
 */

export type DeleteDataSourceData = undefined;

export interface DeleteDataSourceProps {
  catalogId: string;
  dataSourceId: string;
  children: MutateProps<DeleteDataSourceData, {}, {}, {}>["children"];
}

export const DeleteDataSource: React.SFC<DeleteDataSourceProps> = props => (
  <Mutate<DeleteDataSourceData, {}>
    base={getConfig("backend")}
    verb="DELETE"
    path={`/catalogs/${props.catalogId}/dataSources/${props.dataSourceId}`}
  >
    {props.children}
  </Mutate>
);

/**
 * QueryDataSource
 */

export type QueryDataSourceData = string;

export interface QueryDataSourceProps {
  catalogId: string;
  dataSourceId: string;
  children: MutateProps<QueryDataSourceData, {}, {}, {}>["children"];
}

export const QueryDataSource: React.SFC<QueryDataSourceProps> = props => (
  <Mutate<QueryDataSourceData, {}>
    base={getConfig("backend")}
    verb="POST"
    path={`/catalogs/${props.catalogId}/dataSources/${props.dataSourceId}/nativeQuery`}
  >
    {props.children}
  </Mutate>
);

/**
 * TestDataSource
 */

export interface ConnectionTestResult {
  valid: boolean;
  message?: string;
}

export const connectionTestResultDecoder = tucson.object({
  valid: tucson.boolean,
  message: tucson.optional(tucson.string),
});

export interface TestDataSourceConnectionProps {
  productId: string;
  children: MutateProps<ConnectionTestResult, {}, {}, {}>["children"];
}

export const TestDataSourceConnection: React.SFC<TestDataSourceConnectionProps> = props => (
  <Mutate<ConnectionTestResult, {}>
    base={getConfig("backend")}
    verb="POST"
    path={`/dataSourceProducts/${props.productId}/testConnection`}
  >
    {props.children}
  </Mutate>
);
