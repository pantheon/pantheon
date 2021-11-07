import React from "react";
import { Get, GetProps, Mutate, MutateProps } from "restful-react";

import { getConfig } from "../components/Config";
import { pageQueryParams, Paginated } from "./paginated";

export interface DataSourceProductProperty {
  name: string;
  type: string;
}

export interface DataSourceProduct {
  id: string;
  name: string;
  description?: string;
  type: string;
  properties: DataSourceProductProperty[];
  isBundled: boolean;
  productRoot: string;
  className?: string;
}

export const makeNewDataSourceProduct = (
  id: string,
  dataSourceChunk?: Partial<DataSourceProduct>,
): DataSourceProduct => {
  return {
    id,
    name: "data-src-product2",
    description: "New data source product",
    type: "jdbc",
    properties: Array.of<DataSourceProductProperty>(),
    isBundled: false,
    productRoot: "/",
    ...dataSourceChunk,
  };
};

/**
 * GetDataSourceProducts
 */

export interface GetDataSourceProductsProps {
  page?: number;
  pageSize?: number;
  children: GetProps<Paginated<DataSourceProduct[]>, {}, {}>["children"];
}

export const GetDataSourceProducts: React.SFC<GetDataSourceProductsProps> = props => (
  <Get<Paginated<DataSourceProduct[]>>
    base={getConfig("backend")}
    path={"/dataSourceProducts"}
    queryParams={pageQueryParams(props.page)}
  >
    {props.children}
  </Get>
);

export interface GetDataSourceProductProps {
  id: string;
  children: GetProps<DataSourceProduct, {}, {}>["children"];
}

export const GetDataSourceProduct: React.SFC<GetDataSourceProductProps> = props => (
  <Get<DataSourceProduct> path={`/dataSourceProducts/${props.id}`} base={getConfig("backend")}>
    {props.children}
  </Get>
);

/**
 * UpdateDataSource
 */

export interface UpdateDataSourceProps {
  dataSourceId: string;
  children: MutateProps<DataSourceProduct, {}, {}, {}>["children"];
}

export const UpdateDataSourceProduct: React.SFC<UpdateDataSourceProps> = props => (
  <Mutate<DataSourceProduct, {}>
    base={getConfig("backend")}
    verb="PUT"
    path={`/dataSourceProducts/${props.dataSourceId}`}
  >
    {props.children}
  </Mutate>
);

/**
 * DeleteDataSource
 */

export interface DeleteDataSourceProductProps {
  dataSourceProductId: string;
  children: MutateProps<undefined, {}, {}, {}>["children"];
}

export const DeleteDataSourceProduct: React.SFC<DeleteDataSourceProductProps> = props => (
  <Mutate<undefined, {}>
    base={getConfig("backend")}
    verb="DELETE"
    path={`/dataSourceProducts/${props.dataSourceProductId}`}
  >
    {props.children}
  </Mutate>
);
