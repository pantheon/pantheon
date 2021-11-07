import React from "react";
import { Get, GetProps, Mutate, MutateProps } from "restful-react";
import * as tucson from "tucson-decode";

import { getConfig } from "../components/Config";
import { pageQueryParams, Paginated, paginatedDecoder } from "./paginated";

export interface Catalog {
  id: string;
  description?: string;
  name: string;
}

export const makeNewCatalog = (catalogChunk: Partial<Catalog> & Pick<Catalog, "id">): Catalog => ({
  description: "",
  name: "My Catalog",
  ...catalogChunk,
});

export const catalogDecoder: tucson.Decoder<Catalog> = tucson.object({
  id: tucson.string,
  description: tucson.optional(tucson.string),
  name: tucson.string,
});

export const validateCatalog = (catalog: Catalog): { [key: string]: string[] } => ({
  ...(catalog.name.length > 20 ? { name: ["Catalog names cannot be longer than 20 characters"] } : {}),
});

/**
 * GetCatalogs
 */

export type GetCatalogsData = Paginated<Catalog[]>;

export interface GetCatalogsProps {
  page?: number;
  children: GetProps<GetCatalogsData, {}, {}>["children"];
}

export const GetCatalogs: React.SFC<GetCatalogsProps> = props => (
  <Get<GetCatalogsData>
    base={getConfig("backend")}
    path={"/catalogs"}
    queryParams={pageQueryParams(props.page)}
    resolve={data => {
      const decoded = paginatedDecoder(tucson.array(catalogDecoder))(data);
      if (decoded.type === "error") {
        console.warn("Error decoding catalogs", decoded.value);
        throw new Error("Unable to retrieve catalogs");
      }
      return decoded.value;
    }}
  >
    {props.children}
  </Get>
);

/**
 * GetCatalog
 */

export type GetCatalogData = Catalog;

export interface GetCatalogProps {
  catalogId: string;
  children: GetProps<GetCatalogData, {}, {}>["children"];
}

export const GetCatalog: React.SFC<GetCatalogProps> = props => (
  <Get<GetCatalogData>
    path={`/catalogs/${props.catalogId}`}
    base={getConfig("backend")}
    resolve={data => {
      const decoded = catalogDecoder(data);
      if (decoded.type === "error") {
        console.warn("Error decoding catalog", decoded.value);
        throw new Error("Unable to retrieve catalog");
      }
      return decoded.value;
    }}
  >
    {props.children}
  </Get>
);

/**
 * UpdateCatalog
 */

export type UpdateCatalogData = Catalog;

export interface UpdateCatalogProps {
  catalogId: string;
  children: MutateProps<UpdateCatalogData, {}, {}, {}>["children"];
}

export const UpdateCatalog: React.SFC<UpdateCatalogProps> = props => (
  <Mutate<UpdateCatalogData, {}> base={getConfig("backend")} verb="PUT" path={`/catalogs/${props.catalogId}`}>
    {props.children}
  </Mutate>
);

/**
 * CreateCatalog
 */

export type CreateCatalogData = Catalog;

export interface CreateCatalogProps {
  children: MutateProps<CreateCatalogData, {}, {}, {}>["children"];
}

export const CreateCatalog: React.SFC<CreateCatalogProps> = props => (
  <Mutate<CreateCatalogData, {}> base={getConfig("backend")} verb="POST" path={`/catalogs`}>
    {props.children}
  </Mutate>
);

/**
 * DeleteCatalog
 */

export type DeleteCatalogData = undefined;

export interface DeleteCatalogProps {
  catalogId: string;
  children: MutateProps<DeleteCatalogData, {}, {}, {}>["children"];
}

export const DeleteCatalog: React.SFC<DeleteCatalogProps> = props => (
  <Mutate<DeleteCatalogData, {}> base={getConfig("backend")} verb="DELETE" path={`/catalogs/${props.catalogId}`}>
    {props.children}
  </Mutate>
);
