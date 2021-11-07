import React from "react";
import { Get, GetProps, Mutate, MutateProps } from "restful-react";
import * as tucson from "tucson-decode";

import { getConfig } from "../components/Config";
import { pageQueryParams, Paginated, paginatedDecoder } from "./paginated";

export interface Schema {
  id: string;
  name: string;
  description: string;
  psl: string;
  catalogId: string;
}

export const schemaDecoder: tucson.Decoder<Schema> = tucson.object({
  id: tucson.string,
  catalogId: tucson.string,
  name: tucson.string,
  description: tucson.map(tucson.optional(tucson.string), val => val || ""),
  psl: tucson.string,
});

export const removeName = (schema: Schema): {} => {
  const { name, ...nameless } = schema;
  return nameless;
};

export const validateSchema = (schema: Schema): { [key: string]: string[] } => ({
  // Test validation only
  ...(schema.name.match(/ /) ? { name: ["Cannot contain spaces"] } : {}),
});

/**
 * GetSchemas
 */

export type GetSchemasData = Paginated<Schema[]>;

export interface GetSchemasProps {
  catalogId: string;
  page?: number;
  children: GetProps<GetSchemasData, {}, {}>["children"];
}

export const GetSchemas: React.SFC<GetSchemasProps> = props => (
  <Get<GetSchemasData>
    base={getConfig("backend")}
    path={`/catalogs/${props.catalogId}/schemas`}
    queryParams={pageQueryParams(props.page)}
    resolve={res => {
      const decoded = paginatedDecoder(tucson.array(schemaDecoder))(res);
      if (decoded.type === "error") {
        console.warn("Error decoding schemas", decoded.value);
        throw new Error("Failed to fetch schemas");
      }
      return decoded.value;
    }}
  >
    {props.children}
  </Get>
);

/**
 * GetSchema
 */

export type GetSchemaData = Schema;

export interface GetSchemaProps {
  catalogId: string;
  schemaId: string;
  children: GetProps<GetSchemaData, {}, {}>["children"];
}

export const GetSchema: React.SFC<GetSchemaProps> = props => (
  <Get<GetSchemaData>
    path={`/catalogs/${props.catalogId}/schemas/${props.schemaId}`}
    base={getConfig("backend")}
    resolve={res => {
      const decoded = schemaDecoder(res);
      if (decoded.type === "error") {
        console.warn("Error decoding schema", decoded.value);
        throw new Error("Failed to fetch schemas");
      }
      return decoded.value;
    }}
  >
    {props.children}
  </Get>
);

/**
 * UpdateSchema
 */

export type UpdateSchemaData = Schema;

export interface UpdateSchemaProps {
  catalogId: string;
  schemaId: string;
  children: MutateProps<UpdateSchemaData, {}, {}, {}>["children"];
}

export const UpdateSchema: React.SFC<UpdateSchemaProps> = props => (
  <Mutate<UpdateSchemaData, {}>
    base={getConfig("backend")}
    verb="PUT"
    path={`/catalogs/${props.catalogId}/schemas/${props.schemaId}`}
  >
    {props.children}
  </Mutate>
);

/**
 * CreateSchema
 */

export type CreateSchemaData = Schema;

export interface CreateSchemaProps {
  catalogId: string;
  children: MutateProps<CreateSchemaData, {}, {}, {}>["children"];
}

export const CreateSchema: React.SFC<CreateSchemaProps> = props => (
  <Mutate<CreateSchemaData, {}>
    base={getConfig("backend")}
    verb="POST"
    path={`/catalogs/${props.catalogId}/schemas`}
    localErrorOnly
  >
    {props.children}
  </Mutate>
);

/**
 * DeleteSchema
 */

export type DeleteSchemaData = undefined;

export interface DeleteSchemaProps {
  catalogId: string;
  schemaId: string;
  children: MutateProps<DeleteSchemaData, {}, {}, {}>["children"];
}

export const DeleteSchema: React.SFC<DeleteSchemaProps> = props => (
  <Mutate<DeleteSchemaData, {}>
    base={getConfig("backend")}
    verb="DELETE"
    path={`/catalogs/${props.catalogId}/schemas/${props.schemaId}`}
  >
    {props.children}
  </Mutate>
);
