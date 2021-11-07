import { SchemaObject } from "openapi3-ts";
import React from "react";
import { Get, GetProps, Mutate, MutateProps } from "restful-react";
import * as tucson from "tucson-decode";
import uuidV1 from "uuid/v1";

import { getConfig } from "../components/Config";
import { AggregateQuery, aggregateQueryDecoder, makeClearAggregateQuery } from "./aggregateQuery";
import { pageQueryParams, Paginated, paginatedDecoder } from "./paginated";
import { RecordQuery, recordQueryDecoder } from "./recordQuery";
import { SqlQuery, sqlQueryDecoder } from "./sqlQuery";

export type Query = AggregateQuery | RecordQuery | SqlQuery;

export const queryDecoder = tucson.oneOf<Query>(aggregateQueryDecoder, recordQueryDecoder, sqlQueryDecoder);

export interface EndpointData {
  schemaId: string;
  query?: Query;
}

export interface PantheonSchemaObject extends SchemaObject {
  system?: boolean;
}

export interface Endpoint {
  id: string;
  catalogId: string;
  schemaId: string;
  schemaName?: string;
  name?: string;
  description?: string;
  query: Query;
  requestSchema?: PantheonSchemaObject;
}

export const makeNewEndpoint = (
  endpointChunk: Partial<Endpoint> & Pick<Endpoint, "catalogId" | "schemaId">,
): Endpoint => {
  return {
    query: makeClearAggregateQuery(),
    id: uuidV1(),
    ...endpointChunk,
  };
};

export const endpointDecoder: tucson.Decoder<Endpoint> = tucson.object({
  id: tucson.string,
  catalogId: tucson.string,
  schemaId: tucson.string,
  name: tucson.optional(tucson.string),
  description: tucson.optional(tucson.string),
  query: queryDecoder,
});

export const validateEndpoint = (endpoint: Endpoint): { [key: string]: string[] } => ({
  ...(endpoint.name && endpoint.name.match(/ /) ? { name: ["Cannot contain spaces"] } : {}),
});

/**
 * GetEndpoints
 */

export type GetEndpointsData = Paginated<Endpoint[]>;

export interface GetEndpointsProps {
  catalogId: string;
  page?: number;
  children: GetProps<GetEndpointsData, {}, {}>["children"];
}

export const GetEndpoints: React.SFC<GetEndpointsProps> = props => (
  <Get<GetEndpointsData>
    base={getConfig("backend")}
    path={`/catalogs/${props.catalogId}/endpoints`}
    queryParams={pageQueryParams(props.page)}
    resolve={res => {
      const decoded = paginatedDecoder(tucson.array(endpointDecoder))(res);
      if (decoded.type === "error") {
        console.warn("Error decoding endpoints", decoded.value);
        throw new Error("Failed to fetch endpoints");
      }
      return decoded.value;
    }}
  >
    {props.children}
  </Get>
);

/**
 * GetEndpoint
 */

export type GetEndpointData = Endpoint;

export interface GetEndpointProps {
  catalogId: string;
  endpointId: string;
  children: GetProps<GetEndpointData, {}, {}>["children"];
}

export const GetEndpoint: React.SFC<GetEndpointProps> = props => (
  <Get<GetEndpointData>
    path={`/catalogs/${props.catalogId}/endpoints/${props.endpointId}`}
    base={getConfig("backend")}
  >
    {props.children}
  </Get>
);

/**
 * UpdateEndpoint
 */

export type UpdateEndpointData = Endpoint;

export interface UpdateEndpointProps {
  catalogId: string;
  endpointId: string;
  children: MutateProps<UpdateEndpointData, {}, {}, {}>["children"];
}

export const UpdateEndpoint: React.SFC<UpdateEndpointProps> = props => (
  <Mutate<UpdateEndpointData, {}>
    base={getConfig("backend")}
    verb="PUT"
    path={`/catalogs/${props.catalogId}/endpoints/${props.endpointId}`}
  >
    {props.children}
  </Mutate>
);

/**
 * CreateEndpoint
 */

export type CreateEndpointData = Endpoint;

export interface CreateEndpointProps {
  catalogId: string;
  children: MutateProps<CreateEndpointData, {}, {}, {}>["children"];
}

export const CreateEndpoint: React.SFC<CreateEndpointProps> = props => (
  <Mutate<CreateEndpointData, {}>
    base={getConfig("backend")}
    verb="POST"
    path={`/catalogs/${props.catalogId}/endpoints`}
  >
    {props.children}
  </Mutate>
);

/**
 * DeleteEndpoint
 */

export type DeleteEndpointData = undefined;

export interface DeleteEndpointProps {
  catalogId: string;
  endpointId: string;
  children: MutateProps<DeleteEndpointData, {}, {}, {}>["children"];
}

export const DeleteEndpoint: React.SFC<DeleteEndpointProps> = props => (
  <Mutate<DeleteEndpointData, {}>
    base={getConfig("backend")}
    verb="DELETE"
    path={`/catalogs/${props.catalogId}/endpoints/${props.endpointId}`}
  >
    {props.children}
  </Mutate>
);
