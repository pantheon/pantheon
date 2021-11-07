import { IconName } from "@operational/components";
import React from "react";
import { Get, GetProps } from "restful-react";
import * as tucson from "tucson-decode";

import { getConfig } from "../components/Config";
import { AggregateQuery, aggregateQueryDecoder } from "./aggregateQuery";
import { pageQueryParams, Paginated, paginatedDecoder } from "./paginated";
import { RecordQuery, recordQueryDecoder } from "./recordQuery";
import { SqlQuery, sqlQueryDecoder } from "./sqlQuery";

export interface QueryLog {
  id: string;
  catalogId: string;
  type: string;
  startedAt?: string;
  schemaId?: string;
  dataSourceId?: string;
  completedAt?: string;
  queryDescription?: string;
  query: AggregateQuery | RecordQuery | SqlQuery | string | undefined;
  completionStatus?: {
    type: string;
    msg?: string;
  };
  customReference?: string;
  plan?: string;
  backendLogicalPlan?: string;
  backendPhysicalPlan?: string;
}

export const queryLogDecoder: tucson.Decoder<QueryLog> = tucson.object({
  id: tucson.string,
  catalogId: tucson.string,
  type: tucson.string,
  startedAt: tucson.optional(tucson.string),
  schemaId: tucson.optional(tucson.string),
  dataSourceId: tucson.optional(tucson.string),
  completedAt: tucson.optional(tucson.string),
  queryDescription: tucson.optional(tucson.string),
  query: tucson.optional(
    tucson.oneOf<AggregateQuery | RecordQuery | SqlQuery | string>(
      tucson.string,
      aggregateQueryDecoder,
      recordQueryDecoder,
      sqlQueryDecoder,
    ),
  ),
  completionStatus: tucson.optional(
    tucson.object({
      type: tucson.string,
      msg: tucson.optional(tucson.string),
    }),
  ),
  customReference: tucson.optional(tucson.string),
  plan: tucson.optional(tucson.string),
  backendPhysicalPlan: tucson.optional(tucson.string),
  backendLogicalPlan: tucson.optional(tucson.string),
});

export const displayType = (queryLog: QueryLog): string =>
  queryLog.type === "Schema" && queryLog.query && typeof queryLog.query !== "string"
    ? queryLog.query.type
    : queryLog.type;

export const iconName = (queryLog: QueryLog): IconName => {
  if (queryLog.query && typeof queryLog.query !== "string") {
    switch (queryLog.query.type) {
      case "Aggregate":
        return "Olap";
      case "Record":
        return "Entity";
      case "Sql":
        return "Sql";
    }
  }
  return "Document";
};

/**
 * GetQueries
 */

export type GetQueryLogsData = Paginated<QueryLog[]>;

export interface GetQueriesProps {
  catalogId: string;
  page?: number;
  children: GetProps<GetQueryLogsData, {}, {}>["children"];
}

export const GetQueryLogs: React.SFC<GetQueriesProps> = props => {
  return (
    <Get<GetQueryLogsData>
      base={getConfig("backend")}
      path={`/catalogs/${props.catalogId}/queryHistory`}
      queryParams={pageQueryParams(props.page)}
      resolve={data => {
        const decoded = paginatedDecoder(tucson.array(queryLogDecoder))(data);
        if (decoded.type === "error") {
          console.warn("Error decoding query logs", decoded.value);
          throw new Error("Unable to retrieve query log");
        }
        return decoded.value;
      }}
    >
      {props.children}
    </Get>
  );
};

/**
 * GetQuery
 */

export type GetQueryLogData = QueryLog;

export interface GetQueryLogProps {
  catalogId: string;
  queryId: string;
  children: GetProps<GetQueryLogData, {}, {}>["children"];
}

export const GetQueryLog: React.SFC<GetQueryLogProps> = props => (
  <Get<GetQueryLogData>
    path={`/catalogs/${props.catalogId}/queryHistory/${props.queryId}`}
    base={getConfig("backend")}
    resolve={data => {
      const decoded = queryLogDecoder(data);
      if (decoded.type === "error") {
        console.warn("Error decoding query log", decoded.value);
        throw new Error("Unable to retrieve query log");
      }
      return decoded.value;
    }}
  >
    {props.children}
  </Get>
);
