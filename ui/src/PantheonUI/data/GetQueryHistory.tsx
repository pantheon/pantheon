import * as React from "react";
import Get, { GetProps } from "restful-react";
import { getConfig } from "../components/Config";
import { AggregateQuery } from "./aggregateQuery";

interface QueryHistory {
  catalogId: string;
  completedAt: string;
  completionStatus: {
    type: "Succeeded" | "Failed";
  };
  customReference: string;
  id: string;
  query: AggregateQuery;
  schemaId: string;
  startedAt: string;
  type: string;
}

interface QueryHistoryResp {
  data: QueryHistory[];
  page: {
    current: number;
    itemCount: number;
    itemsPerPage: number;
  };
}

export interface GetQueryHistoryProps {
  catalogId: string;
  children: GetProps<QueryHistoryResp, {}, { customRefPattern: string }>["children"];
  customRefPattern: string;
}

export const GetQueryHistory: React.FC<GetQueryHistoryProps> = ({ children, catalogId, customRefPattern }) => (
  <Get<QueryHistoryResp>
    path={`/catalogs/${catalogId}/queryHistory`}
    base={getConfig("backend")}
    queryParams={{ customRefPattern }}
  >
    {children}
  </Get>
);
