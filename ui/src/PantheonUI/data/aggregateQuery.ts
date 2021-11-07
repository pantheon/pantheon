import * as tucson from "tucson-decode";

import { OrderedColumn, orderedColumnDecoder } from "./orderedColumn";
import { TopN, topNDecoder } from "./topN";

export const orderByParamDecoder = tucson.optional(tucson.array(orderedColumnDecoder));
export const orderByParamSample: OrderedColumn[] = [];

export interface AggregateQuery {
  type: "Aggregate";
  rows: string[];
  columns: string[];
  measures: string[];
  offset: number;
  filter?: string;
  aggregateFilter?: string;
  limit?: number;
  orderBy?: OrderedColumn[];
  rowsTopN?: TopN;
  columnsTopN?: TopN;
}

export const makeClearAggregateQuery = (): AggregateQuery => ({
  type: "Aggregate",
  rows: [],
  columns: [],
  measures: [],
  filter: undefined,
  offset: 0,
  orderBy: undefined,
  limit: 10,
  rowsTopN: undefined,
  columnsTopN: undefined,
});

export const aggregateQueryDecoder: tucson.Decoder<AggregateQuery> = tucson.object({
  type: tucson.flatMap(
    tucson.string,
    val => (val === "Aggregate" ? tucson.succeed("Aggregate") : tucson.fail("Bad type")) as tucson.Decoder<"Aggregate">,
  ),
  rows: tucson.array(tucson.string),
  columns: tucson.array(tucson.string),
  measures: tucson.array(tucson.string),
  offset: tucson.map(tucson.optional(tucson.number), val => (typeof val === "number" ? val : 0)),
  limit: tucson.optional(tucson.number),
  filter: tucson.optional(tucson.string),
  orderBy: orderByParamDecoder,
  rowsTopN: tucson.optional(topNDecoder),
  columnsTopN: tucson.optional(topNDecoder),
  aggregateFilter: tucson.optional(tucson.string),
});

export const canQuery = (query: AggregateQuery): boolean =>
  // If there are columns, there have to be measures too
  (query.columns.length === 0 || query.measures.length > 0) &&
  // Querying is not allowed if columns, rows and measures are all empty
  !(query.columns.length === 0 && query.rows.length === 0 && query.measures.length === 0);
