import * as tucson from "tucson-decode";

import { OrderedColumn, orderedColumnDecoder } from "./orderedColumn";

export interface RecordQuery {
  type: "Record";
  rows: string[];
  filter?: string;
  orderBy?: OrderedColumn[];
  limit?: number;
  offset: number;
}

export const makeClearRecordQuery = (): RecordQuery => ({
  type: "Record",
  rows: [],
  filter: undefined,
  orderBy: undefined,
  offset: 0,
  limit: 10,
});

export const recordQueryDecoder: tucson.Decoder<RecordQuery> = tucson.object({
  type: tucson.flatMap(
    tucson.string,
    val => (val === "Record" ? tucson.succeed("Record") : tucson.fail("Bad type")) as tucson.Decoder<"Record">,
  ),
  rows: tucson.array(tucson.string),
  filter: tucson.optional(tucson.string),
  orderBy: tucson.optional(tucson.array(orderedColumnDecoder)),
  offset: tucson.number,
  limit: tucson.number,
});

export const canQuery = (query: RecordQuery): boolean => query.rows.length > 0;
