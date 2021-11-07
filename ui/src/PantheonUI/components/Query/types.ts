import * as tucson from "tucson-decode";
import { Endpoint } from "../../data/endpoint";
import { Schema } from "../../data/schema";
import { RuntimeConfig } from "../Config";

export interface QueryProps {
  schemaId: string;
  schemas?: Schema[];
  catalogId: string;
  endpoint?: Endpoint;
}

/**
 * This interface is passed to the epic middleware upon creation, and available
 * to all epics as an argument.
 */
export interface EpicsDependencies {
  runtimeConfig: RuntimeConfig;
  catalogId: string;
  schemaId: string;
  /**
   * A reference string passed into query requests, tagging them so that they can be recognized
   * as queries created from this part of the interface, as opposed to e.g. `curl` in testing.
   */
  customReference: string;
}

export type QueryType = "Aggregate" | "Record" | "Sql";

/**
 * SQL column response
 */

export interface TableResponse {
  name: string;
  columns: string[];
}

export const tableResponsesSchema: tucson.Decoder<TableResponse[]> = tucson.array(
  tucson.object({
    name: tucson.string,
    columns: tucson.array(tucson.string),
  }),
);
