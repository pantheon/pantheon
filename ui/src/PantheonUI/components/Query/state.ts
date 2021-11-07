import * as tucson from "tucson-decode";

import { AggregateQuery } from "../../data/aggregateQuery";
import { Response } from "../../data/pantheonQueryResponse";
import { QueryLog } from "../../data/queryLog";
import { RecordQuery } from "../../data/recordQuery";
import { SqlQuery } from "../../data/sqlQuery";
import { ResponseOrError, UnixTime } from "../../types";
import * as undoable from "../../utils/undoable";
import { Compatibility, CompatibilityField } from "./compatibilityTypes";
import { QueryType, TableResponse } from "./types";

export interface ResponseOrErrorWithTimestamp<T> {
  payload: ResponseOrError<T>;
  receivedAt: UnixTime;
}

export interface SqlState {
  query: SqlQuery;
  isQueryCancelled: boolean;
  request?: {
    queryId: string;
    requestedAt: UnixTime;
    query: SqlQuery;
    queryLog?: QueryLog;
    response?: ResponseOrErrorWithTimestamp<Response>;
  };
  tables?: ResponseOrError<TableResponse[]>;
}

export interface RecordState {
  query: undoable.Undoable<RecordQuery>;
  isCompatibilityResponsePending: boolean;
  isQueryCancelled: boolean;
  compatibility: ResponseOrError<CompatibilityField[]>;
  request?: {
    queryId: string;
    requestedAt: UnixTime;
    query: RecordQuery;
    queryLog?: QueryLog;
    response?: ResponseOrErrorWithTimestamp<Response>;
  };
  codeView?: {
    queryDraft: string;
    decodeErrors?: tucson.DecodeError[];
  };
}

export interface AggregateState {
  query: undoable.Undoable<AggregateQuery>;
  isCompatibilityResponsePending: boolean;
  isQueryCancelled: boolean;
  compatibility: ResponseOrError<Compatibility>;
  request?: {
    queryId: string;
    requestedAt: UnixTime;
    query: AggregateQuery;
    queryLog?: QueryLog;
    response?: ResponseOrErrorWithTimestamp<Response>;
  };
  codeView?: {
    queryDraft: string;
    decodeErrors?: tucson.DecodeError[];
  };
}

export interface State {
  queryType: QueryType;
  Aggregate: AggregateState;
  Sql: SqlState;
  Record: RecordState;
}
