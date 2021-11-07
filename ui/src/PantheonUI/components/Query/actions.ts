import { AggregateQuery } from "../../data/aggregateQuery";
import { OrderedColumn } from "../../data/orderedColumn";
import { Response } from "../../data/pantheonQueryResponse";
import { QueryLog } from "../../data/queryLog";
import { RecordQuery } from "../../data/recordQuery";
import { SqlQuery } from "../../data/sqlQuery";
import { ResponseOrError, UnixTime } from "../../types";

import { Compatibility, CompatibilityField } from "./compatibilityTypes";
import { QueryType, TableResponse } from "./types";

export enum ActionType {
  // General
  ChangeQueryType = "ChangeQueryType",
  ReceiveQueryHistory = "ReceiveQueryHistory",
  // Aggregate
  BlurAggregateFilterInput = "BlurAggregateFilterInput",
  BlurAggregatePostAggregateFilterInput = "BlurAggregatePostAggregateFilterInput",
  CancelAggregateQuery = "CancelAggregateQuery",
  ChangeAggregateColumns = "ChangeAggregateColumns",
  ChangeAggregateColumnsTopN = "ChangeAggregateColumnsTopN",
  ChangeAggregateFilter = "ChangeAggregateFilter",
  ChangeAggregateLimit = "ChangeAggregateLimit",
  ChangeAggregateMeasures = "ChangeAggregateMeasures",
  ChangeAggregateOffset = "ChangeAggregateOffset",
  ChangeAggregateOrder = "ChangeAggregateOrder",
  ChangeAggregatePostAggregateFilter = "ChangeAggregatePostAggregateFilter",
  ChangeAggregateRows = "ChangeAggregateRows",
  ChangeAggregateRowsTopN = "ChangeAggregateRowsTopN",
  ClearAggregateQuery = "ClearAggregateQuery",
  FreezeAggregateQueryEditing = "FreezeAggregateQueryEditing",
  ReceiveAggregateCompatibility = "ReceiveAggregateCompatibility",
  ReceiveAggregateQuery = "ReceiveAggregateQuery",
  ReceiveAggregateQueryLog = "ReceiveAggregateQueryLog",
  RedoAggregateQueryChange = "RedoAggregateQueryChange",
  RequestAggregateCompatibility = "RequestAggregateCompatibility",
  RequestAggregateQuery = "RequestAggregateQuery",
  RestoreAggregateQuery = "RestoreAggregateQuery",
  ToggleAggregateCodeView = "ToggleAggregateCodeView",
  TypeInAggregateCodeView = "TypeInAggregateCodeView",
  CheckAggregateCodeView = "CheckAggregateCodeView",
  UndoAggregateQueryChange = "UndoAggregateQueryChange",
  // Record
  BlurRecordFilterInput = "BlurRecordFilterInput",
  CancelRecordQuery = "CancelRecordQuery",
  ChangeRecordFilter = "ChangeRecordFilter",
  ChangeRecordLimit = "ChangeRecordLimit",
  ChangeRecordOffset = "ChangeRecordOffset",
  ChangeRecordOrder = "ChangeRecordOrder",
  ChangeRecordRows = "ChangeRecordRows",
  ClearRecordQuery = "ClearRecordQuery",
  FreezeRecordQueryEditing = "FreezeRecordQueryEditing",
  ReceiveRecordCompatibility = "ReceiveRecordCompatibility",
  ReceiveRecordQuery = "ReceiveRecordQuery",
  ReceiveRecordQueryLog = "ReceiveRecordQueryLog",
  RedoRecordQueryChange = "RedoRecordQueryChange",
  RequestRecordCompatibility = "RequestRecordCompatibility",
  RequestRecordQuery = "RequestRecordQuery",
  RestoreRecordQuery = "RestoreRecordQuery",
  ToggleRecordCodeView = "ToggleRecordCodeView",
  TypeInRecordCodeView = "TypeInRecordCodeView",
  CheckRecordCodeView = "CheckRecordCodeView",
  UndoRecordQueryChange = "UndoRecordQueryChange",
  // Sql
  CancelSqlQuery = "CancelSqlQuery",
  ChangeSqlQuery = "ChangeSqlQuery",
  ReceiveSqlQuery = "ReceiveSqlQuery",
  ReceiveSqlQueryLog = "ReceiveSqlQueryLog",
  ReceiveSqlTables = "ReceiveSqlTables",
  RequestSqlQuery = "RequestSqlQuery",
  RequestSqlTables = "RequestSqlTables",
  RestoreSqlQuery = "RestoreSqlQuery",
}

// ChangeQueryType

export interface ChangeQueryType {
  type: ActionType.ChangeQueryType;
  payload: QueryType;
}

export const changeQueryType = (payload: ChangeQueryType["payload"]): ChangeQueryType => ({
  type: ActionType.ChangeQueryType,
  payload,
});

// ReceiveQueryHistory

export interface ReceiveQueryHistory {
  type: ActionType.ReceiveQueryHistory;
  payload: ResponseOrError<QueryLog[]>;
}

export const receiveQueryHistory = (payload: ReceiveQueryHistory["payload"]): ReceiveQueryHistory => ({
  type: ActionType.ReceiveQueryHistory,
  payload,
});

// RestoreAggregateQuery

export interface RestoreAggregateQuery {
  type: ActionType.RestoreAggregateQuery;
  payload: AggregateQuery;
}

export const restoreAggregateQuery = (payload: RestoreAggregateQuery["payload"]): RestoreAggregateQuery => ({
  type: ActionType.RestoreAggregateQuery,
  payload,
});

// RequestAggregateQuery

export interface RequestAggregateQuery {
  type: ActionType.RequestAggregateQuery;
  time: UnixTime;
  queryId: string;
}

export const requestAggregateQuery = (time: UnixTime, queryId: string): RequestAggregateQuery => ({
  type: ActionType.RequestAggregateQuery,
  time,
  queryId,
});

// BlurAggregateFilterInput

export interface BlurAggregateFilterInput {
  type: ActionType.BlurAggregateFilterInput;
}

export const blurAggregateFilterInput = (): BlurAggregateFilterInput => ({
  type: ActionType.BlurAggregateFilterInput,
});

// BlurAggregatePostAggregateFilterInput

export interface BlurAggregatePostAggregateFilterInput {
  type: ActionType.BlurAggregatePostAggregateFilterInput;
}

export const blurAggregatePostAggregateFilterInput = (): BlurAggregatePostAggregateFilterInput => ({
  type: ActionType.BlurAggregatePostAggregateFilterInput,
});

// ToggleAggregateCodeView

export interface ToggleAggregateCodeView {
  type: ActionType.ToggleAggregateCodeView;
  payload: boolean;
}

export const toggleAggregateCodeView = (payload: ToggleAggregateCodeView["payload"]): ToggleAggregateCodeView => ({
  type: ActionType.ToggleAggregateCodeView,
  payload,
});

// TypeInAggregateCodeView

export interface TypeInAggregateCodeView {
  type: ActionType.TypeInAggregateCodeView;
  payload: string;
}

export const typeInAggregateCodeView = (payload: TypeInAggregateCodeView["payload"]): TypeInAggregateCodeView => ({
  type: ActionType.TypeInAggregateCodeView,
  payload,
});

// CheckAggregateCodeView

export interface CheckAggregateCodeView {
  type: ActionType.CheckAggregateCodeView;
}

export const checkAggregateCodeView = (): CheckAggregateCodeView => ({
  type: ActionType.CheckAggregateCodeView,
});

// CancelAggregateQuery

export interface CancelAggregateQuery {
  type: ActionType.CancelAggregateQuery;
}

export const cancelAggregateQuery = (): CancelAggregateQuery => ({
  type: ActionType.CancelAggregateQuery,
});

// ClearAggregateQuery

export interface ClearAggregateQuery {
  type: ActionType.ClearAggregateQuery;
}

export const clearAggregateQuery = (): ClearAggregateQuery => ({
  type: ActionType.ClearAggregateQuery,
});

// ReceiveAggregateQuery

export interface ReceiveAggregateQuery {
  type: ActionType.ReceiveAggregateQuery;
  payload: ResponseOrError<Response>;
  time: UnixTime;
}

export const receiveAggregateQuery = (time: UnixTime) => (
  payload: ReceiveAggregateQuery["payload"],
): ReceiveAggregateQuery => ({
  type: ActionType.ReceiveAggregateQuery,
  payload,
  time,
});

// ReceiveAggregateQueryLog

export interface ReceiveAggregateQueryLog {
  type: ActionType.ReceiveAggregateQueryLog;
  payload: QueryLog;
}

export const receiveAggregateQueryLog = (payload: ReceiveAggregateQueryLog["payload"]): ReceiveAggregateQueryLog => ({
  type: ActionType.ReceiveAggregateQueryLog,
  payload,
});

// ChangeAggregateColumns

export interface ChangeAggregateColumns {
  type: ActionType.ChangeAggregateColumns;
  payload: string[];
}

export const changeAggregateColumns = (payload: ChangeAggregateColumns["payload"]): ChangeAggregateColumns => ({
  type: ActionType.ChangeAggregateColumns,
  payload,
});

// ChangeAggregateMeasures

export interface ChangeAggregateMeasures {
  type: ActionType.ChangeAggregateMeasures;
  payload: string[];
}

export const changeAggregateMeasures = (payload: ChangeAggregateMeasures["payload"]): ChangeAggregateMeasures => ({
  payload,
  type: ActionType.ChangeAggregateMeasures,
});

// ChangeAggregateRows

export interface ChangeAggregateRows {
  type: ActionType.ChangeAggregateRows;
  payload: string[];
}

export const changeAggregateRows = (payload: ChangeAggregateRows["payload"]): ChangeAggregateRows => ({
  payload,
  type: ActionType.ChangeAggregateRows,
});

// ChangeAggregateOrder

export interface ChangeAggregateOrder {
  type: ActionType.ChangeAggregateOrder;
  payload: OrderedColumn[];
}

export const changeAggregateOrder = (payload: ChangeAggregateOrder["payload"]): ChangeAggregateOrder => ({
  payload,
  type: ActionType.ChangeAggregateOrder,
});

// ChangeAggregateFilter

export interface ChangeAggregateFilter {
  type: ActionType.ChangeAggregateFilter;
  payload: string;
}

export const changeAggregateFilter = (payload: ChangeAggregateFilter["payload"]): ChangeAggregateFilter => ({
  payload,
  type: ActionType.ChangeAggregateFilter,
});

// ChangeAggregatePostAggregateFilter

export interface ChangeAggregatePostAggregateFilter {
  type: ActionType.ChangeAggregatePostAggregateFilter;
  payload: string;
}

export const changeAggregatePostAggregateFilter = (
  payload: ChangeAggregatePostAggregateFilter["payload"],
): ChangeAggregatePostAggregateFilter => ({
  payload,
  type: ActionType.ChangeAggregatePostAggregateFilter,
});

// ChangeAggregateLimit

export interface ChangeAggregateLimit {
  type: ActionType.ChangeAggregateLimit;
  payload: number | undefined;
}

export const changeAggregateLimit = (payload: ChangeAggregateLimit["payload"]): ChangeAggregateLimit => ({
  payload,
  type: ActionType.ChangeAggregateLimit,
});

// ChangeAggregateOffset

export interface ChangeAggregateOffset {
  type: ActionType.ChangeAggregateOffset;
  payload: number;
}

export const changeAggregateOffset = (payload: ChangeAggregateOffset["payload"]): ChangeAggregateOffset => ({
  payload,
  type: ActionType.ChangeAggregateOffset,
});

// ChangeAggregateRowsTopN

export interface ChangeAggregateRowsTopN {
  type: ActionType.ChangeAggregateRowsTopN;
  payload: any;
}

export const changeAggregateRowsTopN = (payload: ChangeAggregateRowsTopN["payload"]): ChangeAggregateRowsTopN => ({
  type: ActionType.ChangeAggregateRowsTopN,
  payload,
});

// ChangeAggregateColumnsTopN

export interface ChangeAggregateColumnsTopN {
  type: ActionType.ChangeAggregateColumnsTopN;
  payload: any;
}

export const changeAggregateColumnsTopN = (
  payload: ChangeAggregateColumnsTopN["payload"],
): ChangeAggregateColumnsTopN => ({
  type: ActionType.ChangeAggregateColumnsTopN,
  payload,
});

// RequestAggregateCompatibility

export interface RequestAggregateCompatibility {
  type: ActionType.RequestAggregateCompatibility;
}

export const requestAggregateCompatibility = (): RequestAggregateCompatibility => ({
  type: ActionType.RequestAggregateCompatibility,
});

// ReceiveAggregateCompatibility

export interface ReceiveAggregateCompatibility {
  type: ActionType.ReceiveAggregateCompatibility;
  payload: ResponseOrError<Compatibility>;
}

export const receiveAggregateCompatibility = (
  payload: ReceiveAggregateCompatibility["payload"],
): ReceiveAggregateCompatibility => ({
  payload,
  type: ActionType.ReceiveAggregateCompatibility,
});

// FreezeAggregateQueryEditing

export interface FreezeAggregateQueryEditing {
  type: ActionType.FreezeAggregateQueryEditing;
}

export const freezeAggregateQueryEditing = (): FreezeAggregateQueryEditing => ({
  type: ActionType.FreezeAggregateQueryEditing,
});

// UndoAggregateQueryChange

export interface UndoAggregateQueryChange {
  type: ActionType.UndoAggregateQueryChange;
}

export const undoAggregateQueryChange = (): UndoAggregateQueryChange => ({
  type: ActionType.UndoAggregateQueryChange,
});

// RedoAggregateQueryChange

export interface RedoAggregateQueryChange {
  type: ActionType.RedoAggregateQueryChange;
}

export const redoAggregateQueryChange = (): RedoAggregateQueryChange => ({
  type: ActionType.RedoAggregateQueryChange,
});

// ToggleRecordCodeView

export interface ToggleRecordCodeView {
  type: ActionType.ToggleRecordCodeView;
  payload: boolean;
}

export const toggleRecordCodeView = (payload: ToggleRecordCodeView["payload"]): ToggleRecordCodeView => ({
  type: ActionType.ToggleRecordCodeView,
  payload,
});

// TypeInRecordCodeView

export interface TypeInRecordCodeView {
  type: ActionType.TypeInRecordCodeView;
  payload: string;
}

export const typeInRecordCodeView = (payload: TypeInRecordCodeView["payload"]): TypeInRecordCodeView => ({
  type: ActionType.TypeInRecordCodeView,
  payload,
});

// CheckRecordCodeView

export interface CheckRecordCodeView {
  type: ActionType.CheckRecordCodeView;
}

export const checkRecordCodeView = (): CheckRecordCodeView => ({
  type: ActionType.CheckRecordCodeView,
});

// CancelRecordQuery

export interface CancelRecordQuery {
  type: ActionType.CancelRecordQuery;
}

export const cancelRecordQuery = (): CancelRecordQuery => ({
  type: ActionType.CancelRecordQuery,
});

// BlurRecordFilterInput

export interface BlurRecordFilterInput {
  type: ActionType.BlurRecordFilterInput;
}

export const blurRecordFilterInput = (): BlurRecordFilterInput => ({
  type: ActionType.BlurRecordFilterInput,
});

// ChangeRecordRows

export interface ChangeRecordRows {
  type: ActionType.ChangeRecordRows;
  payload: string[];
}

export const changeRecordRows = (payload: ChangeRecordRows["payload"]): ChangeRecordRows => ({
  type: ActionType.ChangeRecordRows,
  payload,
});

// ChangeRecordFilter

export interface ChangeRecordFilter {
  type: ActionType.ChangeRecordFilter;
  payload: string;
}

export const changeRecordFilter = (payload: ChangeRecordFilter["payload"]): ChangeRecordFilter => ({
  type: ActionType.ChangeRecordFilter,
  payload,
});

// ChangeRecordOrder

export interface ChangeRecordOrder {
  type: ActionType.ChangeRecordOrder;
  payload: OrderedColumn[];
}

export const changeRecordOrder = (payload: ChangeRecordOrder["payload"]): ChangeRecordOrder => ({
  type: ActionType.ChangeRecordOrder,
  payload,
});

// ChangeRecordLimit

export interface ChangeRecordLimit {
  type: ActionType.ChangeRecordLimit;
  payload: number | undefined;
}

export const changeRecordLimit = (payload: ChangeRecordLimit["payload"]): ChangeRecordLimit => ({
  type: ActionType.ChangeRecordLimit,
  payload,
});

// ChangeRecordOffset

export interface ChangeRecordOffset {
  type: ActionType.ChangeRecordOffset;
  payload: number;
}

export const changeRecordOffset = (payload: ChangeRecordOffset["payload"]): ChangeRecordOffset => ({
  type: ActionType.ChangeRecordOffset,
  payload,
});

// RequestRecordCompatibility

export interface RequestRecordCompatibility {
  type: ActionType.RequestRecordCompatibility;
}

export const requestRecordCompatibility = (): RequestRecordCompatibility => ({
  type: ActionType.RequestRecordCompatibility,
});

// ReceiveRecordCompatibility

export interface ReceiveRecordCompatibility {
  type: ActionType.ReceiveRecordCompatibility;
  payload: ResponseOrError<CompatibilityField[]>;
}

export const receiveRecordCompatibility = (
  payload: ReceiveRecordCompatibility["payload"],
): ReceiveRecordCompatibility => ({
  type: ActionType.ReceiveRecordCompatibility,
  payload,
});

// RequestRecordQuery

export interface RequestRecordQuery {
  type: ActionType.RequestRecordQuery;
  time: UnixTime;
  queryId: string;
}

export const requestRecordQuery = (time: UnixTime, queryId: string): RequestRecordQuery => ({
  type: ActionType.RequestRecordQuery,
  time,
  queryId,
});

// ReceiveRecordQuery

export interface ReceiveRecordQuery {
  type: ActionType.ReceiveRecordQuery;
  payload: ResponseOrError<Response>;
  time: UnixTime;
}

export const receiveRecordQuery = (time: UnixTime) => (payload: ReceiveRecordQuery["payload"]): ReceiveRecordQuery => ({
  type: ActionType.ReceiveRecordQuery,
  payload,
  time,
});

// ReceiveRecordQueryLog

export interface ReceiveRecordQueryLog {
  type: ActionType.ReceiveRecordQueryLog;
  payload: QueryLog;
}

export const receiveRecordQueryLog = (payload: ReceiveRecordQueryLog["payload"]): ReceiveRecordQueryLog => ({
  type: ActionType.ReceiveRecordQueryLog,
  payload,
});

// UndoRecordQueryChange

export interface UndoRecordQueryChange {
  type: ActionType.UndoRecordQueryChange;
}

export const undoRecordQueryChange = (): UndoRecordQueryChange => ({
  type: ActionType.UndoRecordQueryChange,
});

// RedoRecordQueryChange

export interface RedoRecordQueryChange {
  type: ActionType.RedoRecordQueryChange;
}

export const redoRecordQueryChange = (): RedoRecordQueryChange => ({
  type: ActionType.RedoRecordQueryChange,
});

// ClearRecordQuery

export interface ClearRecordQuery {
  type: ActionType.ClearRecordQuery;
}

export const clearRecordQuery = (): ClearRecordQuery => ({
  type: ActionType.ClearRecordQuery,
});

// FreezeRecordQueryEditing

export interface FreezeRecordQueryEditing {
  type: ActionType.FreezeRecordQueryEditing;
}

export const freezeRecordQueryEditing = (): FreezeRecordQueryEditing => ({
  type: ActionType.FreezeRecordQueryEditing,
});

// RestoreRecordQuery

export interface RestoreRecordQuery {
  type: ActionType.RestoreRecordQuery;
  payload: RecordQuery;
}

export const restoreRecordQuery = (payload: RestoreRecordQuery["payload"]): RestoreRecordQuery => ({
  type: ActionType.RestoreRecordQuery,
  payload,
});

// ChangeSqlQuery

export interface ChangeSqlQuery {
  type: ActionType.ChangeSqlQuery;
  payload: string;
}

export const changeSqlQuery = (payload: ChangeSqlQuery["payload"]): ChangeSqlQuery => ({
  type: ActionType.ChangeSqlQuery,
  payload,
});

// RequestSqlQuery

export interface RequestSqlQuery {
  type: ActionType.RequestSqlQuery;
  time: UnixTime;
  queryId: string;
}

export const requestSqlQuery = (time: UnixTime, queryId: string): RequestSqlQuery => ({
  type: ActionType.RequestSqlQuery,
  time,
  queryId,
});

// CancelSqlQuery

export interface CancelSqlQuery {
  type: ActionType.CancelSqlQuery;
}

export const cancelSqlQuery = (): CancelSqlQuery => ({
  type: ActionType.CancelSqlQuery,
});

// ReceiveSqlQuery

export interface ReceiveSqlQuery {
  type: ActionType.ReceiveSqlQuery;
  payload: ResponseOrError<Response>;
  time: UnixTime;
}

export const receiveSqlQuery = (time: UnixTime) => (payload: ResponseOrError<Response>): ReceiveSqlQuery => ({
  type: ActionType.ReceiveSqlQuery,
  payload,
  time,
});

// ReceiveSqlQueryLog

export interface ReceiveSqlQueryLog {
  type: ActionType.ReceiveSqlQueryLog;
  payload: QueryLog;
}

export const receiveSqlQueryLog = (payload: ReceiveSqlQueryLog["payload"]): ReceiveSqlQueryLog => ({
  type: ActionType.ReceiveSqlQueryLog,
  payload,
});

// RestoreSqlQuery

export interface RestoreSqlQuery {
  type: ActionType.RestoreSqlQuery;
  payload: SqlQuery;
}

export const restoreSqlQuery = (payload: RestoreSqlQuery["payload"]): RestoreSqlQuery => ({
  type: ActionType.RestoreSqlQuery,
  payload,
});

// RequestSqlTables

export interface RequestSqlTables {
  type: ActionType.RequestSqlTables;
}

export const requestSqlTables = (): RequestSqlTables => ({
  type: ActionType.RequestSqlTables,
});

// ReceiveSqlTables

export interface ReceiveSqlTables {
  type: ActionType.ReceiveSqlTables;
  payload: ResponseOrError<TableResponse[]>;
}

export const receiveSqlTables = (payload: ResponseOrError<TableResponse[]>): ReceiveSqlTables => ({
  type: ActionType.ReceiveSqlTables,
  payload,
});

export type Action =
  // General
  | ChangeQueryType
  | ReceiveQueryHistory
  // Aggregate
  | BlurAggregateFilterInput
  | BlurAggregatePostAggregateFilterInput
  | CancelAggregateQuery
  | ChangeAggregateColumns
  | ChangeAggregateColumnsTopN
  | ChangeAggregateFilter
  | ChangeAggregateLimit
  | ChangeAggregateMeasures
  | ChangeAggregateOffset
  | ChangeAggregateOrder
  | ChangeAggregatePostAggregateFilter
  | ChangeAggregateRows
  | ChangeAggregateRowsTopN
  | ClearAggregateQuery
  | FreezeAggregateQueryEditing
  | ReceiveAggregateCompatibility
  | ReceiveAggregateQuery
  | ReceiveAggregateQueryLog
  | RedoAggregateQueryChange
  | RequestAggregateCompatibility
  | RequestAggregateQuery
  | RestoreAggregateQuery
  | ToggleAggregateCodeView
  | TypeInAggregateCodeView
  | CheckAggregateCodeView
  | UndoAggregateQueryChange
  // Record
  | BlurRecordFilterInput
  | CancelRecordQuery
  | ChangeRecordFilter
  | ChangeRecordLimit
  | ChangeRecordOffset
  | ChangeRecordOrder
  | ChangeRecordRows
  | ClearRecordQuery
  | FreezeRecordQueryEditing
  | ReceiveRecordCompatibility
  | ReceiveRecordQuery
  | ReceiveRecordQueryLog
  | RedoRecordQueryChange
  | RequestRecordCompatibility
  | RequestRecordQuery
  | RestoreRecordQuery
  | ToggleRecordCodeView
  | TypeInRecordCodeView
  | CheckRecordCodeView
  | UndoRecordQueryChange
  // Sql
  | CancelSqlQuery
  | ChangeSqlQuery
  | ReceiveSqlQuery
  | ReceiveSqlQueryLog
  | ReceiveSqlTables
  | RequestSqlQuery
  | RequestSqlTables
  | RestoreSqlQuery;
