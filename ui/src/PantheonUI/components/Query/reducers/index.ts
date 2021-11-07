import { Action, ActionType } from "../actions";
import { State } from "../state";
import aggregateReducer, { makeDefaultAggregateState } from "./aggregateReducer";
import recordReducer, { makeDefaultRecordState } from "./recordReducer";
import sqlReducer, { makeDefaultSqlState } from "./sqlReducer";

export const queryTypeReducer = (state: State["queryType"] = "Aggregate", action: Action): State["queryType"] => {
  switch (action.type) {
    case ActionType.ChangeQueryType:
      return action.payload;
  }
  return state;
};

/**
 * The logic inside this function resembles what `combineReducers` would do. It is handled here
 * manually because state objects are not completely unrelated to support query persistance
 * independent to the currently selected query type. The extra logic handles this coupling.
 */
export const reducer = (
  state: State = {
    queryType: "Aggregate",
    Aggregate: makeDefaultAggregateState(),
    Sql: makeDefaultSqlState(),
    Record: makeDefaultRecordState(),
  },
  action: Action,
): State => ({
  queryType: (() => {
    /**
     * This is an extra set of logic on top of reducer logic already handled on
     * query restoration, so these two actions are mapped on twice.
     */
    switch (action.type) {
      case ActionType.RestoreAggregateQuery:
        return "Aggregate";
      case ActionType.RestoreSqlQuery:
        return "Sql";
      case ActionType.RestoreRecordQuery:
        return "Record";
      default:
        return queryTypeReducer(state.queryType, action);
    }
  })(),
  Aggregate: aggregateReducer(state.Aggregate, action),
  Sql: sqlReducer(state.Sql, action),
  Record: recordReducer(state.Record, action),
});
