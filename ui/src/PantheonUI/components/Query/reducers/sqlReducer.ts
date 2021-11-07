import { makeClearSqlQuery } from "../../../data/sqlQuery";
import { Action, ActionType } from "../actions";
import { State } from "../state";

export const makeDefaultSqlState = () => ({
  isQueryCancelled: false,
  query: makeClearSqlQuery(),
});

const sqlReducer = (state: State["Sql"] = makeDefaultSqlState(), action: Action): State["Sql"] => {
  switch (action.type) {
    case ActionType.ChangeSqlQuery:
      return {
        ...state,
        query: {
          ...state.query,
          sql: action.payload,
        },
      };
    case ActionType.RequestSqlQuery:
      return {
        ...state,
        isQueryCancelled: false,
        request: {
          requestedAt: action.time,
          queryId: action.queryId,
          query: state.query,
          response: undefined,
        },
      };
    case ActionType.ReceiveSqlQuery: {
      return {
        ...state,
        request: state.request
          ? {
              ...state.request,
              response: {
                payload: action.payload,
                receivedAt: action.time,
              },
            }
          : state.request,
      };
    }
    case ActionType.ReceiveSqlQueryLog: {
      return {
        ...state,
        request: state.request
          ? {
              ...state.request,
              queryLog: action.payload,
            }
          : state.request,
      };
    }
    case ActionType.CancelSqlQuery:
      return {
        ...state,
        request: undefined,
        isQueryCancelled: true,
      };
    case ActionType.RestoreSqlQuery:
      return {
        ...state,
        query: action.payload,
      };
    case ActionType.RequestSqlTables:
      return state;

    case ActionType.ReceiveSqlTables:
      return {
        ...state,
        tables: action.payload,
      };
  }
  return state;
};

export default sqlReducer;
