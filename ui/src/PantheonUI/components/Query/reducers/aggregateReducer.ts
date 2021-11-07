import * as tucson from "tucson-decode";

import { AggregateQuery, aggregateQueryDecoder, makeClearAggregateQuery } from "../../../data/aggregateQuery";
import { JSONValue, safeJsonParse } from "../../../utils/dataProcessing";
import * as result from "../../../utils/result";
import * as undoable from "../../../utils/undoable";
import { Action, ActionType } from "../actions";
import { AggregateState } from "../state";

export const makeDefaultAggregateState = (): AggregateState => ({
  isQueryCancelled: false,
  isCompatibilityResponsePending: false,
  compatibility: result.success({ measures: [], dimensions: [] }),
  query: undoable.of(makeClearAggregateQuery()),
});

const aggregateReducer = (state: AggregateState = makeDefaultAggregateState(), action: Action): AggregateState => {
  const presentQuery = undoable.getPresent(state.query);
  switch (action.type) {
    case ActionType.RequestAggregateQuery:
      return {
        ...state,
        isQueryCancelled: false,
        request: {
          requestedAt: action.time,
          queryId: action.queryId,
          query: undoable.getPresent(state.query),
          response: undefined,
        },
      };
    case ActionType.CancelAggregateQuery:
      return {
        ...state,
        /**
         * Remove the last query from the query response history. This is the only query that has an absent
         * `response` field, and since it will no longer be in state when the query response arrives,
         * that response is never added to the state (see logic at `RunQueryResponse` below).
         */
        request: undefined,
        isQueryCancelled: true,
      };
    case ActionType.RestoreAggregateQuery:
      return {
        ...state,
        query: undoable.clearHistory(undoable.setPresent(action.payload, state.query)),
      };
    case ActionType.ClearAggregateQuery:
      return {
        ...state,
        query: undoable.setPresent(makeClearAggregateQuery(), state.query),
        request: undefined,
      };
    case ActionType.ReceiveAggregateQuery: {
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
    case ActionType.ReceiveAggregateQueryLog: {
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
    case ActionType.ChangeAggregateColumns:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            columns: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.ChangeAggregateRows:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            rows: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.ChangeAggregateMeasures:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            measures: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.ChangeAggregateFilter:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            filter: action.payload === "" ? undefined : action.payload,
          },
          state.query,
        ),
      };

    case ActionType.ChangeAggregatePostAggregateFilter:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            aggregateFilter: action.payload === "" ? undefined : action.payload,
          },
          state.query,
        ),
      };

    case ActionType.ChangeAggregateOrder:
      return {
        ...state,
        query: undoable.setPresent<AggregateQuery>(
          {
            ...presentQuery,
            orderBy: action.payload,
          },
          state.query,
        ),
      };

    case ActionType.ChangeAggregateRowsTopN:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            rowsTopN: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.ChangeAggregateColumnsTopN:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            columnsTopN: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.ChangeAggregateOffset:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            offset: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.ChangeAggregateLimit:
      /**
       * Casting is necessary because tsc has trouble recognizing that string can
       * be set in a field that is typed string | undefined. This issue popped up
       * after Undoable was introduced.
       */
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            limit: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.UndoAggregateQueryChange:
      return {
        ...state,
        query: undoable.undo(state.query),
      };
    case ActionType.RedoAggregateQueryChange:
      return {
        ...state,
        query: undoable.redo(state.query),
      };
    case ActionType.RequestAggregateCompatibility:
      return {
        ...state,
      };
    case ActionType.FreezeAggregateQueryEditing:
      return {
        ...state,
        isCompatibilityResponsePending: true,
      };
    case ActionType.ReceiveAggregateCompatibility:
      return {
        ...state,
        isCompatibilityResponsePending: false,
        compatibility: action.payload,
      };
    case ActionType.ToggleAggregateCodeView:
      return {
        ...state,
        codeView: action.payload
          ? {
              queryDraft: JSON.stringify(undoable.getPresent(state.query), undefined, 2),
            }
          : undefined,
      };
    case ActionType.CheckAggregateCodeView: {
      if (!state.codeView) {
        return state;
      }
      const jsonValue = result.mapError(safeJsonParse(String(state.codeView.queryDraft)), () => [
        {
          path: [],
          received: state.codeView!.queryDraft,
          error: "Invalid JSON",
        },
      ]);
      const decodedQuery = result.flatMap<tucson.DecodeError[], JSONValue, AggregateQuery>(
        jsonValue,
        aggregateQueryDecoder,
      );
      return decodedQuery.type === "error"
        ? {
            ...state,
            codeView: {
              ...state.codeView,
              decodeErrors: decodedQuery.value,
            },
          }
        : {
            ...state,
            query: undoable.setPresent(decodedQuery.value, state.query),
            codeView: {
              ...state.codeView,
              decodeErrors: undefined,
            },
          };
    }
    case ActionType.TypeInAggregateCodeView:
      return {
        ...state,
        codeView: {
          ...state.codeView,
          queryDraft: action.payload,
        },
      };
  }
  return state;
};

export default aggregateReducer;
