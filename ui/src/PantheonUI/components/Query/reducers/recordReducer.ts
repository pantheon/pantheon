import * as tucson from "tucson-decode";

import { makeClearRecordQuery, RecordQuery, recordQueryDecoder } from "../../../data/recordQuery";
import { JSONValue, safeJsonParse } from "../../../utils/dataProcessing";
import * as result from "../../../utils/result";
import * as undoable from "../../../utils/undoable";
import { Action, ActionType } from "../actions";
import { RecordState } from "../state";

export const makeDefaultRecordState = (): RecordState => ({
  isQueryCancelled: false,
  isCompatibilityResponsePending: false,
  compatibility: result.success([]),
  query: undoable.of(makeClearRecordQuery()),
});

const recordReducer = (state: RecordState = makeDefaultRecordState(), action: Action): RecordState => {
  const presentQuery = undoable.getPresent(state.query);
  switch (action.type) {
    case ActionType.RequestRecordQuery:
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
    case ActionType.CancelRecordQuery:
      return {
        ...state,
        request: undefined,
        isQueryCancelled: true,
      };
    case ActionType.RestoreRecordQuery:
      return {
        ...state,
        query: undoable.clearHistory(undoable.setPresent(action.payload, state.query)),
      };
    case ActionType.ClearRecordQuery:
      return {
        ...state,
        query: undoable.setPresent(makeClearRecordQuery(), state.query),
        request: undefined,
      };
    case ActionType.ReceiveRecordQuery: {
      return state.request
        ? {
            ...state,
            request: {
              ...state.request,
              response: {
                payload: action.payload,
                receivedAt: action.time,
              },
            },
          }
        : state;
    }
    case ActionType.ReceiveRecordQueryLog: {
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
    case ActionType.ChangeRecordFilter:
      return {
        ...state,
        query: undoable.setPresent(
          {
            ...presentQuery,
            filter: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.ChangeRecordOrder:
      return {
        ...state,
        query: undoable.setPresent<RecordQuery>(
          {
            ...presentQuery,
            orderBy: action.payload,
          },
          state.query,
        ),
      };
    case ActionType.ChangeRecordOffset:
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
    case ActionType.ChangeRecordRows:
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
    case ActionType.ChangeRecordLimit:
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
    case ActionType.UndoRecordQueryChange:
      return {
        ...state,
        query: undoable.undo(state.query),
      };
    case ActionType.RedoRecordQueryChange:
      return {
        ...state,
        query: undoable.redo(state.query),
      };
    case ActionType.RequestRecordCompatibility:
      return {
        ...state,
      };
    case ActionType.FreezeRecordQueryEditing:
      return {
        ...state,
        isCompatibilityResponsePending: true,
      };
    case ActionType.ReceiveRecordCompatibility:
      return {
        ...state,
        isCompatibilityResponsePending: false,
        compatibility: action.payload,
      };
    case ActionType.ToggleRecordCodeView:
      return {
        ...state,
        codeView: action.payload
          ? {
              queryDraft: JSON.stringify(undoable.getPresent(state.query), undefined, 2),
            }
          : undefined,
      };
    case ActionType.CheckRecordCodeView: {
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
      const decodedQuery = result.flatMap<tucson.DecodeError[], JSONValue, RecordQuery>(jsonValue, recordQueryDecoder);
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
    case ActionType.TypeInRecordCodeView:
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

export default recordReducer;
