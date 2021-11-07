import isEqual from "lodash/isEqual";
import { initialAggregateState } from "./aggregate.reducer";
import { QueryEditorActions, QueryEditorState } from "./QueryEditor.reducer";
import { initialRecordState } from "./record.reducer";

interface History {
  canUndo: boolean;
  canRedo: boolean;
  canClear: boolean;
  clearAction: QueryEditorActions;
  undoAction: QueryEditorActions;
  redoAction: QueryEditorActions;
}

export const getHistory = (state: QueryEditorState): History | undefined => {
  switch (state.queryType) {
    case "Aggregate":
      return {
        canUndo: state.aggregate.previousQueries.length > 0,
        canRedo: state.aggregate.nextQueries.length > 0,
        canClear: !isEqual(state.aggregate.query, initialAggregateState.query),
        clearAction: { type: "[aggregate] clearQuery" },
        undoAction: { type: "[aggregate] undo" },
        redoAction: { type: "[aggregate] redo" },
      };
    case "Record":
      return {
        canUndo: state.record.previousQueries.length > 0,
        canRedo: state.record.nextQueries.length > 0,
        canClear: !isEqual(state.record.query, initialRecordState.query),
        clearAction: { type: "[record] clearQuery" },
        undoAction: { type: "[record] undo" },
        redoAction: { type: "[record] redo" },
      };
    case "Sql":
      return undefined;
  }
};

/**
 * Get the current query as a string
 *
 * @param state
 * @param withType Add type field (used for query the API)
 */
export const getQuery = (state: QueryEditorState, withType?: boolean) => {
  switch (state.queryType) {
    case "Aggregate":
      return withType
        ? JSON.stringify({ ...state.aggregate.query, type: "Aggregate" })
        : JSON.stringify(state.aggregate.query, null, 2);
    case "Record":
      return withType
        ? JSON.stringify({ ...state.record.query, type: "Record" })
        : JSON.stringify(state.record.query, null, 2);
    case "Sql":
      return state.sql;
  }
};
