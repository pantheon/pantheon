import { QueryEditorActions } from "./QueryEditor.reducer";

import isEmpty from "lodash/isEmpty";
import isEqual from "lodash/isEqual";
import { Reducer } from "react";

export interface RecordQuery {
  rows: string[];
  offset: number;
  limit: number;
}

export interface QueryEditorRecordState {
  mode: "visual" | "code";
  query: RecordQuery;
  previousQueries: RecordQuery[];
  nextQueries: RecordQuery[];
}

export const initialRecordState: QueryEditorRecordState = {
  mode: "visual",
  query: {
    rows: [],
    offset: 0,
    limit: 10,
  },
  previousQueries: [],
  nextQueries: [],
};

export const isQueryEditorRecordActions = (action: QueryEditorActions): action is QueryEditorRecordActions =>
  action.type.startsWith("[record]");

export type QueryEditorRecordActions =
  | { type: "[record] updateQuery"; payload: RecordQuery }
  | { type: "[record] clearQuery" }
  | { type: "[record] undo" }
  | { type: "[record] redo" }
  | { type: "[record] switchMode"; payload: QueryEditorRecordState["mode"] };

export const queryEditorRecordReducer: Reducer<QueryEditorRecordState, QueryEditorRecordActions> = (
  prevState,
  action,
) => {
  switch (action.type) {
    case "[record] clearQuery":
      return {
        ...prevState,
        query: initialRecordState.query,
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    case "[record] updateQuery":
      if (isEqual(prevState.query, action.payload)) {
        return prevState; // prevent duplicate queries in the history
      }
      return {
        ...prevState,
        query: action.payload,
        previousQueries: [...prevState.previousQueries, prevState.query],
        nextQueries: [],
      };
    case "[record] undo":
      if (isEmpty(prevState.previousQueries)) {
        return prevState;
      } else {
        return {
          ...prevState,
          query: prevState.previousQueries[prevState.previousQueries.length - 1],
          previousQueries: prevState.previousQueries.slice(0, -1),
          nextQueries: [prevState.query, ...prevState.nextQueries],
        };
      }
    case "[record] redo":
      if (isEmpty(prevState.nextQueries)) {
        return prevState;
      } else {
        return {
          ...prevState,
          query: prevState.nextQueries[0],
          previousQueries: [prevState.query, ...prevState.previousQueries],
          nextQueries: prevState.nextQueries.slice(1),
        };
      }
    case "[record] switchMode":
      return {
        ...prevState,
        mode: action.payload,
      };
    default:
      return prevState;
  }
};
