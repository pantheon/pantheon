import { Reducer } from "react";

import { Schema } from "../queries";

import {
  initialAggregateState,
  isQueryEditorAggregateActions,
  QueryEditorAggregateActions,
  queryEditorAggregateReducer,
  QueryEditorAggregateState,
} from "./aggregate.reducer";

import {
  initialRecordState,
  isQueryEditorRecordActions,
  QueryEditorRecordActions,
  queryEditorRecordReducer,
  QueryEditorRecordState,
} from "./record.reducer";

import { DragStart } from "react-beautiful-dnd";

export interface QueryEditorState {
  schema: Schema;
  queryType: "Aggregate" | "Record" | "Sql";
  aggregate: QueryEditorAggregateState;
  record: QueryEditorRecordState;
  sql: string;
  dragItem?: DragStart;
}

export const initialState: QueryEditorState = {
  queryType: "Aggregate",
  aggregate: initialAggregateState,
  record: initialRecordState,
  sql: "",
  schema: {
    catalogId: "",
    description: "",
    id: "",
    name: "",
    psl: "",
  },
};

export type QueryEditorActions =
  | QueryEditorAggregateActions
  | QueryEditorRecordActions
  | { type: "updateQueryType"; payload: QueryEditorState["queryType"] }
  | { type: "[sql] updateQuery"; payload: string }
  | { type: "[draggable] onDragStart"; payload: DragStart }
  | { type: "[draggable] onDragEnd" };

export type Dispatch = React.Dispatch<QueryEditorActions>;

export const queryEditorReducer: Reducer<QueryEditorState, QueryEditorActions> = (prevState, action) => {
  if (isQueryEditorAggregateActions(action)) {
    return { ...prevState, aggregate: queryEditorAggregateReducer(prevState.aggregate, action) };
  }
  if (isQueryEditorRecordActions(action)) {
    return { ...prevState, record: queryEditorRecordReducer(prevState.record, action) };
  }

  switch (action.type) {
    case "updateQueryType":
      return { ...prevState, queryType: action.payload };
    case "[sql] updateQuery":
      return { ...prevState, sql: action.payload };
    case "[draggable] onDragStart":
      return { ...prevState, dragItem: action.payload };
    case "[draggable] onDragEnd":
      return { ...prevState, dragItem: undefined };
  }

  return prevState;
};
