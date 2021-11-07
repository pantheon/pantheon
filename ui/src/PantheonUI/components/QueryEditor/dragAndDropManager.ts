/**
 * This file is responsible of all drag & drop logic of the query editor.
 *
 * Rules:
 *  - all draggableId and droppableId must be unique
 *    -> we use namespace to help us here
 *  - we have a main "dispatcher" to deal with drag & drop
 *  - droppableId and draggable namespace are equal
 */

import { ModalConfirmContext } from "@operational/components";
import { DropResult } from "react-beautiful-dnd";
import { AddFilterModal } from "./AddFilterModal";
import { QueryEditorActions, QueryEditorState } from "./QueryEditor.reducer";

type DraggableNamespace =
  | "inventoryMeasure"
  | "inventoryDimension"
  | "aggregateMeasure"
  | "aggregateRow"
  | "aggregateColumn"
  | "aggregateOrder"
  | "aggregateFilter";

export const getDroppableId = (namespace: DraggableNamespace, id: string = "") => `[${namespace}]${id}`;
export const getDroppableNamespace = (droppableId: string) => droppableId.split("]")[0].slice(1) as DraggableNamespace;

export const getDraggableId = (namespace: DraggableNamespace, ref: string) => `[${namespace}]${ref}`;
export const getDraggableRef = (draggableId: string) => draggableId.split("]")[1];
export const getDraggableNamespace = (draggableId: string) => draggableId.split("]")[0].slice(1);

export const mapDragEndToDispatch = (
  dispatch: React.Dispatch<QueryEditorActions>,
  confirm: ModalConfirmContext["confirm"],
) => (result: DropResult) => {
  dispatch({ type: "[draggable] onDragEnd" });
  const source = getDroppableNamespace(result.source.droppableId);
  const ref = getDraggableRef(result.draggableId);

  if (result.destination) {
    const destination = getDroppableNamespace(result.destination.droppableId);
    const action = destination === source ? "reorder" : "add";

    switch (destination) {
      case "aggregateMeasure":
        if (action === "add") {
          return dispatch({ type: "[aggregate] addMeasure", payload: { ref, index: result.destination.index } });
        } else {
          return dispatch({
            type: "[aggregate] reorderMeasure",
            payload: { ref, startIndex: result.source.index, endIndex: result.destination.index },
          });
        }
      case "aggregateRow":
        if (action === "add") {
          if (source === "aggregateColumn") {
            dispatch({ type: "[aggregate] removeColumn", payload: { ref } });
          }
          return dispatch({ type: "[aggregate] addRow", payload: { ref, index: result.destination.index } });
        } else {
          return dispatch({
            type: "[aggregate] reorderRow",
            payload: { ref, startIndex: result.source.index, endIndex: result.destination.index },
          });
        }
      case "aggregateColumn":
        if (action === "add") {
          if (source === "aggregateRow") {
            dispatch({ type: "[aggregate] removeRow", payload: { ref } });
          }
          return dispatch({ type: "[aggregate] addColumn", payload: { ref, index: result.destination.index } });
        } else {
          return dispatch({
            type: "[aggregate] reorderColumn",
            payload: { ref, startIndex: result.source.index, endIndex: result.destination.index },
          });
        }
      case "aggregateOrder":
        if (action === "add") {
          return dispatch({ type: "[aggregate] addOrderBy", payload: { ref, index: result.destination.index } });
        } else {
          return dispatch({
            type: "[aggregate] reorderOrderBy",
            payload: { ref, startIndex: result.source.index, endIndex: result.destination.index },
          });
        }
      case "aggregateFilter":
        if (action === "add") {
          confirm({
            title: `Configure filter for ${ref}`,
            body: AddFilterModal,
            state: {
              dimension: ref,
              type: "=",
              value: "",
            },
            onConfirm: state =>
              dispatch({
                type: "[aggregate] addFilterPredicate",
                payload: {
                  dimension: ref,
                  index: result.destination ? result.destination.index : 0,
                  operator: state.type,
                  value: state.value,
                  type: "predicate",
                },
              }),
          });
        } else {
          return dispatch({
            type: "[aggregate] reorderFilterPredicate",
            payload: { startIndex: result.source.index, endIndex: result.destination.index },
          });
        }
    }
  }
};

export const isAggregateMeasuresDroppable = (state: QueryEditorState) => {
  if (!state.dragItem) {
    return false;
  }
  const source = getDroppableNamespace(state.dragItem.draggableId);
  const ref = getDraggableRef(state.dragItem.draggableId);
  const allowedNamespace: DraggableNamespace[] = ["inventoryMeasure", "aggregateMeasure"];
  const isDuplicate = source !== "aggregateMeasure" && state.aggregate.query.measures.includes(ref);

  return allowedNamespace.includes(source) && !isDuplicate;
};

export const isAggregateRowsDroppable = (state: QueryEditorState) => {
  if (!state.dragItem) {
    return false;
  }
  const source = getDroppableNamespace(state.dragItem.draggableId);
  const ref = getDraggableRef(state.dragItem.draggableId);
  const allowedNamespace: DraggableNamespace[] = ["inventoryDimension", "aggregateColumn", "aggregateRow"];
  const isDuplicate = source !== "aggregateRow" && state.aggregate.query.rows.includes(ref);

  return allowedNamespace.includes(source) && !isDuplicate;
};

export const isAggregateColumnsDroppable = (state: QueryEditorState) => {
  if (!state.dragItem) {
    return false;
  }
  const source = getDroppableNamespace(state.dragItem.draggableId);
  const ref = getDraggableRef(state.dragItem.draggableId);
  const allowedNamespace: DraggableNamespace[] = ["inventoryDimension", "aggregateColumn", "aggregateRow"];
  const isDuplicate = source !== "aggregateColumn" && state.aggregate.query.columns.includes(ref);

  return allowedNamespace.includes(source) && !isDuplicate;
};

export const isAggregateOrderByDroppable = (state: QueryEditorState) => {
  if (!state.dragItem) {
    return false;
  }
  const source = getDroppableNamespace(state.dragItem.draggableId);
  const ref = getDraggableRef(state.dragItem.draggableId);
  const allowedNamespace: DraggableNamespace[] = [
    "inventoryDimension",
    "inventoryMeasure",
    "aggregateRow",
    "aggregateColumn",
    "aggregateOrder",
  ];
  const isDuplicate =
    source !== "aggregateOrder" && (state.aggregate.query.orderBy || []).map(i => i.name).includes(ref);

  return allowedNamespace.includes(source) && !isDuplicate;
};
