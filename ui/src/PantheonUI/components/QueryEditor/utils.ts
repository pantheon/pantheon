import { TreeProps } from "@operational/components";
import * as t from "io-ts";
import groupBy from "lodash/groupBy";
import map from "lodash/map";
import { Dispatch } from "react";
import { PslColor } from "../../utils";
import { CompatibilityResponse } from "../queries";
import { getDraggableId, getDroppableId } from "./dragAndDropManager";
import { QueryEditorActions } from "./QueryEditor.reducer";

/**
 * Convert measures to `Tree` props
 *
 * @param measures
 */
export const measuresToTree = (
  measures: CompatibilityResponse["measures"],
  dispatch: Dispatch<QueryEditorActions>,
  compatibleRefs: string[],
): TreeProps["trees"] =>
  measures.map(m => ({
    key: m.ref,
    label: m.ref,
    color: PslColor.measure,
    disabled: !compatibleRefs.includes(m.ref),
    tag: "M",
    onClick: () => dispatch({ type: "[aggregate] addMeasure", payload: { ref: m.ref } }),
    cursor: "grab",
    draggableProps: { draggableId: getDraggableId("inventoryMeasure", m.ref) },
  }));

/**
 * Convert dimensions to `Tree` props
 *
 * @param dimensions
 */
export const dimensionsToTree = (
  dimensions: CompatibilityResponse["dimensions"],
  dispatch: Dispatch<QueryEditorActions>,
  compatibleRefs: string[],
): TreeProps["trees"] =>
  map(groupBy(dimensions, "dimension"), (attributes, dimensionName) => ({
    label: dimensionName,
    key: dimensionName,
    color: PslColor.dimension,
    initiallyOpen: true,
    droppableProps: { droppableId: getDroppableId("inventoryDimension", dimensionName), isDropDisabled: true },
    childNodes: attributes.map(a => ({
      tag: "D",
      key: a.ref,
      label: a.ref.replace(/^\w*\./, ""), // ex: `Customer.city` -> `city` (remove the dimensionName)
      color: PslColor.dimension,
      disabled: !compatibleRefs.includes(a.ref),
      onClick: () => dispatch({ type: "[aggregate] addRow", payload: { ref: a.ref } }),
      cursor: "grab",
      draggableProps: { draggableId: getDraggableId("inventoryDimension", a.ref) },
    })),
  }));

/**
 * AggregateQuery runtime encoder/decoder
 */
export const AggregateQuery = t.intersection([
  t.type({
    rows: t.array(t.string),
    columns: t.array(t.string),
    measures: t.array(t.string),
    offset: t.number,
    limit: t.number,
  }),
  t.partial({
    filter: t.string,
    orderBy: t.array(
      t.type({
        name: t.string,
        order: t.union([t.literal("Desc"), t.literal("Asc")]),
      }),
    ),
    postFilter: t.string,
  }),
]);

/**
 * RecordQuery runtime encoder/decoder
 */
export const RecordQuery = t.type({
  rows: t.array(t.string),
  offset: t.number,
  limit: t.number,
});
