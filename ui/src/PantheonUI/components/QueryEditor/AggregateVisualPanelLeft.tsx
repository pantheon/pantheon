import { Card, CardSection, Input, Tree } from "@operational/components";
import React, { useCallback, useContext } from "react";
import { QueryEditorContext } from ".";
import { PslColor } from "../../utils";
import { QueryEditorAggregateState } from "./aggregate.reducer";

import {
  getDraggableId,
  getDroppableId,
  isAggregateMeasuresDroppable,
  isAggregateOrderByDroppable,
  isAggregateRowsDroppable,
} from "./dragAndDropManager";

import { PslFilter } from "../PslFilter";
import { DropDisabledEffect } from "./DropDisabledEffect";
import { DimensionDropZone, MeasureDropZone, OrderDropZone } from "./DropZones";

export const AggregateVisualPanelLeft: React.FC = () => {
  const { state, dispatch } = useContext(QueryEditorContext);

  const toggleSection = useCallback(
    (payload: keyof QueryEditorAggregateState["sections"]) => () =>
      dispatch({ type: "[aggregate] toggleSection", payload }),
    [],
  );

  return (
    <Card
      className="expanded" // flag for BaseLayout
      sections={
        <>
          <CardSection
            title="Measures"
            collapsed={state.aggregate.sections.measures}
            onToggle={toggleSection("measures")}
          >
            {state.dragItem && !isAggregateMeasuresDroppable(state) && <DropDisabledEffect />}
            <Tree
              droppableProps={{
                droppableId: getDroppableId("aggregateMeasure"),
                isDropDisabled: !isAggregateMeasuresDroppable(state),
              }}
              trees={state.aggregate.query.measures.map(m => ({
                label: m,
                key: m,
                color: PslColor.measure,
                tag: "M",
                onRemove: () => dispatch({ type: "[aggregate] removeMeasure", payload: { ref: m } }),
                draggableProps: { draggableId: getDraggableId("aggregateMeasure", m) },
              }))}
              placeholder={MeasureDropZone}
            />
          </CardSection>
          <CardSection title="Rows" collapsed={state.aggregate.sections.rows} onToggle={toggleSection("rows")}>
            {state.dragItem && !isAggregateRowsDroppable(state) && <DropDisabledEffect />}
            <Tree
              droppableProps={{
                droppableId: getDroppableId("aggregateRow"),
                isDropDisabled: !isAggregateRowsDroppable(state),
              }}
              trees={state.aggregate.query.rows.map(m => ({
                label: m,
                key: m,
                color: PslColor.dimension,
                tag: "D",
                onClick: () => dispatch({ type: "[aggregate] switchRowColumn", payload: { ref: m } }),
                cursor: "drag",
                onRemove: () => dispatch({ type: "[aggregate] removeRow", payload: { ref: m } }),
                draggableProps: { draggableId: getDraggableId("aggregateRow", m) },
              }))}
              placeholder={DimensionDropZone}
            />
          </CardSection>
          <CardSection
            title="Top N Rows"
            collapsed={state.aggregate.sections.topNRows}
            onToggle={toggleSection("topNRows")}
          />
          <PslFilter title="Filters" collapsed={state.aggregate.sections.filters} onToggle={toggleSection("filters")} />
          <CardSection
            title="Post aggregate filters"
            collapsed={state.aggregate.sections.postAggregateFilters}
            onToggle={toggleSection("postAggregateFilters")}
          />
          <CardSection title="Order" collapsed={state.aggregate.sections.order} onToggle={toggleSection("order")}>
            {state.dragItem && !isAggregateOrderByDroppable(state) && <DropDisabledEffect />}
            <Tree
              droppableProps={{
                droppableId: getDroppableId("aggregateOrder"),
                isDropDisabled: !isAggregateOrderByDroppable(state),
              }}
              trees={(state.aggregate.query.orderBy || []).map(r => ({
                label: r.name,
                key: r.name,
                color: PslColor.dimension,
                tag: r.order === "Asc" ? "🔽" : "🔼",
                onRemove: () => dispatch({ type: "[aggregate] removeOrderBy", payload: { ref: r.name } }),
                onClick: () =>
                  dispatch({
                    type: "[aggregate] updateOrderBy",
                    payload: { ref: r.name, order: r.order === "Asc" ? "Desc" : "Asc" },
                  }),
                cursor: "drag",
                draggableProps: { draggableId: getDraggableId("aggregateOrder", r.name) },
              }))}
              placeholder={OrderDropZone}
            />
          </CardSection>
          <CardSection
            title="Limit and offset"
            collapsed={state.aggregate.sections.limitAndOffset}
            onToggle={toggleSection("limitAndOffset")}
          >
            <Input
              label="Limit"
              type="number"
              hint="Max: 1000"
              value={state.aggregate.query.limit.toString()}
              onChange={val =>
                dispatch({
                  type: "[aggregate] updateQuery",
                  payload: { ...state.aggregate.query, limit: Math.min(+val, 1000) },
                })
              }
              {...{ style: { marginBottom: 12 } }}
            />
            <Input
              label="Offset"
              type="number"
              value={state.aggregate.query.offset.toString()}
              onChange={val =>
                dispatch({
                  type: "[aggregate] updateQuery",
                  payload: { ...state.aggregate.query, offset: +val },
                })
              }
            />
          </CardSection>
        </>
      }
    />
  );
};
