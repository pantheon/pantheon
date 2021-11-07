import { Card, CardSection, Tree } from "@operational/components";
import React, { useCallback, useContext } from "react";
import { QueryEditorContext } from ".";
import { PslColor } from "../../utils";
import { getDraggableId, getDroppableId, isAggregateColumnsDroppable } from "./dragAndDropManager";
import { DropDisabledEffect } from "./DropDisabledEffect";
import { DimensionDropZone } from "./DropZones";

export const AggregateVisualPanelTop: React.FC = () => {
  const { state, dispatch } = useContext(QueryEditorContext);

  const toggleSection = useCallback(() => {
    // We are toggle both sections here because it's horizontal
    dispatch({ type: "[aggregate] toggleSection", payload: "topNColumns" });
    dispatch({ type: "[aggregate] toggleSection", payload: "columns" });
  }, []);

  return (
    <Card
      stackSections="horizontal"
      sections={
        <>
          <CardSection title="Columns" collapsed={state.aggregate.sections.columns} onToggle={toggleSection}>
            {state.dragItem && !isAggregateColumnsDroppable(state) && <DropDisabledEffect />}
            <Tree
              droppableProps={{
                droppableId: getDroppableId("aggregateColumn"),
                isDropDisabled: !isAggregateColumnsDroppable(state),
              }}
              trees={state.aggregate.query.columns.map(c => ({
                label: c,
                key: c,
                color: PslColor.dimension,
                tag: "D",
                onClick: () => dispatch({ type: "[aggregate] switchRowColumn", payload: { ref: c } }),
                cursor: "drag",
                onRemove: () => dispatch({ type: "[aggregate] removeColumn", payload: { ref: c } }),
                draggableProps: { draggableId: getDraggableId("aggregateColumn", c) },
              }))}
              placeholder={DimensionDropZone}
            />
          </CardSection>
          <CardSection
            title="Top N Columns"
            collapsed={state.aggregate.sections.topNColumns}
            onToggle={toggleSection}
          />
        </>
      }
    />
  );
};
