import { CardSection, CardSectionProps } from "@operational/components";
import React, { useCallback, useContext, useEffect, useState } from "react";
import { Draggable, Droppable } from "react-beautiful-dnd";
import { QueryEditorContext } from "../QueryEditor";
import { getDraggableId, getDroppableId } from "../QueryEditor/dragAndDropManager";
import { DimensionDropZone } from "../QueryEditor/DropZones";
import { Operator } from "./Operator";
import pslFilter, { isOperator, isPredicate, unmatchedParenthesis } from "./parseFilter";
import { Predicate } from "./Predicate";

const predicateHeight = 75;

export const PslFilter: React.SFC<CardSectionProps> = props => {
  const { state, dispatch } = useContext(QueryEditorContext);
  const items = pslFilter.parse(state.aggregate.query.filter || "");

  // Parentheses addition/deletion management. `index` is relative to the `items` array.
  // Inserted parentheses are internally managed by the component to avoid pollute the application
  // state with non-valid query filter.
  const [insertedParentheses, setInsertedParentheses] = useState<Array<{ index: number; type: "open" | "close" }>>([]);

  // Clean state on filter update (undo/redo)
  useEffect(() => setInsertedParentheses([]), [state.aggregate.query.filter]);

  const addParenthesis = useCallback(
    (type: "open" | "close", index: number) => {
      const matchingParenthesis =
        type === "open"
          ? insertedParentheses.find(i => i.type === "close" && i.index > index)
          : insertedParentheses.find(i => i.type === "open" && i.index < index);

      if (matchingParenthesis) {
        dispatch({
          type: "[aggregate] addParenthesesInFilter",
          payload: {
            openParenthesisIndex: type === "open" ? index : matchingParenthesis.index,
            closeParenthesisIndex: type === "open" ? matchingParenthesis.index : index,
          },
        });
        setInsertedParentheses(prev =>
          prev.filter(i => !(i.index === matchingParenthesis.index && i.type === matchingParenthesis.type)),
        );
      } else {
        setInsertedParentheses(prev => [...prev, { type, index }]);
      }
    },
    [insertedParentheses, dispatch],
  );

  const removeParenthesis = useCallback(
    (type: "open" | "close", index: number, parenthesisId: number) => {
      const insertedParenthesis = insertedParentheses.findIndex(i => i.type === type && i.index === index);
      if (insertedParenthesis !== -1 && Number.isNaN(parenthesisId)) {
        setInsertedParentheses(prev => prev.filter((_, i) => i !== insertedParenthesis));
      } else {
        dispatch({ type: "[aggregate] removeParenthesesInFilter", payload: { parenthesisId } });
      }
    },
    [insertedParentheses, dispatch],
  );

  const canOpenParenthese = useCallback(
    (index: number) => {
      const isLast = index === items.length - 1;
      const havePendingCloseParentheseBefore = insertedParentheses.find(i => i.type === "close" && i.index <= index);
      const havePendingOpenParenthese = insertedParentheses.find(i => i.type === "open");

      return !isLast && !havePendingCloseParentheseBefore && !havePendingOpenParenthese;
    },
    [items, insertedParentheses],
  );

  const canCloseParenthesis = useCallback(
    (index: number) => {
      const isFirst = index === 0;
      const havePendingCloseParentheseBefore = insertedParentheses.find(i => i.type === "open" && i.index >= index);
      const havePendingCloseParenthese = insertedParentheses.find(i => i.type === "close");

      return !isFirst && !havePendingCloseParentheseBefore && !havePendingCloseParenthese;
    },
    [items, insertedParentheses],
  );

  return (
    <CardSection {...props}>
      <div style={{ position: "relative", paddingBottom: 8 }}>
        {/* Predicates */}
        <Droppable droppableId={getDroppableId("aggregateFilter")}>
          {({ droppableProps, innerRef, placeholder }, droppableSnapshot) => (
            <div {...droppableProps} ref={innerRef}>
              {items.length === 0 && <DimensionDropZone {...droppableSnapshot} />}
              {items.filter(isPredicate).map((item, i) => (
                <Draggable draggableId={getDraggableId("aggregateFilter", `${item.dimension}-${i}`)} index={i} key={i}>
                  {provided => (
                    <div {...provided.draggableProps} {...provided.dragHandleProps} ref={provided.innerRef}>
                      <Predicate item={item} index={i} />
                    </div>
                  )}
                </Draggable>
              ))}
              {placeholder}
            </div>
          )}
        </Droppable>
        {/* Operators */}
        {items.filter(isOperator).map((item, i) => (
          <Operator
            key={i}
            top={i * predicateHeight}
            canCloseParenthesis={canCloseParenthesis(i * 2)}
            canOpenParenthesis={canOpenParenthese(i * 2)}
            onCloseParenthesisAdd={() => addParenthesis("close", i * 2)}
            onCloseParenthesisRemove={parenthesisId => removeParenthesis("close", i * 2, parenthesisId)}
            onOpenParenthesisAdd={() => addParenthesis("open", i * 2)}
            onOpenParenthesisRemove={parenthesisId => removeParenthesis("open", i * 2, parenthesisId)}
            onOperatorToggle={() => dispatch({ type: "[aggregate] toggleFilterOperator", payload: { index: i * 2 } })}
            disabled={Boolean(state.dragItem)}
            {...item}
            closeParentheses={[
              ...item.closeParentheses,
              ...insertedParentheses
                .filter(j => j.index / 2 === i && j.type === "close")
                .map(() => unmatchedParenthesis),
            ]}
            openParentheses={[
              ...insertedParentheses
                .filter(j => j.index / 2 === i && j.type === "open")
                .map(() => unmatchedParenthesis),
              ...item.openParentheses,
            ]}
          />
        ))}
      </div>
    </CardSection>
  );
};
