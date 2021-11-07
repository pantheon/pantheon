import React, { createContext, useCallback, useReducer } from "react";
import { DragDropContext, DragStart } from "react-beautiful-dnd";

import { Code, Message, ModalConfirmContext, PageContent, Spinner, styled } from "@operational/components";
import { OperationalRouter, OperationalRouterProps } from "@operational/router";
import noop from "lodash/noop";
import { CompatibilityResponse, Endpoint, Schema } from "../queries";
import { AggregateView } from "./AggregateView";
import { mapDragEndToDispatch } from "./dragAndDropManager";
import { initialState, QueryEditorActions, queryEditorReducer, QueryEditorState } from "./QueryEditor.reducer";
import { TopBar } from "./TopBar";

export const QueryEditorContext = createContext<{
  state: QueryEditorState;
  dispatch: React.Dispatch<QueryEditorActions>;
  modal: ModalConfirmContext["modal"];
  confirm: ModalConfirmContext["confirm"];
  pushState: OperationalRouterProps["pushState"];
}>({
  state: initialState,
  dispatch: noop,
  modal: noop,
  confirm: noop,
  pushState: noop,
});

export interface QueryEditorProps {
  schemaId: string;
  schemas?: Schema[];
  catalogId: string;
  endpoint?: Endpoint;
  mock?: CompatibilityResponse;
}

const Container = styled("div")`
  height: 100%;
  display: grid;
  grid-template-rows: ${({ theme }) => theme.topbarHeight}px auto;
  background-color: ${({ theme }) => theme.color.background.lighter};
`;

export const QueryEditor: React.FC<QueryEditorProps> = ({ schemas, mock, schemaId }) => {
  if (schemas === undefined) {
    return (
      <PageContent fill>
        <Spinner />
      </PageContent>
    );
  }

  const schema = schemas.find(s => s.id === schemaId);

  if (!schema) {
    return (
      <PageContent fill>
        <Message type="error" title="Not found" body="Schema not found!" />;
      </PageContent>
    );
  }

  const [state, dispatch] = useReducer(queryEditorReducer, { ...initialState, schema });

  const onDragEnd = useCallback((confirm: ModalConfirmContext["confirm"]) => mapDragEndToDispatch(dispatch, confirm), [
    dispatch,
  ]);

  const onDragStart = useCallback(
    (initial: DragStart) => dispatch({ type: "[draggable] onDragStart", payload: initial }),
    [dispatch],
  );

  return (
    <OperationalRouter>
      {({ pushState }) => (
        <PageContent fill noPadding {...{ style: { minWidth: 1000 } }}>
          {({ modal, confirm }) => (
            <QueryEditorContext.Provider value={{ state, dispatch, modal, confirm, pushState }}>
              <Container>
                <TopBar schemas={schemas} />
                <DragDropContext onDragEnd={onDragEnd(confirm)} onDragStart={onDragStart}>
                  {state.queryType === "Aggregate" && <AggregateView mock={mock} />}
                  {state.queryType === "Record" && <Code syntax="json" src={state} />}
                  {state.queryType === "Sql" && <Code syntax="json" src={state} />}
                </DragDropContext>
              </Container>
            </QueryEditorContext.Provider>
          )}
        </PageContent>
      )}
    </OperationalRouter>
  );
};

export default QueryEditor;
