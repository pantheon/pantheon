import { IconName, Topbar as OpTopbar, TopbarSelect } from "@operational/components";
import React, { useContext, useMemo } from "react";
import { QueryEditorContext } from ".";
import { Schema } from "../queries";
import { CodeViewToggle } from "./CodeViewToggle";
import { HistoryManager } from "./HistoryManager";
import { QueryEditorState } from "./QueryEditor.reducer";

export interface TopBarProps {
  schemas: Schema[];
}

const getIcon = (queryType: QueryEditorState["queryType"]): IconName => {
  switch (queryType) {
    case "Aggregate":
      return "Olap";
    case "Record":
      return "Entity";
    case "Sql":
      return "Sql";
  }
};

export const TopBar: React.SFC<TopBarProps> = React.memo(props => {
  const { state, dispatch, pushState } = useContext(QueryEditorContext);

  const schemaItems = useMemo(
    () => props.schemas.map(s => ({ label: s.name, onClick: () => pushState(`schemas/${s.id}/query`) })),
    [props.schemas, pushState],
  );

  const queryTypeItems = useMemo(
    () =>
      (["Aggregate", "Record", "Sql"] as Array<QueryEditorState["queryType"]>).map(i => ({
        label: i,
        icon: getIcon(i),
        onClick: () => dispatch({ type: "updateQueryType", payload: i }),
      })),
    [],
  );

  return (
    <OpTopbar
      left={
        <>
          <TopbarSelect label="Schema:" selected={state.schema && state.schema.name} items={schemaItems} />
          <TopbarSelect label="Query type:" selected={state.queryType} items={queryTypeItems} />
          <CodeViewToggle />
          <HistoryManager />
        </>
      }
    />
  );
});
