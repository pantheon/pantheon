import { styled, Toggle } from "@operational/components";
import { Value } from "@operational/components/lib/Toggle/Toggle";
import { title } from "case";
import React, { useCallback, useContext, useMemo } from "react";
import { QueryEditorContext } from ".";

const Container = styled("div")`
  margin-left: ${props => props.theme.space.small}px;
`;

export const CodeViewToggle: React.FC = () => {
  const { state, dispatch } = useContext(QueryEditorContext);

  const onAggregateChange = useCallback(
    (mode: string) => dispatch({ type: "[aggregate] switchMode", payload: mode as "code" | "visual" }),
    [dispatch],
  );

  const onRecordChange = useCallback(
    (mode: string) => dispatch({ type: "[record] switchMode", payload: mode as "code" | "visual" }),
    [dispatch],
  );

  const options = useMemo(() => ["code", "visual"].map(i => ({ label: title(i), value: i })) as [Value, Value], []);

  switch (state.queryType) {
    case "Aggregate":
      return (
        <Container>
          <Toggle options={options} value={state.aggregate.mode} onChange={onAggregateChange} condensed />
        </Container>
      );
    case "Record":
      return (
        <Container>
          <Toggle options={options} value={state.record.mode} onChange={onRecordChange} condensed />
        </Container>
      );
    case "Sql":
      return null;
  }
};
