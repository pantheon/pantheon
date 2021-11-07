import { Card, Textarea } from "@operational/components";
import { PathReporter } from "io-ts/lib/PathReporter";
import React, { useCallback, useContext, useEffect, useRef, useState } from "react";

import { QueryEditorContext } from ".";
import { validateFilter } from "../PslFilter/parseFilter";
import { getQuery } from "./QueryEditor.selector";
import { AggregateQuery, RecordQuery } from "./utils";

// TODO use monaco + JSON schemas
// ref: https://microsoft.github.io/monaco-editor/playground.html#extending-language-services-configure-json-defaults
export const CodeCard: React.FC = () => {
  const { state, dispatch } = useContext(QueryEditorContext);
  const [draft, setDraft] = useState(getQuery(state));
  const [error, setError] = useState<string | undefined>();
  const isEditing = useRef(false); // we use a ref here to track the state without re-render (text focus problem)

  const onFocus = useCallback(() => (isEditing.current = true), [isEditing]);
  const onBlur = useCallback(() => (isEditing.current = false), [isEditing]);

  // Validate and propagate the code/error to the state
  useEffect(() => {
    try {
      setError(undefined);
      switch (state.queryType) {
        case "Aggregate": {
          const payload = AggregateQuery.decode(JSON.parse(draft));
          if (payload.isLeft()) {
            throw new Error(PathReporter.report(payload).join("\n"));
          }
          if (payload.value.filter) {
            const filterErrors = validateFilter(payload.value.filter);
            if (filterErrors) {
              throw new Error(filterErrors.map(i => i.message).join("\n"));
            }
          }
          return dispatch({ type: "[aggregate] updateQuery", payload: payload.value });
        }
        case "Record": {
          const payload = RecordQuery.decode(JSON.parse(draft));
          if (payload.isLeft()) {
            throw new Error(PathReporter.report(payload).join("\n"));
          }
          return dispatch({ type: "[record] updateQuery", payload: payload.value });
        }
        case "Sql":
          return dispatch({ type: "[sql] updateQuery", payload: draft });
      }
    } catch (e) {
      setError(e.message);
    }
  }, [draft, state.queryType]);

  // Subscribe to state update
  useEffect(() => {
    try {
      // Avoid to re-indent and loose the focus if the state is the same
      if (isEditing.current === false) {
        setDraft(() => getQuery(state));
      }
    } catch (e) {
      setDraft(() => getQuery(state));
    }
  }, [state.queryType, state.aggregate.query, state.record.query, state.sql]);

  return (
    <Card title={`${state.queryType} query`}>
      <Textarea
        onFocus={onFocus}
        onBlur={onBlur}
        code
        value={draft}
        onChange={setDraft}
        fullWidth
        error={error}
        height={150}
      />
    </Card>
  );
};
