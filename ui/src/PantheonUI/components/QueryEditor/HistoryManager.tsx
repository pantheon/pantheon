import { TopbarButton } from "@operational/components";
import React, { useContext } from "react";
import { QueryEditorContext } from ".";
import { getHistory } from "./QueryEditor.selector";

export const HistoryManager: React.FC = () => {
  const { state, dispatch } = useContext(QueryEditorContext);
  const history = getHistory(state);
  if (!history) {
    return null;
  }

  return (
    <>
      <TopbarButton icon="Undo" onClick={() => dispatch(history.undoAction)} disabled={!history.canUndo}>
        Undo
      </TopbarButton>
      <TopbarButton icon="Redo" onClick={() => dispatch(history.redoAction)} disabled={!history.canRedo}>
        Redo
      </TopbarButton>
      <TopbarButton icon="No" onClick={() => dispatch(history.clearAction)} disabled={!history.canClear}>
        Clear
      </TopbarButton>
    </>
  );
};
