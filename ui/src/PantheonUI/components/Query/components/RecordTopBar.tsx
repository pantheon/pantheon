import { TopbarButton } from "@operational/components";
import React from "react";
import { connect } from "react-redux";

import * as undoable from "../../../utils/undoable";
import * as actions from "../actions";
import { State as ReduxState } from "../state";
import CodeViewToggle from "./CodeViewToggle";

export interface Props {
  Record: ReduxState["Record"];
  // Actions
  changeQueryType: typeof actions.changeQueryType;
  clearRecordQuery: typeof actions.clearRecordQuery;
  undoRecordQueryChange: typeof actions.undoRecordQueryChange;
  redoRecordQueryChange: typeof actions.redoRecordQueryChange;
  toggleRecordCodeView: typeof actions.toggleRecordCodeView;
}

const RecordTopBar: React.SFC<Props> = props => (
  <>
    <CodeViewToggle
      isCodeView={Boolean(props.Record.codeView)}
      onChange={isCodeView => {
        props.toggleRecordCodeView(isCodeView);
      }}
    />
    <TopbarButton
      icon="No"
      onClick={() => {
        props.clearRecordQuery();
      }}
    >
      Clear
    </TopbarButton>
    <TopbarButton
      icon="Undo"
      disabled={!undoable.canUndo(props.Record.query)}
      onClick={() => {
        props.undoRecordQueryChange();
      }}
    >
      Undo
    </TopbarButton>
    <TopbarButton
      icon="Redo"
      disabled={!undoable.canRedo(props.Record.query)}
      onClick={() => {
        props.redoRecordQueryChange();
      }}
    >
      Redo
    </TopbarButton>
  </>
);

const mapStateToProps = (state: ReduxState) => ({
  Record: state.Record,
});

const mapDispatchToProps = {
  changeQueryType: actions.changeQueryType,
  clearRecordQuery: actions.clearRecordQuery,
  undoRecordQueryChange: actions.undoRecordQueryChange,
  redoRecordQueryChange: actions.redoRecordQueryChange,
  toggleRecordCodeView: actions.toggleRecordCodeView,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(RecordTopBar);
