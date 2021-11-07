import { TopbarButton } from "@operational/components";
import React from "react";
import { connect } from "react-redux";

import * as undoable from "../../../utils/undoable";
import * as actions from "../actions";
import { State as ReduxState } from "../state";
import CodeViewToggle from "./CodeViewToggle";

export interface Props {
  Aggregate: ReduxState["Aggregate"];
  // Actions
  clearAggregateQuery: typeof actions.clearAggregateQuery;
  undoAggregateQueryChange: typeof actions.undoAggregateQueryChange;
  redoAggregateQueryChange: typeof actions.redoAggregateQueryChange;
  toggleAggregateCodeView: typeof actions.toggleAggregateCodeView;
}

export const AggregateTopBar: React.SFC<Props> = props => (
  <>
    <CodeViewToggle
      isCodeView={Boolean(props.Aggregate.codeView)}
      onChange={isCodeView => {
        props.toggleAggregateCodeView(isCodeView);
      }}
    />
    <TopbarButton
      icon="No"
      onClick={() => {
        props.clearAggregateQuery();
      }}
    >
      Clear
    </TopbarButton>
    <TopbarButton
      icon="Undo"
      disabled={!undoable.canUndo(props.Aggregate.query)}
      onClick={() => {
        props.undoAggregateQueryChange();
      }}
    >
      Undo
    </TopbarButton>
    <TopbarButton
      icon="Redo"
      disabled={!undoable.canRedo(props.Aggregate.query)}
      onClick={() => {
        props.redoAggregateQueryChange();
      }}
    >
      Redo
    </TopbarButton>
  </>
);

const mapStateToProps = (state: ReduxState) => ({
  Aggregate: state.Aggregate,
});

const mapDispatchToProps = {
  clearAggregateQuery: actions.clearAggregateQuery,
  undoAggregateQueryChange: actions.undoAggregateQueryChange,
  redoAggregateQueryChange: actions.redoAggregateQueryChange,
  toggleAggregateCodeView: actions.toggleAggregateCodeView,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(AggregateTopBar);
