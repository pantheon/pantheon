import { TopbarButton } from "@operational/components";
import React from "react";
import { connect } from "react-redux";

import { State as ReduxState } from "../state";

export interface Props {
  Sql: ReduxState["Sql"];
}

const SqlTopBar: React.SFC<Props> = () => (
  <>
    <TopbarButton icon="No" disabled>
      Clear
    </TopbarButton>
    <TopbarButton icon="Undo" disabled>
      Undo
    </TopbarButton>
    <TopbarButton icon="Redo" disabled>
      Redo
    </TopbarButton>
  </>
);

const mapStateToProps = (state: ReduxState) => ({
  Sql: state.Sql,
});

const mapDispatchToProps = {};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(SqlTopBar);
