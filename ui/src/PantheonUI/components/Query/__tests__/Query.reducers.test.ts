import { createStore, Store } from "redux";
import * as actions from "../actions";
import { reducer } from "../reducers";

const batchDispatch = ([actionsHead, ...actionsTail]: actions.Action[]) => (store: Store): void => {
  if (!actionsHead) {
    return;
  }
  store.dispatch(actionsHead);
  batchDispatch(actionsTail)(store);
};

test("Cancelled queries are not reflected in state", done => {
  const store = createStore(reducer);
  batchDispatch([
    actions.requestAggregateQuery(new Date().getTime()),
    actions.receiveAggregateQuery(new Date().getTime())({ type: "success", value: { columns: [], rows: [] } }),
    actions.requestAggregateQuery(new Date().getTime()),
    actions.cancelAggregateQuery(),
    actions.receiveAggregateQuery(new Date().getTime())({ type: "success", value: { columns: [], rows: [[], []] } }),
  ])(store);
  const state = store.getState();
  expect(typeof state.Aggregate.queryResponse).toEqual("undefined");
  done();
});
