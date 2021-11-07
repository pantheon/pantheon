import noop from "lodash/noop";
import React from "react";
import { Provider } from "react-redux";
import { applyMiddleware, createStore, Store } from "redux";
import { composeWithDevTools } from "redux-devtools-extension";
import { createEpicMiddleware } from "redux-observable";
import shortid from "shortid";

import { aggregateQueryDecoder } from "../data/aggregateQuery";
import { recordQueryDecoder } from "../data/recordQuery";
import { sqlQueryDecoder } from "../data/sqlQuery";
import { decodeFromUrl, encodeForUrl, exhaustiveCheck, getQueryFromUrl, setQueryInUrl } from "../utils";
import * as undoable from "../utils/undoable";
import { getAllConfig } from "./Config";
import * as actions from "./Query/actions";
import View from "./Query/components/View";
import rootEpic from "./Query/epics";
import { reducer } from "./Query/reducers";
import { State } from "./Query/state";
import { EpicsDependencies, QueryProps } from "./Query/types";

const queryDataKey = "querydata";

export interface LocalState {
  isStoreBeingTornDown: boolean;
  providerKey: number;
}

const frontendId = shortid.generate();
const customReference = `ui-schemaquery-${frontendId}`

class Query extends React.Component<QueryProps, LocalState> {
  public store?: Store<State>;

  public state = {
    isStoreBeingTornDown: false,
    providerKey: 0,
  };

  public handleChange(newlyCreatedStore?: Store) {
    const store = newlyCreatedStore || this.store;
    if (!store) {
      return;
    }
    const state: State = store.getState();
    switch (state.queryType) {
      case "Aggregate":
        setQueryInUrl(queryDataKey)(encodeForUrl(undoable.getPresent(state.Aggregate.query)));
        return;
      case "Sql":
        setQueryInUrl(queryDataKey)(encodeForUrl(state.Sql.query));
        return;
      case "Record":
        setQueryInUrl(queryDataKey)(encodeForUrl(undoable.getPresent(state.Record.query)));
        return;
      default:
        return exhaustiveCheck(state.queryType);
    }
  }

  public setupLocalRedux() {
    /**
     * This component instantiates its own Redux store locally, which acts as a formalized
     * local state. Though the complexity of this use-case does not make this necessary,
     * it is used as a testbed for more complex versions of this component later on.
     */
    const epicMiddleware = createEpicMiddleware<actions.Action, any, State, EpicsDependencies>({
      dependencies: {
        runtimeConfig: getAllConfig(),
        catalogId: this.props.catalogId,
        schemaId: this.props.schemaId,
        customReference,
      },
    });
    const store = createStore(reducer, composeWithDevTools(applyMiddleware(epicMiddleware)));
    epicMiddleware.run(rootEpic);
    const queryFromUrl = getQueryFromUrl(queryDataKey);
    if (queryFromUrl) {
      const decodedQueryFromUrl = decodeFromUrl(queryFromUrl);
      const decodedAggregateQuery = aggregateQueryDecoder(decodedQueryFromUrl);
      if (decodedAggregateQuery.type === "success") {
        store.dispatch(actions.restoreAggregateQuery(decodedAggregateQuery.value));
      }
      const decodedRecordQuery = recordQueryDecoder(decodedQueryFromUrl);
      if (decodedRecordQuery.type === "success") {
        store.dispatch(actions.restoreRecordQuery(decodedRecordQuery.value));
      }
      const decodedSqlQuery = sqlQueryDecoder(decodedQueryFromUrl);
      if (decodedSqlQuery.type === "success") {
        store.dispatch(actions.restoreSqlQuery(decodedSqlQuery.value));
      }
    }

    // Subscribe to changes
    this.handleChange(store);
    store.subscribe(() => {
      this.handleChange();
    });

    // Fire initial actions
    store.dispatch(actions.requestAggregateCompatibility());
    store.dispatch(actions.requestRecordCompatibility());
    store.dispatch(actions.requestSqlTables());

    // Set store on the instance
    this.store = store;

    this.setState(prevState => ({
      providerKey: prevState.providerKey + 1,
    }));
  }

  public teardownLocalRedux() {
    if (this.store) {
      // This acts as store.unsubscribe (not supported by redux), and allows the
      // torn-down store to be garbage-collected.
      this.store.subscribe(noop);
      this.store = undefined;
    }
  }

  public componentDidMount() {
    this.setupLocalRedux();
  }

  public componentDidUpdate(prevProps: QueryProps) {
    if (this.props.schemaId !== prevProps.schemaId) {
      this.teardownLocalRedux();
      this.setupLocalRedux();
    }
  }

  public render() {
    return this.store ? (
      <Provider store={this.store} key={this.state.providerKey}>
        <View
          endpoint={this.props.endpoint}
          catalogId={this.props.catalogId}
          schemaId={this.props.schemaId}
          schemas={this.props.schemas}
          customReference={customReference}
        />
      </Provider>
    ) : null;
  }
}

export default Query;
