import { ActionsObservable, combineEpics, Epic, StateObservable } from "redux-observable";
import { from, merge, of } from "rxjs";
import { filter, switchMap, takeUntil, withLatestFrom } from "rxjs/operators";

import { customFetch } from "../../../utils";
import { fromPromiseWithSensibleMinDelay } from "../../../utils/observables";
import * as undoable from "../../../utils/undoable";
import { createQueryLogPollStream } from "./utils";

import {
  Action,
  ActionType,
  freezeAggregateQueryEditing,
  receiveAggregateCompatibility,
  receiveAggregateQuery,
  receiveAggregateQueryLog,
  RequestAggregateQuery,
} from "../actions";

import { State } from "../state";

import { responseDecoder } from "../../../data/pantheonQueryResponse";
import { compatibilityDecoder } from "../compatibilityTypes";
import { EpicsDependencies } from "../types";
import { processJsonResponse } from "../utils";

const aggregateQueryEpic: Epic<Action, Action, State, EpicsDependencies> = (
  action$: ActionsObservable<Action>,
  state$: StateObservable<State>,
  { catalogId, schemaId, customReference }: EpicsDependencies,
) =>
  action$.pipe(
    filter(action => action.type === ActionType.RequestAggregateQuery),
    withLatestFrom(state$, (action, state) => ({ state, queryId: (action as RequestAggregateQuery).queryId })),
    switchMap(({ state, queryId }) => {
      const requestStream = from(
        customFetch(`/catalogs/${catalogId}/schemas/${schemaId}/query`, {
          method: "POST",
          body: JSON.stringify({ ...undoable.getPresent(state.Aggregate.query), customReference, queryId }),
          headers: {
            "Content-Type": "application/json",
          },
        })
          .then(processJsonResponse(responseDecoder))
          .then(receiveAggregateQuery(new Date().getTime())),
      ).pipe(takeUntil(action$.ofType(ActionType.CancelAggregateQuery)));

      const queryLogPollStream = createQueryLogPollStream({
        catalogId,
        queryId,
        stopOn: action$.ofType(ActionType.CancelAggregateQuery, ActionType.ReceiveAggregateQuery),
        responseActionCreator: receiveAggregateQueryLog,
      });

      return merge(requestStream, queryLogPollStream);
    }),
  );

const aggregateCompatibilityEpic: Epic<Action, Action, State, EpicsDependencies> = (
  action$: ActionsObservable<Action>,
  state$: StateObservable<State>,
  { catalogId, schemaId }: EpicsDependencies,
) =>
  action$.pipe(
    filter(action =>
      [
        ActionType.ChangeAggregateColumns,
        ActionType.ChangeAggregateMeasures,
        ActionType.ChangeAggregateRows,
        ActionType.RequestAggregateCompatibility,
        ActionType.BlurAggregateFilterInput,
        ActionType.BlurAggregatePostAggregateFilterInput,
        ActionType.ClearAggregateQuery,
        ActionType.UndoAggregateQueryChange,
        ActionType.RedoAggregateQueryChange,
      ].includes(action.type),
    ),
    withLatestFrom(state$, (_, state) => state),
    switchMap(state => {
      const query = undoable.getPresent(state.Aggregate.query);
      const payload = {
        query: {
          type: "Aggregate",
          measures: query.measures,
          columns: query.columns,
          rows: query.rows,
          // `|| undefined` makes sure empty strings aren't sent (which would be interpreted by the backend as an incorrect entry as opposed to a missing one)
          filter: query.filter || undefined,
          aggregateFilter: query.aggregateFilter || undefined,
        },
      };
      const promise = customFetch(`/catalogs/${catalogId}/schemas/${schemaId}/nestedCompatibility`, {
        method: "POST",
        body: JSON.stringify(payload),
        headers: {
          "Content-Type": "application/json",
        },
      })
        .then(processJsonResponse(compatibilityDecoder))
        .then(receiveAggregateCompatibility);
      return merge(of(freezeAggregateQueryEditing()), fromPromiseWithSensibleMinDelay(promise));
    }),
  );

export default combineEpics(aggregateCompatibilityEpic, aggregateQueryEpic);
