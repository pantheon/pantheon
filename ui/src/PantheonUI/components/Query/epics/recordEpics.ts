import { ActionsObservable, combineEpics, Epic, StateObservable } from "redux-observable";
import { from, merge, of } from "rxjs";
import { switchMap, takeUntil, withLatestFrom } from "rxjs/operators";

import { responseDecoder } from "../../../data/pantheonQueryResponse";
import { customFetch } from "../../../utils";
import { fromPromiseWithSensibleMinDelay } from "../../../utils/observables";
import * as result from "../../../utils/result";
import * as undoable from "../../../utils/undoable";
import { createQueryLogPollStream } from "./utils";

import {
  Action,
  ActionType,
  freezeRecordQueryEditing,
  receiveRecordCompatibility,
  receiveRecordQuery,
  receiveRecordQueryLog,
  RequestRecordQuery,
} from "../actions";

import { compatibilityDecoder } from "../compatibilityTypes";
import { State } from "../state";
import { EpicsDependencies } from "../types";
import { processJsonResponse } from "../utils";

const recordQueryEpic: Epic<Action, Action, State, EpicsDependencies> = (
  action$: ActionsObservable<Action>,
  state$: StateObservable<State>,
  { catalogId, schemaId, customReference }: EpicsDependencies,
) =>
  action$
    .ofType(ActionType.RequestRecordQuery)
    .pipe(withLatestFrom(state$, (action, state) => ({ state, queryId: (action as RequestRecordQuery).queryId })))
    .pipe(
      switchMap(({ state, queryId }) => {
        const requestStream = from(
          customFetch(`/catalogs/${catalogId}/schemas/${schemaId}/query`, {
            method: "POST",
            body: JSON.stringify({ ...undoable.getPresent(state.Record.query), customReference, queryId }),
            headers: {
              "Content-Type": "application/json",
            },
          })
            .then(processJsonResponse(responseDecoder))
            .then(receiveRecordQuery(new Date().getTime())),
        ).pipe(takeUntil(action$.ofType(ActionType.CancelRecordQuery)));

        const queryLogPollStream = createQueryLogPollStream({
          catalogId,
          queryId,
          stopOn: action$.ofType(ActionType.CancelRecordQuery, ActionType.ReceiveRecordQuery),
          responseActionCreator: receiveRecordQueryLog,
        });

        return merge(requestStream, queryLogPollStream);
      }),
    );

const recordCompatibilityEpic: Epic<Action, Action, State, EpicsDependencies> = (
  action$: ActionsObservable<Action>,
  state$: StateObservable<State>,
  { catalogId, schemaId }: EpicsDependencies,
) =>
  action$
    .ofType(
      ActionType.ChangeRecordRows,
      ActionType.RequestRecordCompatibility,
      ActionType.BlurRecordFilterInput,
      ActionType.UndoRecordQueryChange,
      ActionType.RedoRecordQueryChange,
      ActionType.ClearRecordQuery,
    )
    .pipe(withLatestFrom(state$, (_, state) => state))
    .pipe(
      switchMap(state => {
        const query = undoable.getPresent(state.Record.query);
        const payload = {
          query: {
            type: "Record",
            rows: query.rows,
            // `|| undefined` makes sure empty strings aren't sent (which would be interpreted by the backend as an incorrect entry as opposed to a missing one)
            filter: query.filter || undefined,
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
          .then(r => receiveRecordCompatibility(result.map(r, v => v.dimensions)));
        return merge(of(freezeRecordQueryEditing()), fromPromiseWithSensibleMinDelay(promise));
      }),
    );

export default combineEpics(recordCompatibilityEpic, recordQueryEpic);
