import { ActionsObservable, combineEpics, Epic, StateObservable } from "redux-observable";
import { from, merge } from "rxjs";
import { switchMap, takeUntil, withLatestFrom } from "rxjs/operators";

import { responseDecoder } from "../../../data/pantheonQueryResponse";
import { customFetch } from "../../../utils";
import { Action, ActionType, receiveSqlQuery, receiveSqlQueryLog, receiveSqlTables, RequestSqlQuery } from "../actions";
import { State } from "../state";
import { EpicsDependencies, tableResponsesSchema } from "../types";
import { processJsonResponse } from "../utils";
import { createQueryLogPollStream } from "./utils";

const sqlQueryEpic: Epic<Action, Action, State, EpicsDependencies> = (
  action$: ActionsObservable<Action>,
  state$: StateObservable<State>,
  { catalogId, schemaId, customReference }: EpicsDependencies,
) =>
  action$
    .ofType(ActionType.RequestSqlQuery)
    .pipe(withLatestFrom(state$, (action, state) => ({ state, queryId: (action as RequestSqlQuery).queryId })))
    .pipe(
      switchMap(({ state, queryId }) => {
        const requestStream = from(
          customFetch(`/catalogs/${catalogId}/schemas/${schemaId}/query`, {
            method: "POST",
            body: JSON.stringify({ ...state.Sql.query, customReference, queryId }),
            headers: {
              "Content-Type": "application/json",
            },
          })
            .then(processJsonResponse(responseDecoder))
            .then(receiveSqlQuery(new Date().getTime())),
        ).pipe(takeUntil(action$.ofType(ActionType.CancelSqlQuery)));

        const queryLogPollStream = createQueryLogPollStream({
          catalogId,
          queryId,
          stopOn: action$.ofType(ActionType.CancelSqlQuery, ActionType.ReceiveSqlQuery),
          responseActionCreator: receiveSqlQueryLog,
        });

        return merge(requestStream, queryLogPollStream);
      }),
    );

const sqlTablesEpic: Epic<Action, Action, State, EpicsDependencies> = (
  action$: ActionsObservable<Action>,
  _: StateObservable<State>,
  { catalogId, schemaId }: EpicsDependencies,
) =>
  action$.ofType(ActionType.RequestSqlTables).pipe(
    switchMap(() =>
      from(
        customFetch(`/catalogs/${catalogId}/schemas/${schemaId}/tables`, {
          credentials: "include",
        })
          .then(processJsonResponse(tableResponsesSchema))
          .then(receiveSqlTables),
      ),
    ),
  );

export default combineEpics(sqlQueryEpic, sqlTablesEpic);
