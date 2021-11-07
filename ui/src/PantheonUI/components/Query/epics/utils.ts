import { concat, from, interval, Observable, of } from "rxjs";
import { filter, map, switchMap, takeUntil, tap } from "rxjs/operators";

import { QueryLog, queryLogDecoder } from "../../../data/queryLog";
import { customFetch } from "../../../utils";
import { isError, isSuccess } from "../../../utils/result";
import { Action } from "../actions";
import { processJsonResponse } from "../utils";

export interface CreateQueryLogPollStreamParams {
  /** Completes when this observable emits */
  stopOn: Observable<Action>;
  catalogId: string;
  queryId: string;
  /** An action creator that successfully fetched query log objects should be handed to */
  responseActionCreator: (response: QueryLog) => Action;
}

/**
 * This function creates an observable of periodically polled query logs, re-used across
 * query type-specific epics.
 *
 */
export const createQueryLogPollStream = (params: CreateQueryLogPollStreamParams) =>
  concat(
    // Requesting logs on an interval until either the query is cancelled or response is received
    interval(2000).pipe(takeUntil(params.stopOn)),
    // Requesting a few more times at the end in case plans are only available a bit later
    of(0),
  ).pipe(
    switchMap(() =>
      from(
        customFetch(`/catalogs/${params.catalogId}/queryHistory/${params.queryId}`, {
          method: "GET",
          headers: {
            "Content-Type": "application/json",
          },
        }).then(processJsonResponse(queryLogDecoder)),
      ),
    ),
    // This step is only used for logging. Putting this logic in the `filter` below makes the type guard not work anymore
    tap(response => {
      if (isError(response)) {
        console.warn("Unable to retrieve query logs", response);
      }
    }),
    filter(isSuccess),
    map(response => params.responseActionCreator(response.value)),
  );
