import { combineLatest, from, Observable, of } from "rxjs";
import { delay } from "rxjs/operators";

/**
 * Transforms a promise into an observable, delaying it to a minimum delay but only
 * if it took a shorter time.
 * Examples:
 * - if minDelay is 300 and the promise took 250ms to complete, the observable will fire after 300ms.
 * - if minDelay is 300 and the promise took 350ms to complete, the observable will fire after 350ms.
 */
const fromPromiseWithMinDelay = <T>(promise: Promise<T>, minDelay: number): Observable<T> =>
  combineLatest<T>(from(promise), of(undefined).pipe(delay(minDelay)), (val: T) => val);

/**
 * The specific version of `fromPromiseWithMinDelay` above, but defaulting to a sensible minimum
 * duration good for UX.
 */
export const fromPromiseWithSensibleMinDelay = <T>(promise: Promise<T>): Observable<T> =>
  fromPromiseWithMinDelay(promise, 300);
