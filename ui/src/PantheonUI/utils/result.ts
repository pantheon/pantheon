export interface Success<T> {
  type: "success";
  value: T;
}

export const success = <T>(t: T): Success<T> => ({
  type: "success",
  value: t,
});

export interface Error<E> {
  type: "error";
  value: E;
}

export const error = <T>(e: T): Error<T> => ({
  type: "error",
  value: e,
});

export type Result<E, T> = Error<E> | Success<T>;

export const isError = <E, T>(v: Result<E, T>): v is Error<E> => {
  return v.type === "error";
};

export const isSuccess = <E, T>(v: Result<E, T>): v is Success<T> => {
  return v.type === "success";
};

export const map = <E, T, R>(r: Result<E, T>, f: (_: T) => R): Result<E, R> => (isError(r) ? r : success(f(r.value)));

export const flatMap = <E, T, R>(r: Result<E, T>, f: (_: T) => Result<E, R>): Result<E, R> =>
  isError(r) ? r : f(r.value);

export const mapError = <E, T, E1>(r: Result<E, T>, f: (_: E) => E1): Result<E1, T> =>
  isError(r) ? error(f(r.value)) : r;

export const withDefault = <T, E>(r: Result<E, T>, defaultValue: T): T => {
  if (isError(r)) {
    return defaultValue;
  }
  return r.value;
};

export const fold = <E, T, R>(v: Result<E, T>, fnSuccess: (val: T) => R, fnError: (err: E) => R) =>
  isSuccess(v) ? fnSuccess(v.value) : fnError(v.value);

/**
 * Turns an unsafe function into a result, returning an error if a runtime error is thrown, or a success with the return value.
 */
export const unsafe = <Value>(value: () => Value): Result<string, Value> => {
  try {
    return success(value());
  } catch (err) {
    return error(JSON.stringify(err, Object.getOwnPropertyNames(err)));
  }
};
