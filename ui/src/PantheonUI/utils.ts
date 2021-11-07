import Cookies from "js-cookie";
import { getConfig } from "./components/Config";

/**
 * Wrap fetch in a method that adds credentials and auth headers  to the request.
 * This method is designed to be as recognizable and vanilla as possible, and refrains from e.g.
 * autodetecting `Content-Type` headers. Will eventually retire in favor of new `restful-react`
 * functionality outlined in https://github.com/contiamo/restful-react/issues/77.
 */
export const customFetch = (url: string, config?: RequestInit) =>
  fetch(getConfig("backend") + url, {
    ...config,
    credentials: "include",
    headers: {
      ...(config ? config.headers : {}),
      "x-double-cookie": Cookies.get("double-cookie") || "",
    },
  });

export const matchFields = (field: string, record1: any, record2: any): boolean => {
  const val1 = record1[field];
  const val2 = record2[field];
  if (val1 && typeof val1 === "object" && val1.constructor === Object) {
    return JSON.stringify(val1) === JSON.stringify(val2);
  }
  return val1 === val2;
};

export const formatError = (errors: string[], truncateAt: number = 57): string => {
  const fullError = errors.join(" ");
  // Truncate error message. This relies on the backend
  // sending short ones.
  if (fullError.length > truncateAt) {
    return fullError.slice(0, truncateAt) + "...";
  }
  return fullError;
};

export const exhaustiveCheck = (_: never): never => {
  throw new Error("Switch was not exhaustive");
};

export const encodeForUrl = (json: any) => encodeURIComponent(JSON.stringify(json));

/**
 * Decode a JSON value from an encoded URI component. Since is annotated as `any`, it should be decoded wherever it is used.
 */
export const decodeFromUrl = (encoded: string): any => {
  try {
    const parsed = JSON.parse(decodeURIComponent(encoded));
    return parsed;
  } catch (err) {
    return undefined;
  }
};

export const getQueryField = (key: string) => (url: string): string | undefined => {
  const matches = url.match(new RegExp(`${key}[^\&]+`));
  if (!matches) {
    return;
  }
  const match = matches[0];
  if (!match) {
    return;
  }
  return (match as string).split("=")[1];
};

/**
 * Gets a query parameter from the URL
 * E.g. if the URL is "/somepath?a=b" then getQuery("a") === "b"
 * NOTE: https://www.npmjs.com/package/qs is not used because of issues on
 * how numbers are retrieved from the query string.
 */
export const getQueryFromUrl = (key: string): string | undefined => {
  return getQueryField(key)(window.location.search);
};

/**
 * Sets a query. History is not preserved.
 * setQuery("a")("b") will redirect to "/currentpath?a=b"
 * NOTE: https://www.npmjs.com/package/qs is not used because of issues on
 * how numbers are retrieved from the query string.
 */
export const setQueryInUrl = (key: string) => (value: string) => {
  window.history.replaceState(undefined, "", `${window.location.pathname}?${key}=${value}`);
};

export const PslColor = {
  attribute: "#28A8D8",
  level: "#2491BA",
  measure: "#3CC346",
  table: "#000000",
  column: "#707070",
  dimension: "#28A8D8",
};
