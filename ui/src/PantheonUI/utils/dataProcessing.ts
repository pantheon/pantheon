import * as result from "./result";

export type JSONValue = string | number | boolean | null | undefined | JSONObject | JSONArray;

interface JSONObject extends Record<string, JSONValue> {}
interface JSONArray extends Array<JSONValue> {}

export const safeJsonParse = (val: string): result.Result<string, JSONValue> => {
  try {
    if (val === "") return result.success(undefined);
    return result.success(JSON.parse(val));
  } catch (err) {
    return result.error(String(err));
  }
};
