import * as tucson from "tucson-decode";
import * as result from "./utils/result";

// State keeps track of time in Unix Epoch form, so these values look like 1537867775, and are generated as new Date().getTime()
export type UnixTime = number;

// Errors

export interface BackendError {
  errors: string[];
  fieldErrors: null | {
    [key: string]: string[];
  };
}

export type ResponseOrError<T> = result.Result<BackendError, T>;

export const backendErrorDecoder: tucson.Decoder<BackendError> = tucson.object({
  errors: tucson.map(tucson.optional(tucson.array(tucson.string)), errors => errors || []),
  fieldErrors: tucson.map(
    tucson.optional(tucson.dictionary(tucson.array(tucson.string))),
    fieldErrors => fieldErrors || null,
  ),
});

export interface Validation<T> {
  validatedData: T;
  fieldErrors: {
    [key: string]: string[];
  };
}

export type EditStatus<T> = { status: "readonly" } | { status: "editing"; draft: T } | { status: "saving"; draft: T };

//  'Draft<T> | null' is shorter version of the EditStatus<T>
export interface Draft<T> {
  draft: T;
  saving?: boolean;
}
