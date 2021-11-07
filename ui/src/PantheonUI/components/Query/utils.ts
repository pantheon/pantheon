import * as tucson from "tucson-decode";

import { BackendError, backendErrorDecoder, ResponseOrError } from "../../types";
import { error, isError, mapError, Result, success } from "../../utils/result";

const stringifyDecodeErrors = (decodeErrors: tucson.DecodeError[]): string => decodeErrors.map(e => e.error).join(" ");

const processBody = <ProcessResult, RawResponseBodyType>(
  bodyAsync: Promise<RawResponseBodyType>,
  process: (body: RawResponseBodyType) => Result<string, ProcessResult>,
): Promise<ResponseOrError<ProcessResult>> =>
  bodyAsync
    .then(body => {
      const res = process(body);
      if (isError(res)) {
        console.warn("Error processing response body", res.value);
        return error({ errors: ["Failed to process server response"], fieldErrors: null });
      }
      return success(res.value);
    })
    .catch(() => error({ errors: ["Failed to get server response body"], fieldErrors: null }));

/**
 * This method provides proper errors processing in the following cases:
 * 1) if the promise fails
 * 2) if the transformation fails
 */
const processResponse = <ResponseType, RawResponseBodyType>(
  getBody: (r: Response) => Promise<RawResponseBodyType>,
  transform: (body: RawResponseBodyType) => Result<string, ResponseType>,
): ((r: Response) => Promise<ResponseOrError<ResponseType>>) => {
  return res =>
    res.ok
      ? processBody(getBody(res), transform)
      : processBody(res.json(), v =>
          mapError<tucson.DecodeError[], BackendError, string>(backendErrorDecoder(v), stringifyDecodeErrors),
        ).then(v => error(v.value));
};

export const processJsonResponse = <T>(decode: tucson.Decoder<T>): ((r: Response) => Promise<ResponseOrError<T>>) =>
  processResponse(r => r.json(), v => mapError<tucson.DecodeError[], T, string>(decode(v), stringifyDecodeErrors));
