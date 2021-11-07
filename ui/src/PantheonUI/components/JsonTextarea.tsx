import { styled, Textarea } from "@operational/components";
import React, { useState } from "react";

import { DecodeError } from "tucson-decode/lib";
import { safeJsonParse } from "../utils/dataProcessing";
import * as result from "../utils/result";

/** /!\ ++++++++++++++++++++++ /!\
 *
 *  Warning: This component is totally unstable and broken!
 *  This must be replace by a proper editor.
 *
 * /!\ ++++++++++++++++++++++ /!\
 */

export interface Props {
  /**
   * Despite having a JSONValue type in this project, `any` is used here for the time being
   * because other types cannot be cast to it.
   * @todo have this component work with a decoder and a strict type
   */
  value: any;
  label?: string;
  onChange?: (newVal: any) => void;
  readOnly?: boolean;
  validate?: (val: any) => result.Result<DecodeError[] | string, any>;
  placeholder?: string;
}

export interface State {
  // The current typed JSON that may or may not be valid.
  // This value is usually displayed in the UI to avoid jumps
  // in the cursor when the output is parsed.
  raw: string;
  error?: string;
}

const StyledTextarea = styled(Textarea)`
  min-width: auto;
  max-width: 100%;
  width: 100%;
  & textarea {
    min-height: 95px;
  }
`;

const parseObject = (raw: string, validate?: (val: any) => result.Result<DecodeError[] | string, any>) => {
  const parsed = safeJsonParse(raw);
  if (result.isError(parsed)) {
    return result.error("Invalid JSON");
  }
  if (parsed.value !== undefined && validate) {
    return validate(parsed.value);
  }
  return parsed;
};

const JsonTextarea = (props: Props) => {
  const [raw, setRaw] = useState(JSON.stringify(props.value, null, 2));
  const [error, setError] = useState<string | undefined>(undefined);

  return (
    <StyledTextarea
      disabled={props.readOnly}
      code
      label={props.label}
      value={raw}
      error={error}
      placeholder={props.placeholder}
      onFocus={() => {
        if (error) {
          setError(undefined);
        }
      }}
      onBlur={() => {
        // On blur, the component checks if the JSON is valid unless the raw value is empty.
        // If the JSON isn't valid, an error is set that is cleared upon re-focusing.
        if (!raw) {
          return;
        }
        const parsed = parseObject(raw, props.validate);
        if (result.isError(parsed)) {
          if (Array.isArray(parsed.value)) {
            const err = parsed.value[0];
            setError(`${err.error} at ${err.path.join(".")}`);
          } else {
            setError(parsed.value);
          }
        }
      }}
      onChange={newVal => {
        setRaw(newVal);
        const parsed = parseObject(newVal, props.validate);
        if (result.isSuccess(parsed) && props.onChange) {
          props.onChange(parsed.value);
        }
      }}
    />
  );
};

export default JsonTextarea;
