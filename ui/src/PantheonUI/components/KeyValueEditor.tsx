/**
 * This component edits key-value pairs, maintaining the result in an object
 */
import { Button, Icon, Input, styled } from "@operational/components";
import fromPairs from "lodash/fromPairs";
import isEqual from "lodash/isEqual";
import uniqBy from "lodash/uniqBy";
import React from "react";

import { error, Result, success } from "../utils/result";

export interface Props {
  data: Data;
  onChange: (data: Data) => void;
}

export type Data = Record<string, string>;

/**
 * RawData is a draft copy of the final output data that is maintained independently while
 * the user is doing the editing, and send via the `onChange` prop when appropriate.
 * It protects against unpredictable reordering of keys that would happen if the data were
 * maintained as an object, as well as keeps work-in-progress data in the UI while the user
 * temporarily adds duplicated keys or empty values.
 */
export type RawData = Array<{ key: string; value: string }>;

export interface State {
  raw: RawData;
}

const Container = styled("div")`
  display: block;
`;

const KeyValueContainer = styled("div")`
  display: flex;
  align-items: flex-end;
  justify-content: flex-start;
  margin-bottom: 20px;
  & label {
    width: 240px;
    flex: 0 0 240px;
  }
  :last-child {
    margin-bottom: 0;
  }
`;

const RemoveButton = styled("div")`
  width: 36px;
  height: 36px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: ${props => props.theme.color.error};
  :hover {
    background-color: ${props => props.theme.color.background.lighter};
  }
`;

const fromRaw = (rawData: RawData): Result<string, Data> =>
  rawData.length === uniqBy(rawData, v => v.key).length
    ? success(fromPairs(rawData.map(({ key, value }) => [key, value])))
    : error("Duplicate key");

const toRaw = (data: Data): RawData => Object.entries(data).map(([key, value]) => ({ key, value }));

class KeyValueEditor extends React.Component<Props, State> {
  public constructor(props: Props) {
    super(props);
    this.state = {
      raw: toRaw(props.data),
    };
  }

  public componentDidUpdate(prevProps: Props) {
    const draftRawData = fromRaw(this.state.raw);
    if (
      !isEqual(this.props.data, prevProps.data) &&
      !(draftRawData.type === "success" && isEqual(draftRawData.value, this.props.data))
    ) {
      this.setState(() => ({
        raw: toRaw(this.props.data),
      }));
    }
  }

  private updateProps = () => {
    const dataResult = fromRaw(this.state.raw);
    if (dataResult.type === "success") {
      this.props.onChange(dataResult.value);
    }
  };

  public render() {
    return (
      <Container>
        {this.state.raw.map(({ key, value }, index) => (
          <KeyValueContainer key={index}>
            <Input
              label="Key"
              value={key}
              data-cy={`pantheon--data-source-wizard__custom-parameter-key-${index}`}
              onChange={(newKey: string) => {
                this.setState(
                  prevState => ({
                    raw: prevState.raw.map((keyValuePair, keyValuePairIndex) =>
                      index === keyValuePairIndex ? { key: newKey, value: keyValuePair.value } : keyValuePair,
                    ),
                  }),
                  this.updateProps,
                );
              }}
            />
            <Input
              label="Value"
              value={value}
              data-cy={`pantheon--data-source-wizard__custom-parameter-value-${index}`}
              onChange={(newValue: string) => {
                this.setState(
                  prevState => ({
                    raw: prevState.raw.map((keyValuePair, keyValuePairIndex) =>
                      index === keyValuePairIndex ? { key: keyValuePair.key, value: newValue } : keyValuePair,
                    ),
                  }),
                  this.updateProps,
                );
              }}
            />
            <RemoveButton
              onClick={() => {
                this.setState(
                  prevState => ({
                    raw: prevState.raw.filter((_, keyValuePairIndex) => index !== keyValuePairIndex),
                  }),
                  this.updateProps,
                );
              }}
            >
              <Icon name="No" />
            </RemoveButton>
          </KeyValueContainer>
        ))}
        <Button
          onClick={() => {
            this.setState(prevState => ({
              raw: [...prevState.raw, { key: "", value: "" }],
            }));
          }}
          color="grey"
          textColor="basic"
          icon="Add"
          iconColor="primary"
        >
          Add parameter
        </Button>
      </Container>
    );
  }
}

export default KeyValueEditor;
