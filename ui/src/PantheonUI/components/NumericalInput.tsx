import { Input } from "@operational/components";
import React from "react";

export interface Props {
  disabled?: boolean;
  label?: string;
  value: number;
  onChange: (val: number) => void;
}

export interface State {
  unparsedValue: null | string;
}

class NumericalInput extends React.Component<Props, State> {
  public state: State = {
    unparsedValue: null,
  };

  public render() {
    return (
      <Input
        value={this.state.unparsedValue === null ? String(this.props.value) : this.state.unparsedValue}
        disabled={this.props.disabled}
        label={this.props.label}
        onChange={newValue => {
          if (newValue.trim() === "") {
            this.setState(() => ({
              unparsedValue: newValue,
            }));
            this.props.onChange(0);
          }
          const parsed = parseInt(newValue, 10);
          if (isNaN(parsed)) {
            this.setState(() => ({
              unparsedValue: newValue,
            }));
            return;
          }
          this.setState(() => ({
            unparsedValue: null,
          }));
          this.props.onChange(parsed);
        }}
      />
    );
  }
}

export default NumericalInput;
