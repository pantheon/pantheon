import { ConfirmBodyProps, Form, Select, Textarea } from "@operational/components";
import React from "react";
import { Operator, operators } from "../PslFilter/parseFilter";

const isMultival = (operator: Operator) => ["like", "in", "not in"].includes(operator);

export interface AddFilterModalState {
  dimension: string;
  type: Operator;
  value: string;
}

export const AddFilterModal: React.SFC<ConfirmBodyProps<AddFilterModalState>> = props => (
  <Form onSubmit={e => e.preventDefault()}>
    <Select
      label="Filter type"
      options={Object.keys(operators).map(i => ({ label: i, value: i }))}
      value={props.confirmState.type}
      onChange={val => props.setConfirmState({ type: val as Operator })}
    />
    {!props.confirmState.type.includes("null") && (
      <Textarea
        label="Filter value"
        value={props.confirmState.value}
        onChange={value => props.setConfirmState({ value })}
        placeholder={isMultival(props.confirmState.type) ? "'Paris', 'Berlin'" : "'Berlin'"}
      />
    )}
  </Form>
);
