import { styled, Toggle } from "@operational/components";
import React from "react";

export interface CodeViewToggleProps {
  isCodeView: boolean;
  onChange: (isCodeView: boolean) => void;
}

const codeString = "Code";

const visualString = "Visual";

const Container = styled("div")`
  margin-left: ${props => props.theme.space.small}px;
`;

const CodeViewToggle: React.SFC<CodeViewToggleProps> = props => (
  <Container>
    <Toggle
      options={[
        {
          label: visualString,
          value: visualString,
        },
        {
          label: codeString,
          value: codeString,
        },
      ]}
      value={props.isCodeView ? codeString : visualString}
      onChange={val => {
        switch (val) {
          case codeString:
            props.onChange(true);
            return;
          case visualString:
            props.onChange(false);
            return;
          default:
            return;
        }
      }}
      condensed
    />
  </Container>
);

export default CodeViewToggle;
