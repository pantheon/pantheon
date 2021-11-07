import { Icon, IconName, styled } from "@operational/components";
import React from "react";

export interface ValueWithIconProps {
  icon: IconName;
  label: string;
}

const ValueWithIconContainer = styled("p")`
  margin: 0;
  display: flex;
  align-items: center;
  justify-content: flex-start;
  & svg {
    color: ${props => props.theme.color.text.lightest};
  }
`;

export const ValueWithIcon: React.SFC<ValueWithIconProps> = props => (
  <ValueWithIconContainer>
    <Icon name={props.icon} left />
    {props.label}
  </ValueWithIconContainer>
);

export { default as EmptyView } from "./EmptyView";
export { default as NumericalInput } from "./NumericalInput";
export { default as Query } from "./Query";
export { default as GridPageContent } from "./GridPageContent";
export { default as CodeEditor } from "./CodeEditor";
export { default as JsonTextarea } from "./JsonTextarea";
export { default as PendingStatus } from "./PendingStatus";
export { default as NativeQuery } from "./NativeQuery";
export { default as ProductBox } from "./ProductBox";
export { default as Loader } from "./Loader";
export { default as QueryPlans } from "./QueryPlans";
export * from "./DataSourceWizard";
export * from "./EditButton";
export * from "./EditSchemaActions";
