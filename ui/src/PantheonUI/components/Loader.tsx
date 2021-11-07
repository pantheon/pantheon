import { Spinner, styled } from "@operational/components";
import React from "react";

export interface LoaderProps {
  backgroundColor?: string;
}

export const Container = styled("div")<{ backgroundColor_?: string }>`
  background-color: ${props => props.backgroundColor_ || "transparent"};
  height: 100%;
  padding-top: 80px;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  color: ${props => props.theme.color.text.lighter};
`;

const Loader: React.SFC<LoaderProps> = ({ backgroundColor, ...props }) => (
  <Container backgroundColor_={backgroundColor} {...props}>
    <Spinner />
  </Container>
);

export default Loader;
