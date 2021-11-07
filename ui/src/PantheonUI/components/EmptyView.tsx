import { Button, styled } from "@operational/components";
import React from "react";

export interface Props {
  body: string;
  linkLabel: string;
  linkTo: string;
}

const Container = styled("div")({
  label: "empty-view",
  display: "block",
  maxWidth: 400,
  borderRadius: 4,
  padding: 32,
  textAlign: "center",
});

const Body = styled("div")({
  opacity: 0.8,
  margin: `auto auto 16px auto`,
});

const EmptyView = (props: Props) => (
  <Container>
    <Body>{props.body}</Body>
    <Button to={props.linkTo} color="info">
      {props.linkLabel}
    </Button>
  </Container>
);

export default EmptyView;
