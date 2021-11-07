/**
 * Contains very small, specialized components that are re-used across different components.
 */
import { Card, Message, styled, Textarea } from "@operational/components";
import React from "react";
import { BackendError } from "../../../types";

export const CompatibilityError: React.SFC<{ error?: BackendError }> = ({ error }) => {
  let body = "The selected query contains fields that are not compatible.";

  if (error && error.fieldErrors) {
    // Do nothing, errors will be shown in fields section
  } else {
    body += " This can happen when a schema is updated and you have a query built on its previous version.";
  }

  return <Message type="error" title="Compatibility error" body={body} />;
};

export const StaleResultsWarning: React.SFC = () => (
  <Message type="warning" title="Your query has changed" body="Run again to get new results" />
);

export const RunButtonContainer = styled("div")`
  width: 120px;
`;

export const StyledTextarea = styled(Textarea)`
  min-width: auto;
  width: 100%;
`;

export const LimitContainer = styled("div")`
  margin-bottom: 16px;
  & label: {
    min-width: 90px;
    margin-bottom: 0px;
  }
`;

export const SplitCard = styled(Card)`
  /** Expands card sections to occupy 100% of the card height */
  & > div {
    height: 100%;
  }

  /** Sizes horizontally stacked card sections equally */
  & > div > div {
    overflow-x: auto;
    flex: 0 0 50%;
  }
` as typeof Card;

export const ResultsCard = styled(Card)`
  /* The content of the card, under the header */
  > div:nth-child(2) {
    /* minus height of the header */
    height: calc(100% - 40px);
    width: 100%;
    /* move padding from here to the element with scroll - ResultsCardContent*/
    padding: 0px;
  }
` as typeof Card;

export const ResultsCardContent = styled("div")`
  width: 100%;
  height: 100%;
  overflow: auto;
  padding: ${props => props.theme.space.element}px;
`;
