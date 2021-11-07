import { PageContent, PageContentProps, styled } from "@operational/components";
import React from "react";

/**
 * [Type]PageComponent components like this are an alternative API to `@operational/components`'
 * `PageContent`, `PageArea` and `PageAreas`.
 * They are a WIP experiment to see if they suit this project and others better, to be merged into
 * `@operational/components` if successful.
 *
 * The starting point is a `PageContent` instance that fills the available space in the screen,
 * which can either scroll or define a fixed-screen layout with a specific arrangement of cards.
 * There is a separate, specifically named component for each of these arrangements, re-used
 * across one or more apps.
 */

export interface Props {
  noPadding?: boolean;
  children?: PageContentProps["children"];
}

const StyledPageContent = styled(PageContent)`
  position: relative;
  overflow: hidden;
  height: 100%;
` as typeof PageContent;

const InnerContainer = styled("div")<{ noPadding?: boolean }>`
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  padding: ${props => (props.noPadding ? 0 : props.theme.space.element)}px;
`;

const GridPageContent = ({ noPadding, children, ...props }: Props) => (
  <StyledPageContent fill {...props}>
    {modalConfirmContext => (
      <InnerContainer noPadding={noPadding}>
        {typeof children === "function" ? children(modalConfirmContext) : children}
      </InnerContainer>
    )}
  </StyledPageContent>
);

export default GridPageContent;
