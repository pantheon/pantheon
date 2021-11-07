import { styled } from "@operational/components";
import React from "react";

export interface LayoutProps {
  children:  // visual layout - 4 areas
    | [React.ReactElement, React.ReactElement, React.ReactElement, React.ReactElement]
    // code layout - 3 areas
    | [React.ReactElement, React.ReactElement, React.ReactElement];
}

const Container = styled("div")<{ mode: "visual" | "code" }>(({ theme, mode }) => ({
  height: "100%",
  display: "grid",
  gridColumnGap: theme.space.small,
  gridRowGap: theme.space.small,
  padding: theme.space.small,
  // Slots
  "& > div": {
    position: "relative",
  },
  // Cards
  ".expanded": {
    margin: 0,
    position: "absolute",
    overflow: "auto",
    top: 0,
    left: 0,
    bottom: 0,
    right: 0,
  },
  ...(mode === "visual"
    ? {
        gridTemplateColumns: "240px 240px 1fr",
        gridTemplateRows: "min-content 1fr",
        gridTemplateAreas: `
    "r1 r2 r3"
    "r1 r2 r4"
  `,
      }
    : {
        gridTemplateColumns: "240px 1fr",
        gridTemplateRows: "240px 1fr",
        gridTemplateAreas: `
    "r1 r2"
    "r1 r3"
  `,
      }),
}));

const Layout = (props: LayoutProps) => (
  <Container mode={React.Children.toArray(props.children).length === 3 ? "code" : "visual"}>
    {React.Children.toArray(props.children).map((area, i) => (
      <div style={{ gridArea: `r${i + 1}` }} key={area.key || i}>
        {area}
      </div>
    ))}
  </Container>
);

export default Layout;
