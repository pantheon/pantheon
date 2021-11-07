import { styled } from "@operational/components";
import React from "react";

import BaseLayout from "../BaseLayout";

/**
 * This is a custom layout component based on the following regions:
 *
 *  +-----+-----+-----+-----+
 *  |     |     |     |     |
 *  |  1  |  2  |  2  |  2  |
 *  |     |     |     |     |
 *  +-----------------------+
 *  |     |     |     |     |
 *  |  1  |  3  |  3  |  3  |
 *  |     |     |     |     |
 *  +-----------------------+
 *  |     |     |     |     |
 *  |  1  |  3  |  3  |  3  |
 *  |     |     |     |     |
 *  +-----------------------+
 *  |     |     |     |     |
 *  |  1  |  3  |  3  |  3  |
 *  |     |     |     |     |
 *  +-----+-----+-----+-----+
 *
 * The diagram made on http://asciiflow.com
 */

export interface LayoutProps {
  region1: React.ReactNode;
  region2: React.ReactNode;
  region3: React.ReactNode;
}

const Container = styled(BaseLayout)({
  height: "100%",
  gridTemplateColumns: "240px 1fr",
  gridTemplateRows: "180px 1fr",
  position: "absolute",
  top: 0,
  left: 0,
  right: 0,
  bottom: 0,
  gridTemplateAreas: `
    "r1 r2"
    "r1 r3"
  `,
});

const Layout: React.SFC<LayoutProps> = props => (
  <Container>
    <div style={{ gridArea: "r1" }}>{props.region1}</div>
    <div style={{ gridArea: "r2" }}>{props.region2}</div>
    <div style={{ gridArea: "r3" }}>{props.region3}</div>
  </Container>
);

export default Layout;
