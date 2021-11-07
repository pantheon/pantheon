import { styled } from "@operational/components";
import React from "react";

import BaseLayout from "../../BaseLayout";

/**
 * This is a custom layout component based on the following regions:
 *
 *  +-----+-----+-----+
 *  |     |     |     |
 *  |  1  |  2  |  3  |
 *  |     |     |     |
 *  |     |     |-----+
 *  |     |     |     |
 *  |     |     |  4  |
 *  |     |     |     |
 *  +-----------------+
 */

export interface ComplexLayoutProps {
  region1: React.ReactNode;
  region2: React.ReactNode;
  region3: React.ReactNode;
  region4: React.ReactNode;
}

const Container = styled(BaseLayout)({
  height: "100%",
  gridTemplateColumns: "240px 240px 1fr",
  gridTemplateRows: "180px 1fr",
  gridTemplateAreas: `
    "r1 r2 r3"
    "r1 r2 r4"
  `,
});

const ComplexLayout: React.SFC<ComplexLayoutProps> = props => (
  <Container>
    <div style={{ gridArea: "r1" }}>{props.region1}</div>
    <div style={{ gridArea: "r2" }}>{props.region2}</div>
    <div style={{ gridArea: "r3" }}>{props.region3}</div>
    <div style={{ gridArea: "r4" }}>{props.region4}</div>
  </Container>
);

export default ComplexLayout;
