import { SidenavHeader, SidenavItem } from "@operational/components";
import React from "react";
import { getConfig } from "./components/Config";

export interface Props {
  basePath: string;
}

const PantheonUISidenavHeader: React.SFC<Props> = ({ basePath }) => (
  <SidenavHeader label="Pantheon" active compact={getConfig("compactSidenav")}>
    <SidenavItem
      data-cy="pantheon--sidenav__endpoints"
      label="Endpoints"
      to={`${basePath}/endpoints`}
      active={window.location.href.includes("endpoints")}
      icon="Endpoint"
    />
    <SidenavItem
      data-cy="pantheon--sidenav__data-sources"
      label="Data Sources"
      to={`${basePath}/data-sources`}
      active={window.location.href.includes("data-sources")}
      icon="Database"
    />
    <SidenavItem
      data-cy="pantheon--sidenav__schemas"
      label="Schemas"
      to={`${basePath}/schemas`}
      active={window.location.href.includes("schemas")}
      icon="Schema"
    />
    <SidenavItem
      data-cy="pantheon--sidenav__query-logs"
      label="Query Logs"
      to={`${basePath}/executed-queries`}
      active={window.location.href.includes("executed-queries")}
      icon="Document"
    />
  </SidenavHeader>
);

export default PantheonUISidenavHeader;
