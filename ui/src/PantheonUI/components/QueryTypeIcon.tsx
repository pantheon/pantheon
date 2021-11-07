import { Icon, IconName, styled } from "@operational/components";
import React from "react";
import { Endpoint } from "../data/endpoint";
import { QueryType } from "./Query/types";

export interface QueryTypeIconProps {
  endpoint: Endpoint;
}

const iconLookup: Record<QueryType, IconName> = {
  Aggregate: "Olap",
  Record: "Entity",
  Sql: "Sql",
};

const LightIcon = styled(Icon)`
  fill: ${({ theme }) => theme.color.text.lightest};
`;

export const QueryTypeIcon: React.SFC<QueryTypeIconProps> = ({ endpoint }) => {
  return (
    <div style={{ display: "flex", alignItems: "center" }}>
      <LightIcon left name={iconLookup[endpoint.query.type] || "Schema"} /> {endpoint.query.type}
    </div>
  );
};
