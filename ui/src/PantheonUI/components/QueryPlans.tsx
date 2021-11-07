import { CardColumn, Code, Message, CardColumns } from "@operational/components";
import React from "react";

import { QueryLog } from "../data/queryLog";

const QueryPlans: React.SFC<{ queryLog: QueryLog }> = props => (
  <CardColumns columns={1}>
    {props.queryLog.completionStatus && props.queryLog.completionStatus.type === "Failed" && (
      <Message type="error" title="Failed with Error" body={props.queryLog.completionStatus.msg} />
    )}
    {props.queryLog.plan && (
      <CardColumn title="Pantheon Plan">
        <Code>{props.queryLog.plan}</Code>
      </CardColumn>
    )}
    {props.queryLog.backendLogicalPlan && (
      <CardColumn title="Logical Plan">
        <Code>{props.queryLog.backendLogicalPlan}</Code>
      </CardColumn>
    )}
    {props.queryLog.backendPhysicalPlan && (
      <CardColumn title="Physical Plan">
        <Code>{props.queryLog.backendPhysicalPlan}</Code>
      </CardColumn>
    )}
  </CardColumns>
);

export default QueryPlans;
