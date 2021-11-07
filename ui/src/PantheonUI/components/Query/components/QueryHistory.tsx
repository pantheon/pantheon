import { Actions, Button, Progress, Table } from "@operational/components";
import { formatDistanceStrict } from "date-fns/esm";
import React, { FC } from "react";

import { AggregateQuery } from "../../../data/aggregateQuery";
import { GetQueryHistory } from "../../../data/GetQueryHistory";
import { displayType, iconName } from "../../../data/queryLog";
import { RecordQuery } from "../../../data/recordQuery";
import { SqlQuery } from "../../../data/sqlQuery";
import { exhaustiveCheck } from "../../../utils";
import { RestoreAggregateQuery, RestoreRecordQuery, RestoreSqlQuery } from "../actions";

export interface QueryHistoryProps {
  customReference: string;
  catalogId: string;
  restoreAggregateQuery: (payload: AggregateQuery) => RestoreAggregateQuery;
  restoreSqlQuery: (payload: SqlQuery) => RestoreSqlQuery;
  restoreRecordQuery: (payload: RecordQuery) => RestoreRecordQuery;
  close: () => void;
}

const QueryHistory: FC<QueryHistoryProps> = ({
  customReference,
  catalogId,
  restoreAggregateQuery,
  restoreSqlQuery,
  restoreRecordQuery,
  close,
}) => (
  <>
    <GetQueryHistory customRefPattern={customReference} catalogId={catalogId}>
      {response =>
        response ? (
          <Table
            data={response.data}
            icon={iconName}
            columns={[
              {
                heading: "Type",
                cell: displayType,
              },
              {
                heading: "Started",
                cell: queryLog =>
                  queryLog.startedAt && `${formatDistanceStrict(new Date(), new Date(queryLog.startedAt))} ago`,
              },
            ]}
            rowActions={queryLog =>
              queryLog.query
                ? [
                    {
                      label: "Revert to this Query",
                      onClick: () => {
                        if (!queryLog.query || typeof queryLog.query === "string") {
                          return;
                        }

                        switch (queryLog.query.type) {
                          case "Aggregate":
                            restoreAggregateQuery(queryLog.query);
                            close();
                            return;
                          case "Sql":
                            restoreSqlQuery(queryLog.query);
                            close();
                            return;
                          case "Record":
                            restoreRecordQuery(queryLog.query);
                            close();
                            return;
                          default:
                            return exhaustiveCheck(queryLog.query);
                        }
                      },
                    },
                  ]
                : []
            }
          />
        ) : (
          <Progress />
        )
      }
    </GetQueryHistory>
    <Actions>
      <Button onClick={() => close()}>Close</Button>
    </Actions>
  </>
);

export default QueryHistory;
