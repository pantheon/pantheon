import { Card, Page, Paginator, Status, Table } from "@operational/components";
import { OperationalRouter } from "@operational/router";
import differenceInMilliseconds from "date-fns/differenceInMilliseconds";
import format from "date-fns/format";
import React from "react";

import { displayType, GetQueryLogs, iconName } from "../data/queryLog";
import { GetSchemas } from "../data/schema";

export interface Props {
  catalogId: string;
  page: number;
}

const queryStatus = (queryCompletionType: string) => {
  if (queryCompletionType === "Failed") {
    return <Status state="error">{queryCompletionType}</Status>;
  }
  if (queryCompletionType === "Succeeded") {
    return <Status state="success">{queryCompletionType}</Status>;
  }
  return <Status state="running">{queryCompletionType}</Status>;
};

const QueryLogs: React.SFC<Props> = props => {
  return (
    <GetSchemas catalogId={props.catalogId}>
      {schemas => (
        <GetQueryLogs catalogId={props.catalogId} page={props.page}>
          {queryLogs => (
            <Page title="Query Logs" loading={!queryLogs}>
              {queryLogs && (
                <Card
                  title={
                    <>
                      Query Logs <b>({queryLogs ? queryLogs.page.itemCount : "-"})</b>
                    </>
                  }
                >
                  <OperationalRouter>
                    {({ pushState }) => (
                      <>
                        <Table
                          data={queryLogs.data}
                          onRowClick={query => {
                            pushState(`executed-queries/${query.id}`);
                          }}
                          icon={iconName}
                          columns={[
                            {
                              heading: "Type",
                              cell: displayType,
                            },
                            {
                              heading: "Status",
                              cell: queryLog =>
                                queryLog.completionStatus ? queryStatus(queryLog.completionStatus.type) : "",
                            },
                            {
                              heading: "Started at",
                              cell: queryLog =>
                                queryLog.startedAt && format(new Date(queryLog.startedAt), "MMM do, yyyy 'at' HH:mm"),
                            },
                            {
                              heading: "Target",
                              cell: queryLog => {
                                if (!schemas) {
                                  return "";
                                }
                                const targetSchema = schemas.data.filter(schema => schema.id === queryLog.schemaId)[0];
                                if (!targetSchema) {
                                  return "";
                                }
                                return targetSchema.name;
                              },
                            },
                            {
                              heading: "Running time",
                              cell: queryLog =>
                                queryLog.completedAt && queryLog.startedAt
                                  ? `${differenceInMilliseconds(
                                      new Date(queryLog.completedAt),
                                      new Date(queryLog.startedAt),
                                    ) / 1000}s`
                                  : "",
                            },
                            { heading: "Custom reference", cell: queryLog => queryLog.customReference || "" },
                          ]}
                          rowActions={queryLog => [
                            {
                              label: "Details",
                              onClick: () => {
                                pushState(`executed-queries/${queryLog.id}`);
                              },
                            },
                          ]}
                        />
                        <Paginator
                          page={props.page}
                          itemCount={queryLogs.page.itemCount}
                          itemsPerPage={queryLogs.page.itemsPerPage}
                          onChange={newPage => {
                            pushState(`executed-queries/?page=${newPage}`);
                          }}
                        />
                      </>
                    )}
                  </OperationalRouter>
                </Card>
              )}
            </Page>
          )}
        </GetQueryLogs>
      )}
    </GetSchemas>
  );
};

export default QueryLogs;
