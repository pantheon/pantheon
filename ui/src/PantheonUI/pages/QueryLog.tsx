import { Button, Card, CardColumn, CardColumns, CardItem, Icon, Page, Status, styled } from "@operational/components";
import { OperationalRouter } from "@operational/router";
import differenceInMilliseconds from "date-fns/differenceInMilliseconds";
import format from "date-fns/format";
import React from "react";

import { QueryPlans, ValueWithIcon } from "../components";
import { GetQueryLog, QueryLog } from "../data/queryLog";
import { GetSchema } from "../data/schema";
import { encodeForUrl } from "../utils";

export interface Props {
  catalogId: string;
  queryId: string;
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

const queryType = (queryLog: QueryLog) => {
  if (queryLog.query && queryLog.type === "Schema" && typeof queryLog.query !== "string") {
    return <ValueWithIcon icon="Olap" label={queryLog.query.type} />;
  }
  return "Native";
};

const GoToSchema = styled("div")`
  display: flex;
  align-items: center;
`;

const makeQueryEditorUrl = (queryLog: QueryLog): string | undefined => {
  if (queryLog.type === "Schema" && queryLog.schemaId) {
    return `schemas/${queryLog.schemaId}/query?querydata=${encodeForUrl(queryLog.query)}`;
  }
  if (queryLog.type === "Native" && queryLog.dataSourceId && typeof queryLog.query === "string") {
    return `data-sources/${queryLog.dataSourceId}/native-query?querydata=${encodeForUrl(queryLog.query)}`;
  }
  return undefined;
};

const QueryLogPage: React.SFC<Props> = props => (
  <GetQueryLog catalogId={props.catalogId} queryId={props.queryId}>
    {(queryLog, { loading }) => (
      <Page
        title="Query Log"
        loading={loading}
        actions={(() => {
          if (!queryLog) {
            return null;
          }
          const queryEditorUrl = makeQueryEditorUrl(queryLog);
          return (
            queryEditorUrl && (
              <OperationalRouter>
                {({ pushState }) => (
                  <Button condensed color="ghost" icon="Document" onClick={() => pushState(queryEditorUrl)}>
                    Open in Query Editor
                  </Button>
                )}
              </OperationalRouter>
            )
          );
        })()}
      >
        {queryLog && (
          <>
            <Card title="Query Details">
              <CardColumns>
                <CardColumn>
                  <CardItem title="ID" value={queryLog.id} />
                  {queryLog.completionStatus && (
                    <CardItem title="Status" value={queryStatus(queryLog.completionStatus.type)} />
                  )}
                  <CardItem
                    title="Target"
                    value={
                      queryLog.schemaId && (
                        <GetSchema catalogId={props.catalogId} schemaId={queryLog.schemaId}>
                          {schema =>
                            schema ? (
                              <OperationalRouter>
                                {({ pushState }) => (
                                  <GoToSchema onClick={() => pushState(`schemas/${queryLog.schemaId}`)}>
                                    {schema.name}
                                    <Icon name="Open" size={12} right />
                                  </GoToSchema>
                                )}
                              </OperationalRouter>
                            ) : (
                              "Loading..."
                            )
                          }
                        </GetSchema>
                      )
                    }
                  />
                  <CardItem title="Custom reference" value={queryLog.customReference || "N/A"} />
                </CardColumn>
                <CardColumn>
                  <CardItem title="Query type" value={queryType(queryLog)} />
                  {queryLog.startedAt && (
                    <CardItem
                      title="Started at"
                      value={format(new Date(queryLog.startedAt), "MMM do, yyyy 'at' HH:mm")}
                    />
                  )}
                  {queryLog.completedAt && (
                    <CardItem
                      title="Completed at"
                      value={format(new Date(queryLog.completedAt), "MMM do, yyyy 'at' HH:mm")}
                    />
                  )}
                  {queryLog.startedAt && queryLog.completedAt && (
                    <CardItem
                      title="Running time"
                      value={`${differenceInMilliseconds(new Date(queryLog.completedAt), new Date(queryLog.startedAt)) /
                        1000}s`}
                    />
                  )}
                </CardColumn>
              </CardColumns>
            </Card>
            {queryLog.type === "Schema" && (
              <Card title="Plans">
                <QueryPlans queryLog={queryLog} />
              </Card>
            )}
          </>
        )}
      </Page>
    )}
  </GetQueryLog>
);

export default QueryLogPage;
