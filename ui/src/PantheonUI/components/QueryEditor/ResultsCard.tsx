import { Button, Card, IconName, Message, styled } from "@operational/components";
import { Grid, gridConfigToAccessors } from "@operational/visualizations";
import defaultGridConfig from "@operational/visualizations/lib/Grid/gridConfig";
import React, { useContext, useState } from "react";
import { useGet } from "restful-react";
import { QueryEditorContext } from ".";
import { BackendError } from "../../types";
import { Response } from "../queries";
import { getQuery } from "./QueryEditor.selector";
import queryResultToDataset from "./queryResultToDataset";

const ResultsCardContent = styled("div")`
  overflow: auto;
  padding: ${props => props.theme.space.element}px;
  position: absolute;
  top: ${props => props.theme.topbarHeight}px;
  left: 0;
  right: 0;
  bottom: 0;
`;

const ResultError: React.SFC<{ error: BackendError | string }> = ({ error }) => (
  <Message
    type="error"
    title="Query error"
    body={
      typeof error === "string" ? (
        error
      ) : (
        <>
          {error.errors.join(" ")}
          {error.fieldErrors && (
            <ul>
              {Object.entries(error.fieldErrors).map(([field, errors]) => (
                <>
                  <b>{field}</b> {errors.join(", ")}
                </>
              ))}
            </ul>
          )}
        </>
      )
    }
  />
);

const StaleResultsWarning: React.SFC = () => (
  <Message type="warning" title="Your query has changed" body="Run again to get new results" />
);

export const ResultsCard = () => {
  const { state } = useContext(QueryEditorContext);
  const [lastQuery, setLastQuery] = useState<string>();

  const { data, loading, error, cancel, refetch: sendQuery } = useGet<Response, BackendError>({
    path: `catalogs/${state.schema.catalogId}/schemas/${state.schema.id}/query`,
    lazy: true,
    localErrorOnly: true,
    requestOptions: {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: getQuery(state, true),
    },
  });

  let resultIcon: { icon?: IconName; iconColor?: string } = {};
  if (error) {
    resultIcon = { icon: "No", iconColor: "error" };
  } else if (data) {
    resultIcon = { icon: "Yes", iconColor: "success" };
  }

  return (
    <Card
      className="expanded" // flag for BaseLayout
      leftOfTabs={
        loading ? (
          <Button color="warning" condensed onClick={cancel}>
            Cancel query
          </Button>
        ) : (
          <Button
            color="primary"
            data-cy="pantheon--aggreate-view__run-query-button"
            condensed
            onClick={() => {
              sendQuery().then(() => setLastQuery(getQuery(state, true)));
            }}
            icon="Play"
            loading={loading}
          >
            Run query
          </Button>
        )
      }
      tabs={[
        {
          name: "Results",
          loading,
          ...resultIcon,
          children: (
            <ResultsCardContent>
              {lastQuery && getQuery(state, true) !== lastQuery && <StaleResultsWarning />}
              {error && <ResultError error={error.data} />}
              {data && (
                <Grid
                  data={queryResultToDataset(data)}
                  axes={{}}
                  accessors={gridConfigToAccessors(defaultGridConfig)}
                />
              )}
            </ResultsCardContent>
          ),
        },
      ]}
    />
  );
};
