import { Button, Message, styled, Table } from "@operational/components";
import React from "react";

import { Response } from "../data/pantheonQueryResponse";
import { ResponseOrError, BackendError } from "../types";

export interface Props {
  response?: ResponseOrError<Response>;
}

export interface State {
  showSample: boolean;
}

const ResultsTable = styled(Table)`
  width: max-content;
`;

export const ResultError: React.SFC<{ error: BackendError }> = ({ error }) => (
  <Message
    type="error"
    title="Query error"
    body={
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
    }
  />
);

class Results extends React.Component<Props, State> {
  public readonly state = {
    showSample: true,
  };

  public render() {
    if (!this.props.response) {
      return null;
    }
    if (this.props.response.type === "success") {
      const { value } = this.props.response;
      if (value.columns.length * value.rows.length > 50000) {
        return !this.state.showSample ? (
          <>
            <Message
              type="warning"
              title="The query result set is too large"
              body="Your query is too large to be displayed in the browser - please edit the query parameters to get a smaller set. You can still view a small sample of your data:"
            />
            <Button
              color="info"
              onClick={() => {
                this.setState(() => ({
                  showSample: true,
                }));
              }}
            >
              Show sample
            </Button>
          </>
        ) : (
          <>
            <Message
              type="warning"
              title="You are only viewing a result sample"
              body="Your query result set was too large to be displayed in the browser - we are showing a small sample only"
            />
            <ResultsTable
              data={value.rows.slice(0, 10)}
              columns={value.columns.map((col, index) => ({
                heading: col.ref,
                cell: (d: any) => d[index],
              }))}
            />
          </>
        );
      }
      return (
        <ResultsTable
          data={value.rows}
          columns={value.columns.map((col, index) => ({
            heading: col.ref,
            cell: (d: any) => d[index],
          }))}
        />
      );
    }
    return <ResultError error={this.props.response.value} />;
  }
}

export default Results;
