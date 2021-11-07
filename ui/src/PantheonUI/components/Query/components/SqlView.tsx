import { Button, Card, CardSection, Code, Message, ModalConfirmContext } from "@operational/components";
import isEqual from "lodash/isEqual";
import React from "react";
import { connect } from "react-redux";
import uuidV1 from "uuid/v1";

import { Schema } from "../../../data/schema";
import { decodeFromUrl, getQueryFromUrl } from "../../../utils";
import CodeEditor, { Editor } from "../../CodeEditor";
import { getConfig } from "../../Config";
import NakedCard from "../../NakedCard";
import QueryPlans from "../../QueryPlans";
import Results from "../../Results";
import * as actions from "../actions";
import { SqlState, State as ReduxState } from "../state";
import { RunButtonContainer, StaleResultsWarning, ResultsCard, ResultsCardContent } from "./Miscellaneous";
import SimpleLayout from "./SimpleLayout";
import { DragHandlers, TablesSideBar } from "./TablesSideBar";

export interface Props extends SqlState {
  catalogId: string;
  schemaId: string;
  schemas?: Schema[];
  confirm: ModalConfirmContext["confirm"];
  // Actions
  changeSqlQuery: typeof actions.changeSqlQuery;
  cancelSqlQuery: typeof actions.cancelSqlQuery;
  requestSqlQuery: typeof actions.requestSqlQuery;
}

export interface State {
  isDraggingOverQuery: boolean;
  ref?: string;
}

class SqlView extends React.Component<Props, State> {
  public readonly state: State = {
    isDraggingOverQuery: false,
    ref: undefined,
  };

  private editor?: Editor;

  private dragHandlers: DragHandlers = {
    onTableDragStart: table => this.setState(() => ({ ref: table.name })),
    onTableDragEnd: () => this.setState(() => ({ ref: undefined })),
    onColumnDragStart: columnName => this.setState(() => ({ ref: columnName })),
    onColumnDragEnd: () => this.setState(() => ({ ref: undefined })),
  };

  private didQueryChangeAfterLastResponse(): boolean {
    if (!this.props.request) {
      return false;
    }
    return !isEqual(this.props.query, this.props.request.query);
  }

  public render() {
    const { request } = this.props;
    return (
      <SimpleLayout
        region1={
          <NakedCard
            sections={
              <CardSection title="Tables">
                {this.props.tables && this.props.tables.type === "success" && (
                  <TablesSideBar tables={this.props.tables.value} dragHandlers={this.dragHandlers} />
                )}
              </CardSection>
            }
          />
        }
        region2={
          <Card
            sections={
              <CardSection
                noHorizontalPadding
                dragAndDropFeedback={
                  this.state.ref ? (this.state.isDraggingOverQuery ? "dropping" : "validTarget") : undefined
                }
                title="Query"
                onDragEnter={(e: React.DragEvent<HTMLElement>): void => {
                  e.preventDefault();
                }}
                onDragOver={ev => {
                  ev.preventDefault();
                  /**
                   * This additional check is necessary to prevent blocking the render loop from the
                   * multiple re-renders from dragover events.
                   */
                  if (this.state.isDraggingOverQuery) {
                    return;
                  }
                  this.setState(() => ({ isDraggingOverQuery: true }));
                }}
                onDragLeave={() => {
                  this.setState(() => ({ isDraggingOverQuery: false }));
                }}
                onDrop={(): void => {
                  if (this.editor && this.state.ref) {
                    this.editor.insertAtCurrentPosition(this.state.ref);
                    this.props.changeSqlQuery(this.editor.getValue());
                    this.setState(() => ({
                      ref: undefined,
                    }));
                  }
                }}
              >
                <CodeEditor
                  height={200}
                  codeEditorRef={editor => {
                    this.editor = editor;
                  }}
                  value={this.props.query.sql}
                  language="sql"
                  onChange={newCode => {
                    this.props.changeSqlQuery(newCode);
                  }}
                />
              </CardSection>
            }
          />
        }
        region3={
          <ResultsCard
            action={undefined}
            leftOfTabs={
              <RunButtonContainer>
                {request && !request.response ? (
                  <Button
                    onClick={() => {
                      this.props.cancelSqlQuery();
                    }}
                    condensed
                    color="warning"
                  >
                    Cancel query
                  </Button>
                ) : (
                  <Button
                    color="primary"
                    condensed
                    data-cy="sql-query-editor-run-query-button"
                    onClick={() => {
                      this.props.requestSqlQuery(new Date().getTime(), uuidV1());
                    }}
                    icon="Play"
                  >
                    Run query
                  </Button>
                )}
              </RunButtonContainer>
            }
            tabs={[
              {
                name: "Results",
                children: (
                  <ResultsCardContent>
                    {this.didQueryChangeAfterLastResponse() && <StaleResultsWarning />}
                    {this.props.isQueryCancelled && (
                      <Message type="warning" title="Query Cancelled" body="The query execution was cancelled." />
                    )}
                    <Results
                      response={
                        this.props.request && this.props.request.response && this.props.request.response.payload
                      }
                    />
                  </ResultsCardContent>
                ),
                loading: request && !request.response,
                ...(() => {
                  if (!request || !request.response) {
                    return {};
                  }
                  const { payload } = request.response;
                  if (payload.type === "success") {
                    return {
                      icon: "Yes",
                      iconColor: "success",
                    };
                  }
                  return {
                    icon: "No",
                    iconColor: "error",
                  };
                })(),
              },
              {
                name: "Logs",
                children: (
                  <ResultsCardContent>
                    {this.didQueryChangeAfterLastResponse() && <StaleResultsWarning />}

                    {(queryLog => queryLog && <QueryPlans queryLog={queryLog} />)(
                      this.props.request && this.props.request.queryLog,
                    )}
                  </ResultsCardContent>
                ),
              },
              {
                name: "CURL",
                children: (
                  <ResultsCardContent>
                    <Code syntax="bash">
                      {`curl '${getConfig("backend")}/catalogs/${this.props.catalogId}/schemas/${
                        this.props.schemaId
                      }/query' \\
  -H "Authorization: Bearer $ACCOUNT_TOKEN" \\
  -H 'Content-Type: application/json' \\
  --data-binary '${JSON.stringify(decodeFromUrl(getQueryFromUrl("querydata") || ""))}' \\
  --compressed`}
                    </Code>
                  </ResultsCardContent>
                ),
              },
            ]}
          />
        }
      />
    );
  }
}

const mapStateToProps = (state: ReduxState) => ({ ...state.Sql });

const mapDispatchToProps = {
  changeSqlQuery: actions.changeSqlQuery,
  cancelSqlQuery: actions.cancelSqlQuery,
  requestSqlQuery: actions.requestSqlQuery,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(SqlView);
