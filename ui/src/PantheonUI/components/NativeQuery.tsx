import { Button, Card, CardSection, Message, Spinner, styled } from "@operational/components";
import React from "react";
import * as tucson from "tucson-decode";

import { Response as QueryResponse, sqlResponseDecoder } from "../data/pantheonQueryResponse";
import { ResponseOrError } from "../types";
import { customFetch, decodeFromUrl, encodeForUrl, getQueryFromUrl, setQueryInUrl } from "../utils";
import { isError } from "../utils/result";
import CodeEditor, { Editor } from "./CodeEditor";
import NakedCard from "./NakedCard";
import Layout from "./NativeQuery/Layout";
import { DragHandlers, TablesSideBar } from "./Query/components/TablesSideBar";
import { TableResponse, tableResponsesSchema } from "./Query/types";
import { processJsonResponse } from "./Query/utils";
import Results from "./Results";

export interface NativeQueryProps {
  catalogId: string;
  dataSourceId: string;
}

export interface State {
  query: string;
  tables: ResponseOrError<TableResponse[]>;
  isPending: boolean;
  isTablesRequestPending: boolean;
  response?: ResponseOrError<QueryResponse>;
  isDraggingOverQuery: boolean;
  ref?: string;
}

const queryDataKey = "querydata";

const queryDecoder = tucson.object({
  query: tucson.string,
});

const FixedButton = styled(Button)`
  position: absolute;
  left: ${props => props.theme.space.content}px;
  bottom: ${props => props.theme.space.content}px;
`;

class NativeQuery extends React.Component<NativeQueryProps, State> {
  public readonly state: State = {
    // WARNING: THIS IS UNSAFE BECAUSE decodeFromUrl RETURNS `any`.
    // Refactor this component asap.
    query: decodeFromUrl(getQueryFromUrl(queryDataKey) || "") || "",
    tables: { type: "success", value: [] },
    isPending: false,
    isTablesRequestPending: false,
    isDraggingOverQuery: false,
    ref: undefined,
  };

  private editor?: Editor;

  constructor(props: NativeQueryProps) {
    super(props);
    const queryFromUrl = getQueryFromUrl(queryDataKey);
    if (!queryFromUrl) {
      return;
    }
    const decodedQuery = queryDecoder(decodeFromUrl(queryFromUrl));
    if (decodedQuery.type === "success") {
      this.syncEditor(decodedQuery.value.query);
      this.setState(() => ({
        query: decodedQuery.value.query,
      }));
    } else {
      console.warn(decodedQuery.value);
    }
  }

  private syncEditor(newQuery?: string) {
    if (this.editor) {
      this.editor.setAndClearUndoStack(this.state.query || newQuery || "");
    }
  }

  private fetchTables() {
    this.setState(() => ({
      isTablesRequestPending: true,
    }));
    customFetch(`/catalogs/${this.props.catalogId}/dataSources/${this.props.dataSourceId}/tables`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then(processJsonResponse(tableResponsesSchema))
      .then(response => {
        this.setState(() => ({
          tables: response,
          isPending: false,
          isTablesRequestPending: false,
        }));
      });
  }

  private fetchResults() {
    this.setState(() => ({
      isPending: true,
    }));
    customFetch(`/catalogs/${this.props.catalogId}/dataSources/${this.props.dataSourceId}/nativeQuery`, {
      method: "POST",
      body: JSON.stringify({
        query: this.state.query,
        customReference: "ui-nativequery",
      }),
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then(processJsonResponse(sqlResponseDecoder))
      .then(response => {
        this.setState(() => ({
          response,
          isPending: false,
        }));
      });
  }

  public componentDidMount() {
    this.fetchTables();
  }

  private dragHandlers: DragHandlers = {
    onTableDragStart: table => this.setState(() => ({ ref: table.name })),
    onTableDragEnd: () => this.setState(() => ({ ref: undefined })),
    onColumnDragStart: columnName => this.setState(() => ({ ref: columnName })),
    onColumnDragEnd: () => this.setState(() => ({ ref: undefined })),
  };

  public render() {
    return (
      <Layout
        region1={
          <NakedCard
            sections={
              <CardSection>
                {this.state.isTablesRequestPending ? (
                  <NakedCard {...{ style: { display: "flex", justifyContent: "center", alignItems: "center" } }}>
                    <Spinner />
                  </NakedCard>
                ) : isError(this.state.tables) ? (
                  <Message type="error" title="Tables not available" body={JSON.stringify(this.state.tables.value)} />
                ) : (
                  <TablesSideBar tables={this.state.tables.value} dragHandlers={this.dragHandlers} />
                )}
              </CardSection>
            }
          />
        }
        region2={
          <Card
            sections={
              <>
                <CardSection
                  title="Query"
                  dragAndDropFeedback={
                    this.state.ref ? (this.state.isDraggingOverQuery ? "dropping" : "validTarget") : undefined
                  }
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
                  onDrop={() => {
                    if (this.editor && this.state.ref) {
                      this.editor.insertAtCurrentPosition(this.state.ref);
                      const query = this.editor.getValue();
                      this.setState(() => ({
                        query,
                        ref: undefined,
                      }));
                    }
                  }}
                >
                  <textarea data-cy="native-query-query" hidden readOnly value={this.state.query} />
                  <CodeEditor
                    height={60}
                    // @todo refactor this entire component and remove this check
                    value={typeof this.state.query === "string" ? this.state.query : ""}
                    codeEditorRef={editor => {
                      this.editor = editor;
                      this.syncEditor();
                    }}
                    language="sql"
                    onChange={newQuery => {
                      const encodedQuery = encodeForUrl(newQuery);
                      setQueryInUrl(queryDataKey)(encodedQuery);
                      this.setState(() => ({
                        query: newQuery,
                      }));
                    }}
                    onSubmit={() => {
                      this.fetchResults();
                    }}
                  />
                </CardSection>
                <FixedButton
                  color="primary"
                  icon="Play"
                  loading={this.state.isPending}
                  data-cy="native-query-editor-run-query-button"
                  onClick={() => {
                    this.fetchResults();
                  }}
                >
                  Run query
                </FixedButton>
              </>
            }
          />
        }
        region3={
          <Card title="Results">
            <Results response={this.state.response} />
          </Card>
        }
      />
    );
  }
}

export default NativeQuery;
