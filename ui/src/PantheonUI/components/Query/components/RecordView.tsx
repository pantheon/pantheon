import {
  Button,
  Card,
  CardSection,
  Code,
  Message,
  ModalConfirmContext,
  styled,
  Textarea,
  Tree,
  TreeProps,
} from "@operational/components";

import isEqual from "lodash/isEqual";
import React from "react";
import { connect } from "react-redux";
import uuidV1 from "uuid/v1";
import { JsonTextarea, NumericalInput } from "../..";
import { canQuery } from "../../../data/recordQuery";
import { Schema } from "../../../data/schema";
import { getPresent } from "../../../utils/undoable";
import QueryPlans from "../../QueryPlans";
import Results from "../../Results";
import * as actions from "../actions";
import { CompatibilityField } from "../compatibilityTypes";
import { RecordState, State as ReduxState } from "../state";
import ComplexLayout from "./ComplexLayout";
import SimpleLayout from "./SimpleLayout";

import NakedCard from "../../NakedCard";

import { decodeFromUrl, getQueryFromUrl, PslColor } from "../../../utils";

import { getConfig } from "../../Config";

import {
  CompatibilityError,
  LimitContainer,
  ResultsCard,
  ResultsCardContent,
  RunButtonContainer,
  StaleResultsWarning,
  StyledTextarea,
} from "./Miscellaneous";

export interface Props extends RecordState {
  catalogId: string;
  schemaId: string;
  schemas?: Schema[];
  confirm: ModalConfirmContext["confirm"];
  // Actions
  changeRecordFilter: typeof actions.changeRecordFilter;
  changeRecordLimit: typeof actions.changeRecordLimit;
  changeRecordRows: typeof actions.changeRecordRows;
  changeRecordOffset: typeof actions.changeRecordOffset;
  changeRecordOrder: typeof actions.changeRecordOrder;
  cancelRecordQuery: typeof actions.cancelRecordQuery;
  requestRecordQuery: typeof actions.requestRecordQuery;
  blurRecordFilterInput: typeof actions.blurRecordFilterInput;
  checkRecordCodeView: typeof actions.checkRecordCodeView;
  typeInRecordCodeView: typeof actions.typeInRecordCodeView;
}

export interface CollapsedSections {
  compatibleRows: boolean;
  filter: boolean;
  order: boolean;
  limit: boolean;
  offset: boolean;
}

export interface LocalState {
  isFieldDropping?: boolean;
  draggedField?: string;
  collapsedSections: CollapsedSections;
}

const FullHeightCardSection = styled(CardSection)`
  height: 100%;
  min-height: 180px;
`;

class RecordView extends React.Component<Props, LocalState> {
  public readonly state: LocalState = {
    collapsedSections: {
      compatibleRows: false,
      filter: false,
      order: true,
      limit: false,
      offset: true,
    },
  };

  private didQueryChangeAfterLastResponse(): boolean {
    if (!this.props.request) {
      return false;
    }
    return !isEqual(getPresent(this.props.query), this.props.request.query);
  }

  /**
   * Generates a set of props spread to different card sections
   */
  private toggleProps = (key: keyof CollapsedSections) => {
    return {
      collapsed: this.state.collapsedSections[key],
      onToggle: () => {
        this.changeCollapsedSections({ [key]: !this.state.collapsedSections[key] });
      },
    };
  };

  private changeCollapsedSections = (collapsedStateChanges: Partial<LocalState["collapsedSections"]>) => {
    this.setState((prevState: LocalState) => ({
      collapsedSections: {
        ...prevState.collapsedSections,
        ...collapsedStateChanges,
      },
    }));
  };

  private isUiDisabled() {
    const isQueryRunning = this.props.request && !this.props.request.response;
    return this.props.isCompatibilityResponsePending || isQueryRunning;
  }

  private renderCompatibleRows() {
    if (this.props.compatibility.type === "error") {
      return (
        <CardSection title="Rows">
          <CompatibilityError />
        </CardSection>
      );
    }
    const formatRef = (ref: string): string => {
      const chunks = ref.split(".");
      return chunks[chunks.length - 1];
    };
    const makeRowsTree = (dimensions: CompatibilityField[]): TreeProps["trees"] => {
      return dimensions.map((dimension, index) => {
        const disabled = getPresent(this.props.query).rows.includes(dimension.ref);
        return {
          key: index,
          label: formatRef(dimension.ref),
          color: PslColor.dimension,
          initiallyOpen: true,
          disabled,
          ...(dimension.children.length === 0
            ? {
                tag: "D",
                draggable: !disabled,
                onDragStart: () => {
                  this.setState(() => ({
                    draggedField: dimension.ref,
                  }));
                },
              }
            : {}),
          onDragEnd: () => {
            this.setState(() => ({ draggedField: undefined }));
          },

          childNodes: makeRowsTree(dimension.children),
        };
      });
    };
    return (
      <NakedCard
        sections={
          <CardSection title="Rows" disabled={this.isUiDisabled()} {...this.toggleProps("compatibleRows")}>
            <Tree trees={makeRowsTree(this.props.compatibility.value)} />
          </CardSection>
        }
      />
    );
  }

  private renderMainForm() {
    const presentQuery = getPresent(this.props.query);
    return (
      <Card
        sections={
          <>
            <CardSection title="Filter" disabled={this.isUiDisabled()} {...this.toggleProps("filter")}>
              <StyledTextarea
                value={presentQuery.filter || ""}
                onChange={this.props.changeRecordFilter}
                onBlur={() => {
                  this.props.blurRecordFilterInput();
                }}
              />
            </CardSection>
            <CardSection title="Order" disabled={this.isUiDisabled()} {...this.toggleProps("order")}>
              <JsonTextarea value={presentQuery.orderBy || ""} onChange={this.props.changeRecordOrder} />
            </CardSection>
            <CardSection title="Limit" disabled={this.isUiDisabled()} {...this.toggleProps("limit")}>
              {presentQuery.limit !== undefined && (
                <LimitContainer>
                  <NumericalInput value={presentQuery.limit} onChange={this.props.changeRecordLimit} />
                </LimitContainer>
              )}
            </CardSection>
            <CardSection title="Offset" disabled={this.isUiDisabled()} {...this.toggleProps("offset")}>
              <NumericalInput value={presentQuery.offset} onChange={this.props.changeRecordOffset} />
            </CardSection>
          </>
        }
      />
    );
  }

  private renderResults() {
    const presentQuery = getPresent(this.props.query);
    const { request } = this.props;
    return (
      <ResultsCard
        action={undefined}
        leftOfTabs={
          <RunButtonContainer>
            {request && !request.response ? (
              <>
                <Button
                  onClick={() => {
                    this.props.cancelRecordQuery();
                  }}
                  condensed
                  color="warning"
                >
                  Cancel query
                </Button>
              </>
            ) : (
              <>
                <Button
                  color="primary"
                  condensed
                  disabled={!canQuery(presentQuery)}
                  data-cy="record-query-editor-run-query-button"
                  onClick={() => {
                    this.props.requestRecordQuery(new Date().getTime(), uuidV1());
                  }}
                  icon="Play"
                >
                  Run query
                </Button>
              </>
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
                  response={this.props.request && this.props.request.response && this.props.request.response.payload}
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
    );
  }

  private renderRows() {
    const presentQuery = getPresent(this.props.query);
    return (
      <Card
        sections={
          <FullHeightCardSection
            title="Rows"
            dragAndDropFeedback={
              this.state.draggedField ? (this.state.isFieldDropping ? "dropping" : "validTarget") : undefined
            }
            onDragOver={ev => {
              ev.preventDefault();
              /**
               * This additional check is necessary to prevent blocking the render loop from the
               * multiple re-renders from dragover events.
               */
              if (this.state.isFieldDropping) {
                return;
              }
              this.setState(() => ({ isFieldDropping: true }));
            }}
            onDragLeave={() => {
              this.setState(() => ({ isFieldDropping: false }));
            }}
            onDrop={() => {
              this.setState(() => ({
                isFieldDropping: false,
                draggedField: undefined,
              }));
              if (!this.state.draggedField) {
                return;
              }
              this.props.changeRecordRows([...presentQuery.rows, this.state.draggedField]);
            }}
          >
            <Tree
              trees={presentQuery.rows.map(field => ({
                label: field,
                tag: "D",
                onRemove: () => {
                  this.props.changeRecordRows(presentQuery.rows.filter(currentField => field !== currentField));
                },
                childNodes: [],
              }))}
            />
          </FullHeightCardSection>
        }
      />
    );
  }

  private renderCodeView() {
    if (!this.props.codeView) {
      return null;
    }
    return (
      <Card title="Record query">
        <Textarea
          code
          value={this.props.codeView.queryDraft}
          onChange={this.props.typeInRecordCodeView}
          fullWidth
          error={
            this.props.codeView.decodeErrors
              ? this.props.codeView.decodeErrors
                  .map(err => `Error at \`query.${err.path.join(".")}\`: ${err.error}`)
                  .join(" | ")
              : undefined
          }
          height={150}
          onBlur={() => {
            this.props.checkRecordCodeView();
          }}
        />
      </Card>
    );
  }

  public render() {
    return this.props.codeView ? (
      <SimpleLayout
        region1={this.renderCompatibleRows()}
        region2={this.renderCodeView()}
        region3={this.renderResults()}
      />
    ) : (
      <ComplexLayout
        region1={this.renderCompatibleRows()}
        region2={this.renderMainForm()}
        region3={this.renderRows()}
        region4={this.renderResults()}
      />
    );
  }
}

const mapStateToProps = (state: ReduxState) => ({
  ...state.Record,
});

const mapDispatchToProps = {
  changeRecordFilter: actions.changeRecordFilter,
  changeRecordLimit: actions.changeRecordLimit,
  changeRecordRows: actions.changeRecordRows,
  changeRecordOffset: actions.changeRecordOffset,
  changeRecordOrder: actions.changeRecordOrder,
  cancelRecordQuery: actions.cancelRecordQuery,
  requestRecordQuery: actions.requestRecordQuery,
  blurRecordFilterInput: actions.blurRecordFilterInput,
  checkRecordCodeView: actions.checkRecordCodeView,
  typeInRecordCodeView: actions.typeInRecordCodeView,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(RecordView);
