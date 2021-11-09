import {
  Button,
  Card,
  CardSection,
  Code,
  DragAndDropFeedback,
  Message,
  ModalConfirmContext,
  Textarea,
  Tree,
  TreeProps,
} from "@operational/components";

import isEqual from "lodash/isEqual";
import React from "react";
import { DragDropContext } from "react-beautiful-dnd";
import { connect } from "react-redux";
import uuidV1 from "uuid/v1";

import { JsonTextarea, NumericalInput } from "../..";
import { canQuery, orderByParamDecoder, orderByParamSample } from "../../../data/aggregateQuery";
import { Schema } from "../../../data/schema";
import { getPresent } from "../../../utils/undoable";
import QueryPlans from "../../QueryPlans";
import Results from "../../Results";
import * as actions from "../actions";
import { CompatibilityField } from "../compatibilityTypes";
import { AggregateState, State as ReduxState } from "../state";
import ComplexLayout from "./ComplexLayout";
import SimpleLayout from "./SimpleLayout";

import { decodeFromUrl, getQueryFromUrl, PslColor } from "../../../utils";
import NakedCard from "../../NakedCard";

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

export interface Props extends AggregateState {
  catalogId: string;
  schemaId: string;
  schemas?: Schema[];
  confirm: ModalConfirmContext["confirm"];
  // Actions
  changeAggregateColumns: typeof actions.changeAggregateColumns;
  changeAggregateColumnsTopN: typeof actions.changeAggregateColumnsTopN;
  changeAggregateFilter: typeof actions.changeAggregateFilter;
  changeAggregatePostAggregateFilter: typeof actions.changeAggregatePostAggregateFilter;
  changeAggregateLimit: typeof actions.changeAggregateLimit;
  changeAggregateMeasures: typeof actions.changeAggregateMeasures;
  changeAggregateOffset: typeof actions.changeAggregateOffset;
  changeAggregateOrder: typeof actions.changeAggregateOrder;
  changeAggregateRows: typeof actions.changeAggregateRows;
  changeAggregateRowsTopN: typeof actions.changeAggregateRowsTopN;
  undoAggregateQueryChange: typeof actions.undoAggregateQueryChange;
  redoAggregateQueryChange: typeof actions.redoAggregateQueryChange;
  cancelAggregateQuery: typeof actions.cancelAggregateQuery;
  requestAggregateQuery: typeof actions.requestAggregateQuery;
  blurAggregateFilterInput: typeof actions.blurAggregateFilterInput;
  blurAggregatePostAggregateFilterInput: typeof actions.blurAggregatePostAggregateFilterInput;
  checkAggregateCodeView: typeof actions.checkAggregateCodeView;
  typeInAggregateCodeView: typeof actions.typeInAggregateCodeView;
}

export interface CollapsedSections {
  compatibleMeasures: boolean;
  compatibleDimensions: boolean;
  columns: boolean;
  dimensions: boolean;
  filter: boolean;
  limit: boolean;
  measures: boolean;
  offset: boolean;
  order: boolean;
  postAggregateFilter: boolean;
  rows: boolean;
  topNRows: boolean;
}

export interface LocalState {
  dropTarget?: DropTarget;
  dragSource?: DragSource;
  collapsedSections: CollapsedSections;
}

export interface DragSource {
  type: "measure" | "dimension";
  value: string;
}

export type DropTarget = "measures" | "columns" | "rows";

const reorder = (result: string[], startIndex: number, endIndex: number) => {
  const [removed] = result.splice(startIndex, 1);
  result.splice(endIndex, 0, removed);
  return result;
};

const getFieldErrors = (props: Pick<AggregateState, "compatibility">) => {
  const error = props.compatibility;
  return Object.entries(
    (error && error.type === "error" && error.value.fieldErrors) || ({} as { [key: string]: string[] }),
  ).reduce((prev, [key, errors]) => {
    // for some reason compatibility request prefixes fields with query
    return { ...prev, [key.replace(/^query\./, "")]: errors.join(", ") };
  }, {}) as {
    [key: string]: string;
  };
};

class AggregateView extends React.Component<Props, LocalState> {
  public state: LocalState = {
    collapsedSections: {
      compatibleMeasures: false,
      compatibleDimensions: false,
      columns: false,
      dimensions: false,
      filter: true,
      limit: false,
      measures: false,
      offset: true,
      order: true,
      postAggregateFilter: true,
      rows: false,
      topNRows: true,
    },
  };

  public enableLimit() {
    this.props.changeAggregateLimit(1000);
  }

  public disableLimit() {
    this.props.changeAggregateLimit(undefined);
  }

  private canAddCurrentDragItemToMeasures(): boolean {
    const { dragSource } = this.state;
    if (!dragSource) {
      return false;
    }
    return dragSource.type === "measure" && !getPresent(this.props.query).measures.includes(dragSource.value);
  }

  private canAddCurrentDragItemToRowsOrColumns(): boolean {
    const { dragSource } = this.state;
    if (!dragSource) {
      return false;
    }
    return (
      dragSource.type === "dimension" &&
      !getPresent(this.props.query).rows.includes(dragSource.value) &&
      !getPresent(this.props.query).columns.includes(dragSource.value)
    );
  }

  /**
   * Generates a set of props spread to different card sections
   */
  private toggleProps = (key: keyof CollapsedSections) => {
    const fieldErrors = getFieldErrors(this.props);
    return {
      collapsed: this.state.collapsedSections[key] && !fieldErrors[key],
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

  private handleDrop = () => {
    const presentQuery = getPresent(this.props.query);
    const { dragSource } = this.state;
    this.setState(() => ({ dragSource: undefined, dropTarget: undefined }));
    if (!dragSource) {
      return;
    }
    if (this.canAddCurrentDragItemToRowsOrColumns() && this.state.dropTarget === "columns") {
      this.props.changeAggregateColumns([...presentQuery.columns, dragSource.value]);
    }
    if (this.canAddCurrentDragItemToMeasures() && this.state.dropTarget === "measures") {
      // Expand section that was dropped on
      this.changeCollapsedSections({
        measures: false,
      });
      this.props.changeAggregateMeasures([...presentQuery.measures, dragSource.value]);
    }
    if (this.canAddCurrentDragItemToRowsOrColumns() && this.state.dropTarget === "rows") {
      // Expand section that was dropped on
      this.changeCollapsedSections({
        rows: false,
      });
      this.props.changeAggregateRows([...presentQuery.rows, dragSource.value]);
    }
  };

  private getDragAndDropFeedbacks = (): {
    measures: DragAndDropFeedback | undefined;
    columns: DragAndDropFeedback | undefined;
    rows: DragAndDropFeedback | undefined;
  } => {
    if (!this.state.dragSource) {
      return {
        measures: undefined,
        columns: undefined,
        rows: undefined,
      };
    }
    return {
      measures: (() => {
        if (this.canAddCurrentDragItemToMeasures()) {
          return this.state.dropTarget === "measures" ? "dropping" : "validTarget";
        }
        return undefined;
      })(),
      columns: (() => {
        if (this.canAddCurrentDragItemToRowsOrColumns()) {
          return this.state.dropTarget === "columns" ? "dropping" : "validTarget";
        }
        return undefined;
      })(),
      rows: (() => {
        if (this.canAddCurrentDragItemToRowsOrColumns()) {
          return this.state.dropTarget === "rows" ? "dropping" : "validTarget";
        }
        return undefined;
      })(),
    };
  };

  private didQueryChangeAfterLastResponse(): boolean {
    if (!this.props.request) {
      return false;
    }
    return !isEqual(getPresent(this.props.query), this.props.request.query);
  }

  private isEnabled = (field: CompatibilityField): boolean => !getPresent(this.props.query).columns.includes(field.ref);

  public renderMeasuresTree(measures: CompatibilityField[]) {
    const makeMeasuresTree = (enabledMeasures: CompatibilityField[]): TreeProps["trees"] => {
      return enabledMeasures.map((measure, index) => ({
        key: index,
        label: measure.ref,
        color: PslColor.measure,
        initiallyOpen: true,
        ...(measure.children.length === 0
          ? {
              tag: "M",
              draggable: true,
              onDragStart: () => {
                this.setState(() => ({
                  dragSource: {
                    type: "measure",
                    value: measure.ref,
                  },
                }));
              },
              onDragEnd: () => {
                this.setState(() => ({ dragSource: undefined }));
              },
            }
          : {}),
        childNodes: makeMeasuresTree(measure.children),
      }));
    };
    return (
      <CardSection
        title="Available Measures"
        {...this.toggleProps("compatibleMeasures")}
        disabled={this.props.isCompatibilityResponsePending}
      >
        <Tree trees={makeMeasuresTree(measures.filter(this.isEnabled))} />
      </CardSection>
    );
  }

  public renderDimensionsTree(dimensions: CompatibilityField[]) {
    const formatRef = (ref: string): string => {
      const chunks = ref.split(".");
      return chunks[chunks.length - 1];
    };
    const makeDimensionsTree = (enabledDimensions: CompatibilityField[]): TreeProps["trees"] => {
      return enabledDimensions.map((dimension, index) => ({
        key: index,
        label: formatRef(dimension.ref),
        color: PslColor.dimension,
        initiallyOpen: true,
        ...(dimension.children.length === 0
          ? {
              tag: "D",
              draggable: true,
              onDragStart: () => {
                this.setState({
                  dragSource: {
                    type: "dimension",
                    value: dimension.ref,
                  },
                });
              },
            }
          : {}),
        onDragEnd: () => {
          this.setState(() => ({ dragSource: undefined }));
        },

        childNodes: makeDimensionsTree(dimension.children),
      }));
    };
    return (
      <CardSection
        title="Available Dimensions"
        {...this.toggleProps("compatibleDimensions")}
        disabled={this.props.isCompatibilityResponsePending}
      >
        <Tree trees={makeDimensionsTree(dimensions.filter(this.isEnabled))} />
      </CardSection>
    );
  }

  private isUiDisabled() {
    const isQueryRunning = this.props.request && !this.props.request.response;
    return this.props.isCompatibilityResponsePending || isQueryRunning;
  }

  private renderMeasuresAndDimensions() {
    return (
      <NakedCard
        sections={
          this.props.compatibility.type === "error" ? (
            <CardSection title="Available Data">
              <CompatibilityError error={this.props.compatibility.value} />
            </CardSection>
          ) : (
            <>
              {this.renderMeasuresTree(this.props.compatibility.value.measures)}
              {this.renderDimensionsTree(this.props.compatibility.value.dimensions)}
            </>
          )
        }
      />
    );
  }

  private renderMainForm() {
    const presentQuery = getPresent(this.props.query);
    const fieldErrors = getFieldErrors(this.props);
    const dragAndDropFeedbacks = this.getDragAndDropFeedbacks();
    return (
      <Card
        sections={
          <>
            <CardSection
              data-cy="pantheon--aggreate-view__measures-drop-area"
              title="Measures"
              {...this.toggleProps("measures")}
              disabled={this.isUiDisabled()}
              dragAndDropFeedback={dragAndDropFeedbacks.measures}
              onDragOver={ev => {
                ev.preventDefault();
                /**
                 * This additional check is necessary to prevent blocking the render loop from the
                 * multiple re-renders from dragover events.
                 */
                if (this.state.dropTarget === "measures") {
                  return;
                }
                this.setState(() => ({ dropTarget: "measures" }));
              }}
              onDragLeave={() => {
                this.setState(() => ({ dropTarget: undefined }));
              }}
              onDrop={this.handleDrop}
            >
              <DragDropContext
                onDragEnd={result => {
                  if (result.destination) {
                    this.props.changeAggregateMeasures(
                      reorder(presentQuery.measures, result.source.index, result.destination.index),
                    );
                  }
                }}
              >
                <Tree
                  droppableProps={{ droppableId: "measures" }}
                  trees={presentQuery.measures.map(measure => ({
                    label: measure,
                    color: PslColor.measure,
                    tag: "M",
                    onRemove: () => {
                      this.props.changeAggregateMeasures(
                        presentQuery.measures.filter(selectedMeasure => measure !== selectedMeasure),
                      );
                    },
                    childNodes: [],
                  }))}
                />
              </DragDropContext>
            </CardSection>
            <CardSection
              title="Rows"
              data-cy="pantheon--aggreate-view__rows-drop-area"
              disabled={this.isUiDisabled()}
              {...this.toggleProps("rows")}
              dragAndDropFeedback={dragAndDropFeedbacks.rows}
              onDragOver={ev => {
                ev.preventDefault();
                /**
                 * This additional check is necessary to prevent blocking the render loop from the
                 * multiple re-renders from dragover events.
                 */
                if (this.state.dropTarget === "rows") {
                  return;
                }
                this.setState(() => ({ dropTarget: "rows" }));
              }}
              onDragLeave={() => {
                this.setState(() => ({ dropTarget: undefined }));
              }}
              onDrop={this.handleDrop}
            >
              <DragDropContext
                onDragEnd={result => {
                  if (result.destination) {
                    this.props.changeAggregateRows(
                      reorder(presentQuery.rows, result.source.index, result.destination.index),
                    );
                  }
                }}
              >
                <Tree
                  droppableProps={{ droppableId: "dimensions" }}
                  trees={presentQuery.rows.map(row => ({
                    label: row,
                    color: PslColor.dimension,
                    tag: "D",
                    childNodes: [],
                    onRemove: () => {
                      this.props.changeAggregateRows(presentQuery.rows.filter(selectedRow => row !== selectedRow));
                    },
                  }))}
                />
              </DragDropContext>
            </CardSection>
            <CardSection title="Filter" disabled={this.isUiDisabled()} {...this.toggleProps("filter")}>
              <StyledTextarea
                value={presentQuery.filter || ""}
                onChange={this.props.changeAggregateFilter}
                onBlur={this.props.blurAggregateFilterInput}
                error={fieldErrors.filter}
              />
            </CardSection>
            <CardSection
              title="Post aggregate filter"
              disabled={this.isUiDisabled()}
              {...this.toggleProps("postAggregateFilter")}
            >
              <StyledTextarea
                value={presentQuery.aggregateFilter || ""}
                onChange={this.props.changeAggregatePostAggregateFilter}
                onBlur={this.props.blurAggregatePostAggregateFilterInput}
                error={fieldErrors.aggregateFilter}
              />
            </CardSection>
            <CardSection title="Order" disabled={this.isUiDisabled()} {...this.toggleProps("order")}>
              <JsonTextarea
                value={presentQuery.orderBy}
                onChange={this.props.changeAggregateOrder}
                validate={orderByParamDecoder}
                placeholder={JSON.stringify(orderByParamSample, null, 2)}
              />
            </CardSection>
            <CardSection title="Limit" disabled={this.isUiDisabled()} {...this.toggleProps("limit")}>
              {presentQuery.limit !== undefined && (
                <LimitContainer>
                  <NumericalInput value={presentQuery.limit} onChange={this.props.changeAggregateLimit} />
                </LimitContainer>
              )}
            </CardSection>
            <CardSection title="Offset" {...this.toggleProps("offset")} disabled={this.isUiDisabled()}>
              <NumericalInput value={presentQuery.offset} onChange={this.props.changeAggregateOffset} />
            </CardSection>
          </>
        }
      />
    );
  }

  public renderColumnsAndTopNColumns() {
    const presentQuery = getPresent(this.props.query);
    const dragAndDropFeedbacks = this.getDragAndDropFeedbacks();
    return (
      <Card
        sections={
          <>
            <CardSection
              title="Columns"
              dragAndDropFeedback={dragAndDropFeedbacks.columns}
              disabled={this.isUiDisabled()}
              onDragOver={ev => {
                ev.preventDefault();
                /**
                 * This additional check is necessary to prevent blocking the render loop from the
                 * multiple re-renders from dragover events.
                 */
                if (this.state.dropTarget === "columns") {
                  return;
                }
                this.setState(() => ({ dropTarget: "columns" }));
              }}
              onDragLeave={() => {
                this.setState(() => ({ dropTarget: undefined }));
              }}
              onDrop={this.handleDrop}
            >
              <DragDropContext
                onDragEnd={result => {
                  if (result.destination) {
                    this.props.changeAggregateColumns(
                      reorder(presentQuery.columns, result.source.index, result.destination.index),
                    );
                  }
                }}
              >
                <Tree
                  droppableProps={{ droppableId: "columns" }}
                  trees={presentQuery.columns.map(column => ({
                    label: column,
                    color: PslColor.dimension,
                    tag: "D",
                    childNodes: [],
                    onRemove: () => {
                      this.props.changeAggregateColumns(
                        presentQuery.columns.filter(selectedColumn => column !== selectedColumn),
                      );
                    },
                  }))}
                />
              </DragDropContext>
            </CardSection>
          </>
        }
      />
    );
  }

  private renderCodeView() {
    if (!this.props.codeView) {
      return null;
    }
    return (
      <Card title="Aggregate query">
        <Textarea
          code
          value={this.props.codeView.queryDraft}
          onChange={this.props.typeInAggregateCodeView}
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
            this.props.checkAggregateCodeView();
          }}
        />
      </Card>
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
              <Button
                onClick={() => {
                  this.props.cancelAggregateQuery();
                }}
                condensed
                color="warning"
              >
                Cancel query
              </Button>
            ) : (
              <Button
                color="primary"
                data-cy="pantheon--aggreate-view__run-query-button"
                condensed
                disabled={!canQuery(presentQuery)}
                onClick={() => {
                  this.props.requestAggregateQuery(new Date().getTime(), uuidV1());
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
                  data-cy="pantheon--aggreate-view__results"
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

  public render() {
    return this.props.codeView ? (
      <SimpleLayout
        region1={this.renderMeasuresAndDimensions()}
        region2={this.renderCodeView()}
        region3={this.renderResults()}
      />
    ) : (
      <ComplexLayout
        region1={this.renderMeasuresAndDimensions()}
        region2={this.renderMainForm()}
        region3={this.renderColumnsAndTopNColumns()}
        region4={this.renderResults()}
      />
    );
  }
}

const mapStateToProps = (state: ReduxState) => ({
  ...state.Aggregate,
});

const mapDispatchToProps = {
  changeAggregateColumns: actions.changeAggregateColumns,
  changeAggregateColumnsTopN: actions.changeAggregateColumnsTopN,
  changeAggregateFilter: actions.changeAggregateFilter,
  changeAggregatePostAggregateFilter: actions.changeAggregatePostAggregateFilter,
  changeAggregateLimit: actions.changeAggregateLimit,
  changeAggregateMeasures: actions.changeAggregateMeasures,
  changeAggregateOffset: actions.changeAggregateOffset,
  changeAggregateOrder: actions.changeAggregateOrder,
  changeAggregateRows: actions.changeAggregateRows,
  changeAggregateRowsTopN: actions.changeAggregateRowsTopN,
  undoAggregateQueryChange: actions.undoAggregateQueryChange,
  redoAggregateQueryChange: actions.redoAggregateQueryChange,
  cancelAggregateQuery: actions.cancelAggregateQuery,
  requestAggregateQuery: actions.requestAggregateQuery,
  blurAggregateFilterInput: actions.blurAggregateFilterInput,
  blurAggregatePostAggregateFilterInput: actions.blurAggregatePostAggregateFilterInput,
  checkAggregateCodeView: actions.checkAggregateCodeView,
  typeInAggregateCodeView: actions.typeInAggregateCodeView,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(AggregateView);
