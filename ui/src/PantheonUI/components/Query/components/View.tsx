import {
  Body,
  Button,
  Form,
  Input,
  ModalConfirmContext,
  OperationalContext,
  PageContent,
  styled,
  Textarea,
  Topbar,
  TopbarButton,
  TopbarSelect,
} from "@operational/components";

import React from "react";
import { connect } from "react-redux";

import { OperationalRouter } from "@operational/router";
import { Schema } from "../../../data/schema";
import { decodeFromUrl, getQueryFromUrl } from "../../../utils";
import * as actions from "../actions";
import { State as ReduxState } from "../state";

import { CreateEndpoint, Endpoint, EndpointData, makeNewEndpoint, UpdateEndpoint } from "../../../data/endpoint";

const AggregateTopBar = React.lazy(() => import(/* webpackChunkName: "aggregate-top-bar" */ "./AggregateTopBar"));
const AggregateView = React.lazy(() => import(/* webpackChunkName: "aggregate-view" */ "./AggregateView"));
const QueryHistory = React.lazy(() => import(/* webpackChunkName: "query-history" */ "./QueryHistory"));
const RecordTopBar = React.lazy(() => import(/* webpackChunkName: "record-top-bar" */ "./RecordTopBar"));
const RecordView = React.lazy(() => import(/* webpackChunkName: "record-view" */ "./RecordView"));
const SqlTopBar = React.lazy(() => import(/* webpackChunkName: "sql-top-bar" */ "./SqlTopBar"));
const SqlView = React.lazy(() => import(/* webpackChunkName: "sql-view" */ "./SqlView"));

export interface Props extends ReduxState {
  catalogId: string;
  schemaId: string;
  schemas?: Schema[];
  endpoint?: Endpoint;
  customReference: string;
  // Actions
  changeQueryType: typeof actions.changeQueryType;
  restoreAggregateQuery: typeof actions.restoreAggregateQuery;
  restoreRecordQuery: typeof actions.restoreRecordQuery;
  restoreSqlQuery: typeof actions.restoreSqlQuery;
}

const topBarHeight = 40;

const Container = styled("div")`
  height: 100%;
  display: grid;
  grid-template-rows: ${topBarHeight}px auto;
  background-color: ${({ theme }) => theme.color.background.lighter};
`;

const CreateEndpointModalActions = styled("div")({ display: "flex", alignItems: "center", justifyContent: "flex-end" });

class View extends React.Component<Props, { endpointName: string; endpointDescription: string }> {
  public state = {
    endpointName: "",
    endpointDescription: "",
  };

  private renderTopBar() {
    switch (this.props.queryType) {
      case "Aggregate":
        return <AggregateTopBar />;
      case "Record":
        return <RecordTopBar />;
      case "Sql":
        return <SqlTopBar />;
    }
  }

  private renderMainView(confirm: ModalConfirmContext["confirm"]) {
    const props = {
      confirm,
      catalogId: this.props.catalogId,
      schemaId: this.props.schemaId,
      schemas: this.props.schemas,
    };
    switch (this.props.queryType) {
      case "Aggregate":
        return <AggregateView {...props} />;
      case "Record":
        return <RecordView {...props} />;
      case "Sql":
        return <SqlView {...props} />;
    }
  }

  private getEndpointDataFromUrl = (): EndpointData | undefined => {
    const queryFromUrl = getQueryFromUrl("querydata");
    if (!queryFromUrl) {
      return;
    }

    return makeNewEndpoint({
      name: this.props.endpoint ? this.props.endpoint.name : this.state.endpointName,
      description: this.props.endpoint ? this.props.endpoint.description : this.state.endpointDescription,
      catalogId: this.props.catalogId,
      schemaId: this.props.schemaId,
      query: decodeFromUrl(queryFromUrl),
    });
  };

  public render() {
    const selectedSchema = (this.props.schemas || []).filter(schema => schema.id === this.props.schemaId)[0];
    return (
      <OperationalRouter>
        {({ pushState }) => (
          <PageContent fill noPadding {...{ style: { minWidth: 1000 } }}>
            {({ modal, confirm }) => (
              <OperationalContext>
                {({ pushMessage }) => {
                  const onCreateEndpoint = () => {
                    pushMessage({
                      body: "Endpoint Created. Click to see Endpoints.",
                      type: "success",
                      onClick: () => pushState("endpoints"),
                    });
                    close();
                  };
                  return (
                    <Container>
                      <Topbar
                        left={
                          <>
                            <TopbarSelect
                              label="Schema:"
                              selected={selectedSchema && selectedSchema.name}
                              items={(this.props.schemas || []).map(schema => ({
                                label: schema.name,
                                onClick: () => {
                                  const newSchema = (this.props.schemas || []).filter(
                                    schema1 => schema1.name === schema.name,
                                  )[0];
                                  if (!newSchema) {
                                    return;
                                  }
                                  pushState(`schemas/${newSchema.id}/query`);
                                },
                              }))}
                            />
                            {
                              <TopbarSelect
                                label="Query type:"
                                selected={this.props.queryType}
                                items={[
                                  {
                                    label: "Aggregate",
                                    onClick: () => {
                                      this.props.changeQueryType("Aggregate");
                                    },
                                  },
                                  {
                                    label: "Record",
                                    onClick: () => {
                                      this.props.changeQueryType("Record");
                                    },
                                  },
                                  {
                                    label: "Sql",
                                    onClick: () => {
                                      this.props.changeQueryType("Sql");
                                    },
                                  },
                                ]}
                              />
                            }
                            {this.renderTopBar()}
                          </>
                        }
                        right={
                          this.props.endpoint ? (
                            <>
                              <Button
                                condensed
                                onClick={() =>
                                  pushState(`endpoints/${this.props.endpoint ? this.props.endpoint.id : ""}`)
                                }
                              >
                                Cancel
                              </Button>
                              <UpdateEndpoint catalogId={this.props.catalogId} endpointId={this.props.endpoint.id}>
                                {(saveEndpoint, { loading }) => (
                                  <Button
                                    condensed
                                    color="primary"
                                    onClick={() =>
                                      confirm({
                                        title: "Are you sure you want to update this endpoint’s query?",
                                        body:
                                          "This might change the parameters and / or response the consumers receive from this endpoint.",
                                        onConfirm: () =>
                                          saveEndpoint(this.getEndpointDataFromUrl()).then(endpointResponse => {
                                            pushMessage({
                                              body: "Saved. Click to go back to Endpoint.",
                                              type: "success",
                                              onClick: () => pushState(`endpoints/${endpointResponse.id}`),
                                            });
                                          }),
                                      })
                                    }
                                  >
                                    {loading ? "Saving..." : "Save"}
                                  </Button>
                                )}
                              </UpdateEndpoint>
                            </>
                          ) : (
                            <>
                              <TopbarButton
                                onClick={() => {
                                  modal({
                                    title: "Query history",
                                    body: close => (
                                      <QueryHistory
                                        close={close}
                                        customReference={this.props.customReference}
                                        catalogId={this.props.catalogId}
                                        restoreSqlQuery={this.props.restoreSqlQuery}
                                        restoreRecordQuery={this.props.restoreRecordQuery}
                                        restoreAggregateQuery={this.props.restoreAggregateQuery}
                                      />
                                    ),
                                  });
                                }}
                              >
                                Query history
                              </TopbarButton>
                              <CreateEndpoint catalogId={this.props.catalogId}>
                                {(create, { loading }) => (
                                  <TopbarButton
                                    onClick={() => {
                                      modal({
                                        title: "Create Endpoint",
                                        body: close => (
                                          <>
                                            <Body>Please name your endpoint</Body>
                                            <Form
                                              onSubmit={e => {
                                                e.preventDefault();
                                                create(this.getEndpointDataFromUrl()).then(onCreateEndpoint);
                                              }}
                                            >
                                              <Input
                                                data-cy="pantheon--new-endpoint-modal__name-field"
                                                onChange={endpointName => this.setState({ endpointName })}
                                                value={this.state.endpointName}
                                                placeholder="My Endpoint"
                                                label="Name"
                                              />
                                              <Textarea
                                                data-cy="pantheon--new-endpoint-modal__description-field"
                                                onChange={endpointDescription => this.setState({ endpointDescription })}
                                                value={this.state.endpointDescription}
                                                label="Description"
                                              />
                                            </Form>
                                            <br />
                                            <CreateEndpointModalActions>
                                              <Button
                                                data-cy="pantheon--new-endpoint-modal__cancel-button"
                                                onClick={() => close()}
                                              >
                                                Cancel
                                              </Button>
                                              <Button
                                                color="primary"
                                                data-cy="pantheon--new-endpoint-modal__save-button"
                                                disabled={!this.state.endpointName.length}
                                                loading={loading}
                                                onClick={() =>
                                                  create(this.getEndpointDataFromUrl()).then(onCreateEndpoint)
                                                }
                                              >
                                                Save
                                              </Button>
                                            </CreateEndpointModalActions>
                                          </>
                                        ),
                                      });
                                    }}
                                  >
                                    <Button
                                      data-cy="pantheon--aggreate-view__create-endpoint-button"
                                      textColor="primary"
                                      condensed
                                      icon="ChevronRight"
                                      iconSize={12}
                                    >
                                      Create Endpoint
                                    </Button>
                                  </TopbarButton>
                                )}
                              </CreateEndpoint>
                            </>
                          )
                        }
                      />
                      {this.renderMainView(confirm)}
                    </Container>
                  );
                }}
              </OperationalContext>
            )}
          </PageContent>
        )}
      </OperationalRouter>
    );
  }
}

const mapStateToProps = (state: ReduxState) => state;

const mapDispatchToProps = {
  changeQueryType: actions.changeQueryType,
  restoreAggregateQuery: actions.restoreAggregateQuery,
  restoreRecordQuery: actions.restoreRecordQuery,
  restoreSqlQuery: actions.restoreSqlQuery,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(View);
