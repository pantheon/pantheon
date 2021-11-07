import { OperationalContext, styled } from "@operational/components";
import { OperationalRouter, OperationalRouterProvider } from "@operational/router";
import Cookies from "js-cookie";
import get from "lodash/get";
import React, { Suspense } from "react";
import { Redirect, RouterProps, Switch } from "react-router";
import { Route } from "react-router-dom";
import { RestfulProvider } from "restful-react";
import { RestfulReactProviderProps } from "restful-react/lib/Context";

import { getConfig } from "./components/Config";
import { getQueryField } from "./utils";

const DataSource = React.lazy(() => import(/* webpackChunkName: "page-datasource" */ "./pages/DataSource"));
const Endpoint = React.lazy(() => import(/* webpackChunkName: "page-endpoint" */ "./pages/Endpoint"));
const Endpoints = React.lazy(() => import(/* webpackChunkName: "page-endpoints" */ "./pages/Endpoints"));
const NewSchema = React.lazy(() => import(/* webpackChunkName: "page-newschema" */ "./pages/NewSchema"));
const QueryLog = React.lazy(() => import(/* webpackChunkName: "page-querylog" */ "./pages/QueryLog"));
const QueryLogs = React.lazy(() => import(/* webpackChunkName: "page-query-logs" */ "./pages/QueryLogs"));
const QuerySchema = React.lazy(() => import(/* webpackChunkName: "page-queryschema" */ "./pages/QuerySchema"));
const Schema = React.lazy(() => import(/* webpackChunkName: "page-schema" */ "./pages/Schema"));
const Schemas = React.lazy(() => import(/* webpackChunkName: "page-schemas" */ "./pages/Schemas"));
const DataSources = React.lazy(() => import(/* webpackChunkName: "page-data-sources" */ "./pages/DataSources"));
const EditEndpoint = React.lazy(() => import(/* webpackChunkName: "page-edit-endpoint" */ "./pages/EditEndpoint"));

export interface PantheonUIProps {
  locale: "en";
  basePath: string;
  pushState: RouterProps["history"]["push"];
  replaceState: RouterProps["history"]["replace"];
  onRequestError?: RestfulReactProviderProps["onError"];
}

const Container = styled("div")({
  label: "pantheonui",
  height: "100%",
});

const getPage = (search: string): number | undefined => {
  const pageFromQuery = getQueryField("page")(search);
  if (!pageFromQuery) {
    return;
  }
  const pageNumber = Number(pageFromQuery);
  if (isNaN(pageNumber)) {
    return;
  }
  return pageNumber;
};

const PantheonUI: React.SFC<PantheonUIProps> = ({ basePath, onRequestError, pushState, replaceState }) => {
  if (!basePath.includes(":projectId")) {
    throw new Error("Please include a :projectId parameter in the `basePath` prop passed to PantheonUI");
  }
  return (
    <OperationalContext>
      {({ pushMessage }) => (
        <RestfulProvider
          base={getConfig("backend")}
          requestOptions={{
            credentials: "include",
            headers: {
              "x-double-cookie": Cookies.get("double-cookie") || "",
            },
          }}
          onError={(err, _, res) => {
            pushMessage({
              type: "error",
              body: get(err, "data.errors.0", err.message),
              onClick: () => {
                // Copy the error detail in your clipboard
                const el = document.createElement("textarea");
                el.value = JSON.stringify({ url: res.url, status: res.status, body: err.data }, null, 2);
                document.body.appendChild(el);
                el.select();
                document.execCommand("copy");
                document.body.removeChild(el);
                pushMessage({
                  type: "info",
                  body: "Error detail has been copied in your clipboard!",
                });
              },
            });
            if (onRequestError) {
              onRequestError(err, _, res);
            }
          }}
        >
          <Container>
            <Suspense fallback="">
              <OperationalRouterProvider basePath={basePath} pushState={pushState} replaceState={replaceState}>
                {() => (
                  <Switch>
                    <Route
                      exact
                      path={`${basePath}/data-sources`}
                      render={routerProps => {
                        return (
                          <DataSources
                            catalogId={routerProps.match.params.projectId}
                            page={getPage(routerProps.location.search) || 1}
                          />
                        );
                      }}
                    />
                    <Route
                      exact
                      path={`${basePath}/data-sources/new/:product?`}
                      render={routerProps => {
                        return (
                          <DataSources
                            catalogId={routerProps.match.params.projectId}
                            page={getPage(routerProps.location.search) || 1}
                          />
                        );
                      }}
                    />
                    <Route
                      exact
                      path={`${basePath}/data-sources/:id/:tab?`}
                      render={routerProps => (
                        <DataSource
                          catalogId={routerProps.match.params.projectId}
                          dataSourceId={routerProps.match.params.id}
                          tab={routerProps.match.params.tab}
                        />
                      )}
                    />
                    {/* Schemas */}
                    <Route
                      exact
                      path={`${basePath}/schemas`}
                      render={routerProps => (
                        <Schemas
                          catalogId={routerProps.match.params.projectId}
                          page={getPage(routerProps.location.search) || 1}
                        />
                      )}
                    />
                    <Route
                      exact
                      path={`${basePath}/schemas/new`}
                      render={routerProps => <NewSchema catalogId={routerProps.match.params.projectId} />}
                    />
                    <Route
                      exact
                      path={`${basePath}/schemas/:id/query`}
                      render={routerProps => {
                        const schemaId = routerProps.match.params.id;
                        return <QuerySchema catalogId={routerProps.match.params.projectId} schemaId={schemaId} />;
                      }}
                    />
                    <Route
                      exact
                      path={`${basePath}/schemas/:id/:tabName?`}
                      render={routerProps => {
                        const schemaId = routerProps.match.params.id;
                        const tabName = routerProps.match.params.tabName;
                        return (
                          <Schema
                            catalogId={routerProps.match.params.projectId}
                            schemaId={schemaId}
                            tabName={tabName}
                          />
                        );
                      }}
                    />
                    {/* Endpoints */}
                    <Route
                      exact
                      path={`${basePath}/endpoints`}
                      render={routerProps => (
                        <Endpoints
                          catalogId={routerProps.match.params.projectId}
                          page={getPage(routerProps.location.search) || 1}
                        />
                      )}
                    />
                    <Route
                      exact
                      path={`${basePath}/endpoints/:id/edit`}
                      render={routerProps => {
                        const endpointId = routerProps.match.params.id;
                        return <EditEndpoint catalogId={routerProps.match.params.projectId} endpointId={endpointId} />;
                      }}
                    />
                    <Route
                      exact
                      path={`${basePath}/endpoints/:id/:tabName?`}
                      render={routerProps => {
                        const endpointId = routerProps.match.params.id;
                        const tabName = routerProps.match.params.tabName;
                        return (
                          <Endpoint
                            tabName={tabName}
                            catalogId={routerProps.match.params.projectId}
                            endpointId={endpointId}
                          />
                        );
                      }}
                    />
                    {/* Executed Queries */}
                    <Route
                      exact
                      path={`${basePath}/executed-queries`}
                      render={routerProps => (
                        <QueryLogs
                          catalogId={routerProps.match.params.projectId}
                          page={getPage(routerProps.location.search) || 1}
                        />
                      )}
                    />
                    <Route
                      exact
                      path={`${basePath}/executed-queries/:id`}
                      render={routerProps => {
                        const queryId = routerProps.match.params.id;
                        return <QueryLog catalogId={routerProps.match.params.projectId} queryId={queryId} />;
                      }}
                    />
                    <Route
                      exact
                      render={() => (
                        <OperationalRouter>
                          {({ basePath: routerBasePath }) => <Redirect to={`${routerBasePath}/data-sources`} />}
                        </OperationalRouter>
                      )}
                    />
                  </Switch>
                )}
              </OperationalRouterProvider>
            </Suspense>
          </Container>
        </RestfulProvider>
      )}
    </OperationalContext>
  );
};

export default PantheonUI;
