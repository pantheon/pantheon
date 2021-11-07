/**
 * This is a sample app using Pantheon UI. See src/PantheonUI/index.tsx for the package's entry point.
 */
import {
  Debug,
  HeaderBar,
  HeaderMenu,
  Icon,
  IconName,
  Layout,
  Logo,
  OperationalUI,
  Sidenav,
} from "@operational/components";

import * as Sentry from "@sentry/browser";
import React, { Suspense } from "react";
import { render } from "react-dom";
import { BrowserRouter as Router, Route } from "react-router-dom";

import PantheonUI from "./PantheonUI";
import { getAllConfig, getConfig } from "./PantheonUI/components/Config";
import { ListCatalogs } from "./PantheonUI/components/queries";

export interface CatalogResponse {
  data: Array<{
    id: string;
    name: string;
  }>;
}

// Setup Sentry
Sentry.init({
  dsn: getConfig("sentryDSN"),
  environment: process.env.NODE_ENV,
  release: process.env.VERSION,
});

declare global {
  interface Window {
    /**
     * Version of the app: `{package-version}-{git-short-sha}`
     */
    version?: string;
  }
}

const PantheonUISidenavHeader = React.lazy(() => import("./PantheonUI/SidenavHeader"));

// tslint:disable-next-line:no-var-requires
const { dependencies, devDependencies } = require("../package.json");

const App: React.SFC = () => (
  <Router>
    <Route
      path="/:catalogId?"
      render={routerProps => {
        return (
          <OperationalUI onError={Sentry.captureException} pushState={routerProps.history.push}>
            <Layout
              header={
                <HeaderBar
                  logo={<Logo to="/" name="Pantheon" />}
                  main={
                    <ListCatalogs base={getConfig("backend")}>{
                      data => 
                        <HeaderMenu
                          items={data && data.data && data.data.map(c => ({
                                  value: c.id || "",
                                  label: c.name || "",
                                  icon: "Project" as IconName,
                                })) || []}
                          onClick={selected => routerProps.history.push(`/${selected.value}`)}
                          withCaret
                        >
                          <Icon name="Project" left />
                          {data && (data.data.find(c => c.id == routerProps.match.params.catalogId) || {name: ""}).name}
                        </HeaderMenu>
                    }</ListCatalogs>
                  }
                />
              }
              sidenav={
                <Sidenav compact={getConfig("compactSidenav")}>
                  <Suspense fallback="">
                    <PantheonUISidenavHeader
                      basePath={`/${routerProps.match.params.catalogId}`}
                    />
                  </Suspense>
                </Sidenav>
              }
              main={
                <Route
                  render={({ history: { push, replace } }) => (
                    <Suspense fallback="">
                      <PantheonUI
                        locale="en"
                        basePath={`/:projectId`}
                        pushState={push}
                        replaceState={replace}
                      />
                      {getConfig("dev") && (
                        <Debug
                          title={`Pantheon v${process.env.VERSION}`}
                          values={{
                            configMap: getAllConfig(),
                            dependencies,
                            devDependencies,
                          }}
                        />
                      )}
                    </Suspense>
                  )}
                />
              }
            />
          </OperationalUI>
        );
      }}
    />
  </Router>
);

render(<App />, document.querySelector("#app"));
