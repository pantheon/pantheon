import createConfig from "react-runtime-config";

export interface RuntimeConfig {
  backend: string;
  loginUrl: string;
  logoutUrl: string;
  meUrl: string;
}

const defaultConfig = {
  sentryDSN: undefined,
  dev: process.env.NODE_ENV === "development" || Boolean(new URL(window.location.href).searchParams.get("dev")),
  useNewQueryEditor: false,
  compactSidenav: false,
};

export const { Config, setConfig, getConfig, getAllConfig, ...runtimeConfig } = createConfig<
  RuntimeConfig & typeof defaultConfig
>({
  defaultConfig,
  namespace: "contiamo.pantheon",
});

/**
 * Local devtools, for super powers shortcuts
 */
if (process.env.NODE_ENV === "development") {
  let prevKey = "";
  document.addEventListener("keyup", event => {
    /**
     * Toggle `useNewQueryEditor` on `cq` keys (as Config Query editor)
     */
    if (prevKey === "c" && event.key === "q") {
      setConfig("useNewQueryEditor", !getConfig("useNewQueryEditor"));
    }
    /**
     * Toggle `dev` on `cd` keys (as Config Dev)
     */
    if (prevKey === "c" && event.key === "d") {
      setConfig("dev", !getConfig("dev"));
    }
    /**
     * Toggle `compactSidenav` on `cc` keys (as Config CompactSidenav)
     */
    if (prevKey === "c" && event.key === "c") {
      setConfig("compactSidenav", !getConfig("compactSidenav"));
    }

    prevKey = event.key;
  });
  /**
   * Switch the backend to local `contiamo/platform` if the domain is `localhost`
   */
  if (window.location.host.includes("localhost")) {
    // tslint:disable-next-line:no-console
    console.log("Connected to `localhost:4300` backend. Pantheon UI is OVERRIDING YOUR PANTHEON LOCAL CONFIGMAP.");
    setConfig("backend", "http://localhost:4300"); // TODO fix restful-react path composition for useGet
    setConfig("loginUrl", "http://localhost:4300/auth/login");
    setConfig("logoutUrl", "http://localhost:4300/auth/logout");
    setConfig("meUrl", "http://localhost:4300/auth/api");
  }
  /**
   * Switch the backend to dev.contiamo.io if the domain is `*.contiamo.io`
   */
  if (window.location.host.includes("contiamo.io")) {
    // tslint:disable-next-line:no-console
    console.log("Connected to `*.dev.contiamo.io` backend");
    setConfig("backend", "https://pantheon.dev.contiamo.io/contiamo/");
    setConfig("loginUrl", "https://auth.dev.contiamo.io/login");
    setConfig("logoutUrl", "https://auth.dev.contiamo.io/logout");
    setConfig("meUrl", "https://auth.dev.contiamo.io/api");
  }
}
