import Cookies from "js-cookie";
import React from "react";
import { Get, GetProps } from "restful-react";
import { Config } from "./Config";

type ServerErrorResponse = Error;

type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>;

// Types from idp
export type Timestamp = number;

export interface RealmMinimalResp {
  id: string;
  name: string;
  tenantId: string;
}

export interface GroupMinimalResp {
  id: string;
  name: string;
  realmId: string;
  tenantId: string;
}

export interface IdpMeResp {
  id: string;
  name: string;
  email: string;
  realms: Array<{
    id: string;
    name: string;
    tenantId: string;
  }>;
  groups: Array<{
    id: string;
    name: string;
    realmId?: string;
    tenantId: string;
  }>;
  sshKeys?: string[];
  createdAt: string;
  updatedAt: string;
  imageURL: string;
  isAdmin: boolean;
  tenant: {
    id: string;
    name: string;
    slug: string;
    color: string;
    logo: string;
  };
  adminRealmIDs?: string[];
}

export interface MeRespTenantInfo {
  id: string;
  name: string;
  slug: string;
  color: string;
  logo: string;
}

export type GetMeProps = Omit<GetProps<IdpMeResp, ServerErrorResponse, void>, "base" | "path">;

export const GetMe = (props: GetMeProps) => (
  <Config>
    {({ getConfig }) => {
      const authBackend = `${getConfig("meUrl")}/v2`;
      return (
        <Get<IdpMeResp, ServerErrorResponse, void>
          base={authBackend}
          path="/me"
          requestOptions={{
            credentials: "include",
            headers: { "x-double-cookie": Cookies.get("double-cookie") || "" },
          }}
          {...props}
        />
      );
    }}
  </Config>
);
