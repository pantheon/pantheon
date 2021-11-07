import { Body, Button, Card, Page } from "@operational/components";
import React from "react";

import { IdpMeResp } from "../components/GetMe";

export interface DefaultPageProps {
  me: IdpMeResp;
}

const DefaultPage: React.SFC<DefaultPageProps> = ({ me }) => (
  <Page title="Welcome">
    <Card title="Please Select a Project">
      <Body>Welcome to Pantheon UI. Please select a project from the menu above.</Body>
      <Body>
        Alternatively, if you do not have projects assigned to you yet, please ask your account manager for more
        information.
      </Body>
      {me && me.realms.length && (
        <Button to={`c/${me.realms[0].id}`} textColor="primary">
          Go to First Project
        </Button>
      )}
    </Card>
  </Page>
);

export default DefaultPage;
