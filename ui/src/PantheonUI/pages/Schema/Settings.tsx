import {
  Button,
  Card,
  CardItem,
  FinePrint,
  Form,
  Input,
  OperationalContext,
  PageArea,
  PageContent,
  Textarea,
} from "@operational/components";

import { OperationalRouter } from "@operational/router";
import React, { useState } from "react";
import { GetMethod } from "restful-react";
import { EditButton, EditSchemaActions } from "../../components";
import { DeleteSchema, Schema } from "../../data/schema";
import { EditStatus } from "./Schema";

export interface SettingsTabProps {
  schema: Schema;
  refetch: GetMethod<Schema>;
}

export const SettingsTab: React.SFC<SettingsTabProps> = ({ schema, refetch }) => {
  const [editStatus, setEditStatus] = useState<EditStatus>("readonly");
  const [description, setDescription] = useState(schema.description);

  const isReadOnly = editStatus === "readonly";

  return (
    <PageContent>
      {({ confirm }) => (
        <PageArea>
          <Card
            title="Details"
            action={
              isReadOnly ? (
                <EditButton
                  data-cy="pantheon--schema--settings__edit-details-button"
                  onClick={() => setEditStatus("editing")}
                />
              ) : (
                <EditSchemaActions
                  schema={schema}
                  draftSchema={{ ...schema, description }}
                  onCancelClick={() => {
                    setEditStatus("readonly");
                    setDescription(schema.description);
                  }}
                  onSaveBegin={() => setEditStatus("saving")}
                  onSaveSuccess={() => {
                    setEditStatus("readonly");
                    refetch();
                  }}
                  onSaveFailed={() => setEditStatus("editing")}
                />
              )
            }
          >
            {isReadOnly ? (
              <>
                <CardItem title="Name" value={schema.name} />
                <CardItem title="Description" value={schema.description} />
              </>
            ) : (
              <Form onSubmit={e => e.preventDefault()}>
                <Input label="Name" disabled hint="Schema names can only be edited inside PSL" value={schema.name} />
                <Textarea label="Description" value={description} onChange={setDescription} />
              </Form>
            )}
          </Card>
          <DeleteSchema catalogId={schema.catalogId} schemaId={schema.id}>
            {(deleteSchema, { loading: loadingDelete }) => (
              <Card title="Danger zone">
                <OperationalRouter>
                  {({ pushState }) => (
                    <OperationalContext>
                      {({ pushMessage }) => (
                        <Button
                          color="error"
                          loading={loadingDelete}
                          onClick={() => {
                            confirm({
                              title: "Danger zone",
                              body:
                                "Queries using this schema will no longer work. Are you sure you want to delete this schema?",
                              actionButton: <Button color="error">Delete</Button>,
                              onConfirm: () => {
                                deleteSchema().then(() => {
                                  pushMessage({ body: "Schema deleted successfully", type: "info" });
                                  pushState(`schemas`);
                                });
                              },
                            });
                          }}
                        >
                          Delete
                        </Button>
                      )}
                    </OperationalContext>
                  )}
                </OperationalRouter>
                <FinePrint>Queries using this schema will no longer work.</FinePrint>
              </Card>
            )}
          </DeleteSchema>
        </PageArea>
      )}
    </PageContent>
  );
};

export default SettingsTab;
