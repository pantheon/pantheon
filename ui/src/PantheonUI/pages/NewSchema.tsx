import { Button, Card, Form, Input, OperationalContext, Page, Textarea } from "@operational/components";
import { OperationalRouter } from "@operational/router";
import get from "lodash/get";
import isEmpty from "lodash/isEmpty";
import React, { useState } from "react";

import { CreateSchema, Schema } from "../data/schema";
import { Validation } from "../types";

export interface Props {
  catalogId: string;
}

export interface State {
  isSaving: boolean;
  schema: Schema;
  validation: null | Validation<Schema>;
}

const NewSchema: React.SFC<Props> = ({ catalogId }) => {
  const [schema, setSchema] = useState<Partial<Schema>>({});
  const [error, setError] = useState<string | undefined>();

  return (
    <Page title="Create Schema">
      <CreateSchema catalogId={catalogId}>
        {(create, { loading: loadingCreateSchema }) => (
          <OperationalRouter>
            {({ pushState }) => (
              <OperationalContext>
                {({ pushMessage }) => {
                  const saveSchema = () => {
                    if (isEmpty(schema.name)) {
                      return;
                    }
                    create(schema)
                      .then(res => {
                        pushMessage({ body: "Schema created successfully", type: "info" });
                        pushState(`schemas/${res.id}`);
                      })
                      .catch(e => {
                        if (get<string>(e, "data.errors.0", "").includes("already exists")) {
                          setError(`schema "${schema.name}" already exists`);
                        } else {
                          pushMessage({ type: "error", body: get<string>(e, "message", "") });
                        }
                      });
                  };

                  return (
                    <Card
                      title="New Schema"
                      action={
                        <Button
                          color="primary"
                          data-cy="pantheon--new-schema__save-schema"
                          disabled={isEmpty(schema.name)}
                          loading={loadingCreateSchema}
                          condensed
                          onClick={saveSchema}
                        >
                          Save
                        </Button>
                      }
                    >
                      <Form
                        onSubmit={e => {
                          e.preventDefault(), saveSchema();
                        }}
                      >
                        <Input
                          data-cy="pantheon--new-schema__name-input"
                          placeholder="my-schema"
                          label="Name"
                          error={error}
                          value={schema.name}
                          onChange={name => {
                            setSchema({ ...schema, name, psl: `schema ${name} {}` });
                            if (error) {
                              setError(undefined);
                            }
                          }}
                        />
                        <Textarea
                          label="Description"
                          data-cy="schema-new-schema-description"
                          value={schema.description || ""}
                          onChange={description => setSchema({ ...schema, description })}
                        />
                      </Form>
                    </Card>
                  );
                }}
              </OperationalContext>
            )}
          </OperationalRouter>
        )}
      </CreateSchema>
    </Page>
  );
};

export default NewSchema;
