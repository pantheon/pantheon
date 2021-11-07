import { Button } from "@operational/components";
import React from "react";
import { Schema, UpdateSchema } from "../data/schema";

export interface EditSchemaActionsProps {
  schema: Schema;
  draftSchema: Schema;
  onCancelClick: () => void;
  onSaveBegin: () => void;
  onSaveSuccess: () => void;
  onSaveFailed: () => void;
}

export const EditSchemaActions: React.SFC<EditSchemaActionsProps> = props => (
  <>
    <Button data-cy="pantheon--schema__cancel-button" condensed onClick={props.onCancelClick}>
      Cancel
    </Button>
    <UpdateSchema catalogId={props.schema.catalogId} schemaId={props.schema.id}>
      {(updateSchema, { loading }) => (
        <Button
          condensed
          data-cy="pantheon--schema__save-button"
          loading={loading}
          color="primary"
          onClick={() => {
            props.onSaveBegin();
            updateSchema(props.draftSchema)
              .then(props.onSaveSuccess)
              .catch(props.onSaveFailed);
          }}
        >
          Save
        </Button>
      )}
    </UpdateSchema>
  </>
);

export default EditSchemaActions;
