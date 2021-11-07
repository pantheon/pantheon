import { Button, Card } from "@operational/components";
import React from "react";

import { DataSource, UpdateDataSource } from "../data/dataSource";
import { Draft } from "../types";
import { BaseProps as BaseDsEditorProps } from "./DataSourceEditor";

const DataSourceFields: React.SFC<{
  title: string;
  dataSource: DataSource;
  draft: Draft<Partial<DataSource>> | null;
  setDraft: (d: Draft<Partial<DataSource>> | null) => void;
  refetch: () => void;
  onEdit: () => void;
  showFields: (p: BaseDsEditorProps) => React.ReactNode;
}> = ({ title, dataSource, draft, setDraft, refetch, showFields, onEdit }) => (
  <Card
    title={title}
    action={
      <>
        <Button
          data-cy="pantheon--data-source-wizard__edit-button"
          textColor={draft ? undefined : "primary"}
          condensed
          onClick={() => {
            if (draft) {
              setDraft(null);
            } else {
              onEdit();
              setDraft({ draft: dataSource });
            }
          }}
        >
          {draft ? "Cancel" : "Edit"}
        </Button>

        {draft && draft.draft.catalogId && draft.draft.id && (
          <UpdateDataSource catalogId={draft.draft.catalogId} dataSourceId={draft.draft.id}>
            {(update, { loading }) => (
              <Button
                condensed
                color="primary"
                loading={loading}
                onClick={() => {
                  setDraft({ ...draft, saving: true });
                  update(draft.draft)
                    .then(() => refetch())
                    .then(() => setDraft(null));
                }}
              >
                Save
              </Button>
            )}
          </UpdateDataSource>
        )}
      </>
    }
  >
    {showFields({
      dataSource: draft ? draft.draft : dataSource,
      readOnly: draft === null,
      onChange: d => setDraft({ draft: d }),
    })}
  </Card>
);

export default DataSourceFields;
