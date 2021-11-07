import { CardSection, Message, Spinner, Tree } from "@operational/components";
import pick from "lodash/pick";
import React, { useContext } from "react";
import { useGet } from "restful-react";

import { QueryEditorContext } from ".";
import NakedCard from "../NakedCard";
import { CompatibilityResponse } from "../queries";
import { useToggle } from "../useToggle";
import { getDroppableId } from "./dragAndDropManager";
import { dimensionsToTree, measuresToTree } from "./utils";

export const AggregrateInventory: React.FC<{ mock?: CompatibilityResponse }> = ({ mock }) => {
  const { state, dispatch } = useContext(QueryEditorContext);
  const [isMeasuresCollapsed, toggleMeasures] = useToggle(false);
  const [isDimensionsCollapsed, toggleDimensions] = useToggle(false);

  const { data: compatibilityData } = useGet<CompatibilityResponse>({
    path: `catalogs/${state.schema.catalogId}/schemas/${state.schema.id}/compatibility`,
    lazy: Boolean(mock),
    requestOptions: {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query: {
          type: state.queryType,
          ...pick(state.aggregate.query, ["measures", "rows", "columns", "filter"]), // avoid useless calls, only these attributes affect the compatibility response
        },
      }),
    },
  });

  // Reference query, to have all measures/dimensions availables
  const { data, loading, error } = useGet<CompatibilityResponse>({
    path: `catalogs/${state.schema.catalogId}/schemas/${state.schema.id}/compatibility`,
    lazy: Boolean(mock),
    requestOptions: {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query: {
          type: state.queryType,
          columns: [],
          measures: [],
          rows: [],
        },
      }),
    },
  });
  return (
    <NakedCard
      className="expanded" // flag for BaseLayout
      sections={
        error ? (
          <CardSection title="Available Data">
            <Message type="error" title="Compatibility error" body={error.message} />
          </CardSection>
        ) : (
          <>
            <CardSection title="Available Measures" collapsed={isMeasuresCollapsed} onToggle={toggleMeasures}>
              {loading ? (
                <Spinner />
              ) : (
                <Tree
                  droppableProps={{ droppableId: getDroppableId("inventoryMeasure"), isDropDisabled: true }}
                  trees={
                    data || mock
                      ? measuresToTree(
                          mock ? mock.measures : data ? data.measures : [],
                          dispatch,
                          mock
                            ? mock.measures.map(m => m.ref)
                            : compatibilityData
                            ? compatibilityData.measures.map(m => m.ref)
                            : [],
                        )
                      : []
                  }
                />
              )}
            </CardSection>
            <CardSection title="Available Dimensions" collapsed={isDimensionsCollapsed} onToggle={toggleDimensions}>
              {loading ? (
                <Spinner />
              ) : (
                <Tree
                  trees={
                    data || mock
                      ? dimensionsToTree(
                          mock ? mock.dimensions : data ? data.dimensions : [],
                          dispatch,
                          mock
                            ? mock.dimensions.map(m => m.ref)
                            : compatibilityData
                            ? compatibilityData.dimensions.map(d => d.ref)
                            : [],
                        )
                      : []
                  }
                />
              )}
            </CardSection>
          </>
        )
      }
    />
  );
};
