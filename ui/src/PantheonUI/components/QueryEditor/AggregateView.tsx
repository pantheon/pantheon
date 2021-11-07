import React, { useContext } from "react";
import { QueryEditorContext } from ".";
import { CompatibilityResponse } from "../queries";
import { AggregrateInventory } from "./AggregateInventory";
import { AggregateVisualPanelLeft } from "./AggregateVisualPanelLeft";
import { AggregateVisualPanelTop } from "./AggregateVisualPanelTop";
import { CodeCard } from "./CodeCard";
import Layout, { LayoutProps } from "./Layout";
import { ResultsCard } from "./ResultsCard";

export const AggregateView: React.FC<{ mock?: CompatibilityResponse }> = ({ mock }) => {
  const { state } = useContext(QueryEditorContext);

  const areas = [
    <AggregrateInventory mock={mock} key="inventory" />,
    ...(state.aggregate.mode === "visual"
      ? [<AggregateVisualPanelLeft />, <AggregateVisualPanelTop />]
      : [<CodeCard />]),
    <ResultsCard key="results" />,
  ] as LayoutProps["children"];

  return <Layout children={areas} />;
};
