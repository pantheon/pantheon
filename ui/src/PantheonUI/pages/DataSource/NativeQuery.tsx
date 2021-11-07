import React from "react";
import { GridPageContent, NativeQuery } from "../../components";

export interface NativeQueryProps {
  catalogId: string;
  dataSourceId: string;
}

const DataSourceNativeQuery: React.SFC<NativeQueryProps> = ({ catalogId, dataSourceId }) => (
  <GridPageContent>
    <NativeQuery catalogId={catalogId} dataSourceId={dataSourceId} />
  </GridPageContent>
);

export default DataSourceNativeQuery;
