import { Dimension, DimensionWithPrimitiveAndMetadata, MultidimensionalDataset } from "@operational/visualizations";
import { Response } from "../queries";

// Types and interfaces
type PantheonResultColumn = Dimension & {
  values: string[] | null;
  measure: string | null;
};

type PantheonResultRow = Array<number | string>;

export interface PantheonResult {
  measureHeaders: DimensionWithPrimitiveAndMetadata[];
  columns: Array<DimensionWithPrimitiveAndMetadata | PantheonResultColumn>;
  rows: PantheonResultRow[];
  measures: DimensionWithPrimitiveAndMetadata[];
}

const queryToDataset = (query: PantheonResult): MultidimensionalDataset<number> => {
  // Columns without "measure" property are the row dimensions
  const rowDimensions: DimensionWithPrimitiveAndMetadata[] = query.columns.filter(
    col => !("measure" in col) || !col.measure,
  );

  // MeasureHeaders and a "measures" dimension are the column dimensions
  const columnDimensions: DimensionWithPrimitiveAndMetadata[] = [
    ...query.measureHeaders,
    {
      key: "measures",
      metadata: { measures: query.measures },
      primitive: "string",
    },
  ];

  return new MultidimensionalDataset({
    rowDimensions,
    columnDimensions,
    // The first cells are the row dimension values
    rows: query.rows.map(row => row.slice(0, rowDimensions.length).map(cell => (cell || "").toString())),
    // The measure columns' values and the measure name are the column dimension values
    columns: query.columns
      .filter(col => "measure" in col && col.measure !== null)
      .map(col => [
        ...("values" in col && col !== null ? col.values : []),
        // we filtered out null values one row above, but TS can't infer type of thie
        (col as any).measure as string,
      ]),
    // The cells after the row dimension values are the data
    data: query.rows.map(row => row.slice(rowDimensions.length) as number[]),
  });
};

export default (data: Response) =>
  queryToDataset({
    columns: data.columns.map(c => ({
      key: c.ref,
      ...c,
    })),
    measureHeaders: data.measureHeaders.map(mh => ({
      key: mh.ref,
      ...mh,
    })),
    measures: data.measures.map(m => ({
      key: m.ref,
      ...m,
    })),
    rows: data.rows,
  }).transform(cell => () => (cell.value() || "").toString() + " "); /* @todo: fix this in the Grid */
