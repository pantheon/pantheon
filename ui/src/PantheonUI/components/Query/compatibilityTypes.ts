import * as tucson from "tucson-decode";

export type Primitive = boolean | string | number;

export type MeasureAggregate = "Sum" | "Avg" | "Count" | "DistinctCount" | "ApproxDistinctCount" | "Max" | "Min";

/**
 * Server response
 */

interface NestedCompatResponse {
  measures: MeasuresSchemaScopedCompatResponse[];
  dimensions: DimensionsSchemaScopedCompatResponse[];
}

// Measures

interface MeasuresCompatResponse {
  ref: string;
  measure: string;
  aggregate?: MeasureAggregate;
  metadata: { [key: string]: Primitive | Primitive[] };
}

interface MeasuresSchemaScopedCompatResponse {
  schema?: string;
  children: MeasuresCompatResponse[];
}

// Dimensions
interface AttributeCompat {
  attribute: string;
  // dynamic object
  metadata: any;
  ref: string;
}

interface LevelCompat {
  level: string;
  children: AttributeCompat[];
}

interface HierarchyCompat {
  hierarchy: string;
  children: LevelCompat[];
}

interface DimensionCompat {
  dimension: string;
  children: HierarchyCompat[];
}

interface DimensionsSchemaScopedCompatResponse {
  schema?: string;
  children: DimensionCompat[];
}

// Schemas
const dimensionsCompatSchema: tucson.Decoder<DimensionsSchemaScopedCompatResponse> = tucson.object({
  schema: tucson.optional(tucson.string),
  children: tucson.array(
    tucson.object({
      dimension: tucson.string,
      children: tucson.array(
        tucson.object({
          hierarchy: tucson.string,
          children: tucson.array(
            tucson.object({
              level: tucson.string,
              children: tucson.array(
                tucson.object({
                  attribute: tucson.string,
                  ref: tucson.string,
                  metadata: tucson.any,
                }),
              ),
            }),
          ),
        }),
      ),
    }),
  ),
});

const measuresCompatSchema: tucson.Decoder<MeasuresSchemaScopedCompatResponse> = (() => {
  const aggregateSchema = tucson.oneOf(
    tucson.literal("Sum"),
    tucson.literal("Avg"),
    tucson.literal("Count"),
    tucson.literal("DistinctCount"),
    tucson.literal("ApproxDistinctCount"),
    tucson.literal("Max"),
    tucson.literal("Min"),
  );

  return tucson.object({
    schema: tucson.optional(tucson.string),
    children: tucson.array(
      tucson.object<MeasuresCompatResponse>({
        ref: tucson.string,
        measure: tucson.string,
        aggregate: tucson.optional(aggregateSchema),
        metadata: tucson.dictionary(tucson.string),
      }),
    ),
  });
})();

export const compatibilityDecoder: tucson.Decoder<Compatibility> = tucson.map(
  tucson.object<NestedCompatResponse>({
    measures: tucson.array(measuresCompatSchema),
    dimensions: tucson.array(dimensionsCompatSchema),
  }),
  ({ measures, dimensions }) => {
    const nestedDimensions = <T>(
      ts: T[],
      getRef: (_: T) => string | undefined,
      getChildren: (_: T) => CompatibilityField[],
    ): CompatibilityField[] =>
      ts.length === 1 && !getRef(ts[0])
        ? getChildren(ts[0])
        : ts.map(t => ({ ref: getRef(t) || "Default", children: getChildren(t) }));

    const dimFields = nestedDimensions(
      dimensions,
      s => s.schema,
      s =>
        nestedDimensions(
          s.children,
          d => d.dimension,
          d =>
            nestedDimensions(
              d.children,
              h => h.hierarchy,
              h =>
                nestedDimensions(
                  h.children,
                  l => l.level,
                  l =>
                    l.children.map(c => ({
                      ref: c.ref,
                      children: [],
                    })),
                ),
            ),
        ),
    );

    const mesFlds: CompatibilityField[] = (() => {
      const mkRef = (m: MeasuresCompatResponse): CompatibilityField => ({
        ref: m.ref,
        children: [],
      });

      switch (measures.length) {
        case 0:
          return [];
        case 1:
          return measures[0].children.map(mkRef);
        default:
          return measures.map(measure => ({
            ref: measure.schema ? measure.schema : "Default",
            children: measure.children.map(mkRef),
          }));
      }
    })();

    return {
      measures: mesFlds,
      dimensions: dimFields,
    };
  },
);

/**
 * This is a simplified version of the raw backend structure returned from the nested compatibility endpoints.
 * The aim for this simplification is to remove domain-specific information (levels, hierarchies) that do not have
 * a value for UI rendering (which just packs dimensions and measures into 'folders' and renders them in a nested way.
 * It is important to note that it also does away with the strict 3-level nesting structure in favor of a recursive type,
 * which makes frontend code a lot terser.
 * Should new UI needs arise, this interface can be extended to include tags ("h" for hierarchy) etc.
 */
export interface CompatibilityField {
  ref: string;
  children: CompatibilityField[];
}

export interface Compatibility {
  measures: CompatibilityField[];
  dimensions: CompatibilityField[];
}
