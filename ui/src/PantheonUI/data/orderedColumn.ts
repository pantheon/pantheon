import * as tucson from "tucson-decode";

type SortOrder = "Asc" | "Desc";

export interface OrderedColumn {
  name: string;
  order: SortOrder;
}

export const orderedColumnDecoder: tucson.Decoder<OrderedColumn> = tucson.object({
  name: tucson.string,
  order: tucson.flatMap(
    tucson.string,
    val =>
      (["Asc", "Desc"].includes(val)
        ? tucson.succeed(val as SortOrder)
        : tucson.fail("Invalid type")) as tucson.Decoder<SortOrder>,
  ),
});
