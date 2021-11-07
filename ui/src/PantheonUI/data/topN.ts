import * as tucson from "tucson-decode";

import { OrderedColumn, orderedColumnDecoder } from "./orderedColumn";

export interface TopN {
  dimensions: string[];
  n: number;
  orderBy: OrderedColumn[];
}

export const topNDecoder: tucson.Decoder<TopN> = tucson.object({
  dimensions: tucson.array(tucson.string),
  n: tucson.number,
  orderBy: tucson.array(orderedColumnDecoder),
});

export const topNSample: TopN = {
  dimensions: [],
  n: 0,
  orderBy: [],
};
