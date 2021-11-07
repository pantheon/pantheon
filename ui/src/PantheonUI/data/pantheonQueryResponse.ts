import * as tucson from "tucson-decode";

export interface Response {
  columns: Array<{ ref: string }>;
  rows: string[][];
}

export const sqlResponseDecoder: tucson.Decoder<Response> = tucson.object({
  columns: tucson.array(tucson.object({ ref: tucson.string, primitive: tucson.string })),
  //  tucson.any because rows can be of different primitive values. Relying of JS type coercion for stringification in current impl.
  rows: tucson.array(tucson.array(tucson.any)),
});

export const responseDecoder: tucson.Decoder<Response> = tucson.object({
  columns: tucson.array(tucson.object({ ref: tucson.string })),
  rows: tucson.array(tucson.array(tucson.any)),
});
