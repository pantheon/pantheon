import { SchemaObject } from "openapi3-ts";
import { getNonSystemParams } from "../getNonSystemParams";

describe("getNonSystemParams", () => {
  it("should return only the non system properties", () => {
    const properties: SchemaObject["properties"] = {
      a: {
        title: "shouldbefilter",
        type: "string",
        system: true,
      },
      b: {
        title: "shouldnotbefilter",
        type: "string",
      },
    };

    expect(getNonSystemParams(properties)).toEqual({
      b: {
        title: "shouldnotbefilter",
        type: "string",
      },
    });
  });
});
