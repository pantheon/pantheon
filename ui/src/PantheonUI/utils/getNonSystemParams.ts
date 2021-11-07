import omitBy from "lodash/fp/omitBy";
import { SchemaObject } from "openapi3-ts";

export const getNonSystemParams = omitBy((p: SchemaObject) => p.system);
