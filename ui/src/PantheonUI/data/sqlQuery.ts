import * as tucson from "tucson-decode";

export interface SqlQuery {
  type: "Sql";
  sql: string;
}

export const makeClearSqlQuery = (): SqlQuery => ({
  type: "Sql",
  sql: "",
});

export const sqlQueryDecoder: tucson.Decoder<SqlQuery> = tucson.object({
  type: tucson.flatMap(
    tucson.string,
    val => (val === "Sql" ? tucson.succeed("Sql") : tucson.fail("Invalid type")) as tucson.Decoder<"Sql">,
  ),
  sql: tucson.map(tucson.optional(tucson.string), val => val || "select * from my_table"),
});
