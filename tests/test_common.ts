import * as duckdb from "duckdb";
import type { DuckDBNodeDataTypes } from "../src/helper/datatypes";
import { datatypes } from "../src/index";
import { DuckDbDialect } from "../src/index";

import type { ColumnType, Generated } from "kysely";
import { CompiledQuery, Kysely } from "kysely";

export interface Database {
  person: PersonTable;
  t1: { a: number; b: number; };
  t2: {
    int_list: number[];
    string_list: string[];
    m: string;
    st: {
      x: number;
      y: string;
    };
    bs: DuckDBNodeDataTypes["BIT"];
    bl: DuckDBNodeDataTypes["BLOB"];
    bool: DuckDBNodeDataTypes["BOOLEAN"];
    dt: DuckDBNodeDataTypes["DATE"];
    ts: DuckDBNodeDataTypes["TIMESTAMP"];
    tsz: DuckDBNodeDataTypes["TIMESTAMPTZ"];
    enm: string;
    delta: DuckDBNodeDataTypes["INTERVAL"];
  };
}

export interface PersonTable {
  id: Generated<number>;
  first_name: string;
  gender: "man" | "woman" | "other";
  last_name: string | null;
  created_at: ColumnType<Date, string | undefined, never>;
}

export const setupDb = async () => {
  const db = new duckdb.Database(":memory:");
  const duckdbDialect = new DuckDbDialect({
    database: db,
    tableMappings: {
      person:
        `read_json('./tests/person.json', columns={"first_name": "STRING", "gender": "STRING", "last_name": "STRING"})`,
    },
  });
  const kysely = new Kysely<Database>({ dialect: duckdbDialect });
  // t1
  await kysely.executeQuery(CompiledQuery.raw("CREATE TABLE t1 (a INT, b INT);"));
  await kysely.executeQuery(CompiledQuery.raw("INSERT INTO t1 VALUES (1, 2);"));

  // t2
  await kysely.executeQuery(CompiledQuery.raw(
    "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');",
  ));
  await kysely.executeQuery(CompiledQuery.raw(
    "CREATE TABLE t2 (" + [
      "int_list INT[]",
      "string_list STRING[]",
      "m MAP(STRING, STRING)",
      "st STRUCT(x INT, y STRING)",
      "bs BIT",
      "bl BLOB",
      "bool BOOLEAN",
      "dt DATE",
      "ts TIMESTAMP",
      "tsz TIMESTAMPTZ",
      "enm mood",
      "delta INTERVAL",
    ].join(", ") + ");",
  ));

  await kysely.executeQuery(CompiledQuery.raw(
    "INSERT INTO t2 VALUES (" + [
      "[1, 2, 3]",
      "['a', 'b', 'c']",
      "map {'a': 'text', 'b': 'text'}",
      "{'x': 1, 'y': 'a'}",
      "'010101'",
      "'\\xAA'",
      "true",
      "'1992-09-20'",
      "'1992-09-20 11:30:00.123'",
      "'1992-09-20 11:30:00.123+03:00'",
      "'sad'",
      "INTERVAL '1' YEAR",
    ].join(", ") + ");",
  ));

  return kysely;
};
