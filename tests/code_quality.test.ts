import { DuckDBInstance } from "@duckdb/node-api";
import { CompiledQuery, Kysely } from "kysely";
import { KyselyDuckDbExtension } from "../src/extension";
import { DuckDbDialect } from "../src/index";
import { setupDb } from "./test_common";

test("introspection does not emit console output", async () => {
  const kysely = await setupDb();
  const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});

  try {
    await kysely.introspection.getTables();
    expect(logSpy).not.toHaveBeenCalled();
  } finally {
    logSpy.mockRestore();
  }
});

test("tableMappings can be omitted for regular table usage", async () => {
  const db = await DuckDBInstance.create(":memory:");
  const kysely = new Kysely<{ t1: { a: number; }; }>({
    dialect: new DuckDbDialect({ database: db }),
  });

  await kysely.executeQuery(CompiledQuery.raw("CREATE TABLE t1 (a INT);"));
  await kysely.executeQuery(CompiledQuery.raw("INSERT INTO t1 VALUES (1);"));

  const rows = await kysely.selectFrom("t1").selectAll().execute();
  expect(rows).toEqual([{ a: 1 }]);
});

test("createTablesAsSelect quotes generated table names", async () => {
  const db = await DuckDBInstance.create(":memory:");
  const kysely = new KyselyDuckDbExtension<{ source: { a: number; }; }>({
    dialect: new DuckDbDialect({ database: db }),
  });
  const unsafeTableName = `quoted " table; DROP TABLE source; --`;

  await kysely.executeQuery(CompiledQuery.raw("CREATE TABLE source (a INT);"));
  await kysely.executeQuery(CompiledQuery.raw("INSERT INTO source VALUES (1);"));

  const dbWithTable = await kysely.createTablesAsSelect({
    [unsafeTableName]: kysely.selectFrom("source").select("a"),
  });

  const rows = await dbWithTable.selectFrom(unsafeTableName).selectAll().execute();
  const sourceRows = await kysely.selectFrom("source").selectAll().execute();
  expect(rows).toEqual([{ a: 1 }]);
  expect(sourceRows).toEqual([{ a: 1 }]);
});

test("createTablesAsSelect supports schema-qualified generated table names", async () => {
  const db = await DuckDBInstance.create(":memory:");
  const kysely = new KyselyDuckDbExtension<{ source: { a: number; }; "scratch.generated": { a: number; }; }>({
    dialect: new DuckDbDialect({ database: db }),
  });

  await kysely.executeQuery(CompiledQuery.raw("CREATE SCHEMA scratch;"));
  await kysely.executeQuery(CompiledQuery.raw("CREATE TABLE source (a INT);"));
  await kysely.executeQuery(CompiledQuery.raw("INSERT INTO source VALUES (1);"));

  const dbWithTable = await kysely.createTablesAsSelect({
    "scratch.generated": kysely.selectFrom("source").select("a"),
  });

  const rows = await dbWithTable.selectFrom("scratch.generated").selectAll().execute();
  expect(rows).toEqual([{ a: 1 }]);
});
