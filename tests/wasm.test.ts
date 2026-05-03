import { CompiledQuery, Kysely } from "kysely";
import { createDuckDbWasmDatabase, DuckDbWasmDialect } from "../src/wasm-node";

interface WasmDatabase {
  t1: {
    a: number;
    b: string;
  };
}

test("wasm dialect supports select, insert, affected rows, and cleanup", async () => {
  const duckdb = await createDuckDbWasmDatabase();
  const kysely = new Kysely<WasmDatabase>({
    dialect: new DuckDbWasmDialect({
      database: duckdb,
      tableMappings: {},
    }),
  });

  try {
    const selectResult = await kysely
      .selectNoFrom((eb) => [
        eb.val(1).as("a"),
        eb.val("x").as("b"),
      ])
      .executeTakeFirstOrThrow();
    expect(selectResult).toEqual({ a: 1, b: "x" });

    await kysely.executeQuery(CompiledQuery.raw("CREATE TABLE t1 (a INT, b VARCHAR);"));
    const insertResult = await kysely
      .insertInto("t1")
      .values([
        { a: 1, b: "one" },
        { a: 2, b: "two" },
      ])
      .executeTakeFirstOrThrow();

    expect(insertResult.numInsertedOrUpdatedRows).toBe(BigInt(2));

    const updateResult = await kysely
      .updateTable("t1")
      .set({ b: "updated" })
      .where("a", "=", 2)
      .executeTakeFirstOrThrow();
    expect(updateResult.numUpdatedRows).toBe(BigInt(1));

    const rows = await kysely.selectFrom("t1").selectAll().orderBy("a").execute();
    expect(rows).toEqual([
      { a: 1, b: "one" },
      { a: 2, b: "updated" },
    ]);

    const count = await kysely
      .selectFrom("t1")
      .select((eb) => eb.fn.count<number>("a").as("count"))
      .executeTakeFirstOrThrow();
    expect(count).toEqual({ count: BigInt(2) });
  } finally {
    await kysely.destroy();
  }

  expect(duckdb.isDetached()).toBe(true);
});
