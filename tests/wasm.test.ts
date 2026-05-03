import { CompiledQuery, Kysely, sql } from "kysely";
import { createDuckDbWasmDatabase, DuckDbWasmDialect } from "../src/wasm-node";

interface WasmDatabase {
  t1: {
    a: number;
    b: string;
  };
  complex_values: {
    id: number;
    int_list: number[];
    st: {
      x: number;
      y: string;
    };
    bl: Buffer;
    dt: Date;
    ts: Date;
  };
  seeded: {
    id: number;
  };
}

async function setupWasmDb(config: Partial<ConstructorParameters<typeof DuckDbWasmDialect>[0]> = {}) {
  const duckdb = await createDuckDbWasmDatabase();
  const kysely = new Kysely<WasmDatabase>({
    dialect: new DuckDbWasmDialect({
      database: duckdb,
      tableMappings: {},
      ...config,
    }),
  });

  return { duckdb, kysely };
}

test("wasm dialect supports select, insert, affected rows, and cleanup", async () => {
  const { duckdb, kysely } = await setupWasmDb();

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

test("wasm dialect runs onCreateConnection and rolls back transactions", async () => {
  let connectionsCreated = 0;
  const { duckdb, kysely } = await setupWasmDb({
    onCreateConnection: async (connection) => {
      connectionsCreated += 1;
      await connection.query("CREATE TABLE IF NOT EXISTS seeded (id INT);");
    },
  });

  try {
    await kysely.insertInto("seeded").values({ id: 1 }).execute();

    await expect(kysely.transaction().execute(async (trx) => {
      await trx.insertInto("seeded").values({ id: 2 }).execute();
      throw new Error("rollback this transaction");
    })).rejects.toThrow("rollback this transaction");

    const rows = await kysely.selectFrom("seeded")
      .selectAll()
      .orderBy("id")
      .execute();

    expect(rows).toEqual([{ id: 1 }]);
    expect(connectionsCreated).toBeGreaterThan(0);
  } finally {
    await kysely.destroy();
  }

  expect(duckdb.isDetached()).toBe(true);
});

test("wasm dialect streams rows", async () => {
  const { duckdb, kysely } = await setupWasmDb();

  try {
    await kysely.executeQuery(CompiledQuery.raw("CREATE TABLE t1 (a INT, b VARCHAR);"));
    await kysely.insertInto("t1")
      .values([
        { a: 1, b: "one" },
        { a: 2, b: "two" },
        { a: 3, b: "three" },
      ])
      .execute();

    const rows = [];
    for await (
      const row of kysely.selectFrom("t1")
        .selectAll()
        .orderBy("a")
        .stream()
    ) {
      rows.push(row);
    }

    expect(rows).toEqual([
      { a: 1, b: "one" },
      { a: 2, b: "two" },
      { a: 3, b: "three" },
    ]);
  } finally {
    await kysely.destroy();
  }

  expect(duckdb.isDetached()).toBe(true);
});

test("wasm dialect converts nested Arrow values and binary data", async () => {
  const { duckdb, kysely } = await setupWasmDb();

  try {
    await sql`
      CREATE TABLE complex_values (
        id INT,
        int_list INT[],
        st STRUCT(x INT, y VARCHAR),
        bl BLOB,
        dt DATE,
        ts TIMESTAMP
      );
    `.execute(kysely);
    await sql`
      INSERT INTO complex_values VALUES (
        1,
        [1, 2, 3],
        {'x': 10, 'y': 'nested'},
        '\\xAA\\xBB',
        '1992-09-20',
        '1992-09-20 11:30:00.123'
      );
    `.execute(kysely);

    const row = await kysely.selectFrom("complex_values")
      .selectAll()
      .executeTakeFirstOrThrow();

    expect(row).toEqual({
      id: 1,
      int_list: [1, 2, 3],
      st: { x: 10, y: "nested" },
      bl: Buffer.from([0xAA, 0xBB]),
      dt: new Date(1992, 8, 20),
      ts: new Date(1992, 8, 20, 11, 30, 0, 123),
    });
  } finally {
    await kysely.destroy();
  }

  expect(duckdb.isDetached()).toBe(true);
});
