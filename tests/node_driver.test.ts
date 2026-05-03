import { DuckDBInstance } from "@duckdb/node-api";
import { Kysely } from "kysely";
import { DuckDbDialect } from "../src/index";
import { setupDb } from "./test_common";

interface NodeDriverDatabase {
  seeded: {
    id: number;
    label: string;
  };
}

test("node driver initializes from database factory and runs onCreateConnection", async () => {
  let databaseFactoryCalls = 0;
  let connectionsCreated = 0;

  const kysely = new Kysely<NodeDriverDatabase>({
    dialect: new DuckDbDialect({
      database: async () => {
        databaseFactoryCalls += 1;
        return DuckDBInstance.create(":memory:");
      },
      onCreateConnection: async (connection) => {
        connectionsCreated += 1;
        await connection.run("CREATE TABLE IF NOT EXISTS seeded (id INT, label VARCHAR);");
      },
    }),
  });

  try {
    await kysely.insertInto("seeded")
      .values({ id: 1, label: "ready" })
      .execute();

    const rows = await kysely.selectFrom("seeded").selectAll().execute();

    expect(rows).toEqual([{ id: 1, label: "ready" }]);
    expect(databaseFactoryCalls).toBe(1);
    expect(connectionsCreated).toBeGreaterThan(0);
  } finally {
    await kysely.destroy();
  }
});

test("node driver commits and rolls back transactions", async () => {
  const kysely = await setupDb();

  try {
    await kysely.transaction().execute(async (trx) => {
      await trx.insertInto("t1").values({ a: 100, b: 200 }).execute();
    });

    await expect(kysely.transaction().execute(async (trx) => {
      await trx.insertInto("t1").values({ a: 300, b: 400 }).execute();
      throw new Error("rollback node transaction");
    })).rejects.toThrow("rollback node transaction");

    const rows = await kysely.selectFrom("t1")
      .selectAll()
      .where("a", "in", [100, 300])
      .orderBy("a")
      .execute();

    expect(rows).toEqual([{ a: 100, b: 200 }]);
  } finally {
    await kysely.destroy();
  }
});
