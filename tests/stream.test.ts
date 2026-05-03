import { setupDb } from "./test_common";

describe("streamQuery", () => {
  test("streams rows from a table", async () => {
    const kysely = await setupDb();

    // Insert additional rows to have more data to stream
    await kysely.insertInto("t1")
      .values([
        { a: 3, b: 4 },
        { a: 5, b: 6 },
        { a: 7, b: 8 },
      ])
      .execute();

    const results: { a: number; b: number }[] = [];
    const stream = kysely.selectFrom("t1").selectAll().stream();

    for await (const row of stream) {
      results.push(row);
    }

    expect(results.length).toBe(4);
    expect(results).toContainEqual({ a: 1, b: 2 });
    expect(results).toContainEqual({ a: 3, b: 4 });
    expect(results).toContainEqual({ a: 5, b: 6 });
    expect(results).toContainEqual({ a: 7, b: 8 });
  });

  test("streams rows with correct types", async () => {
    const kysely = await setupDb();

    const stream = kysely.selectFrom("t2").selectAll().stream();

    const results: Array<{
      int_list: number[];
      string_list: string[];
      m: string;
      st: { x: number; y: string };
      bs: string;
      bl: Buffer;
      bool: boolean;
      dt: Date;
      ts: Date;
      tsz: Date;
      enm: string;
      delta: { months: number; days: number; micros: number };
    }> = [];

    for await (const row of stream) {
      results.push(row as any);
    }

    expect(results.length).toBe(1);
    const row = results[0];
    expect(row.int_list).toEqual([1, 2, 3]);
    expect(row.string_list).toEqual(["a", "b", "c"]);
    expect(row.st).toEqual({ x: 1, y: "a" });
    expect(row.bs).toEqual("010101");
    expect(row.bl).toEqual(Buffer.from([0xAA]));
    expect(row.bool).toEqual(true);
    expect(row.dt).toEqual(new Date(1992, 8, 20));
    expect(row.ts).toEqual(new Date(1992, 8, 20, 11, 30, 0, 123));
    expect(row.enm).toEqual("sad");
    expect(row.delta).toEqual({ months: 12, days: 0, micros: 0 });
  });

  test("streams empty result set", async () => {
    const kysely = await setupDb();

    const stream = kysely.selectFrom("t1").selectAll().where("a", "=", 999)
      .stream();

    const results: unknown[] = [];
    for await (const row of stream) {
      results.push(row);
    }

    expect(results).toEqual([]);
  });

  test("streams with partial iteration", async () => {
    const kysely = await setupDb();

    // Insert more rows
    await kysely.insertInto("t1")
      .values([
        { a: 10, b: 20 },
        { a: 30, b: 40 },
        { a: 50, b: 60 },
      ])
      .execute();

    const stream = kysely.selectFrom("t1").selectAll().stream();

    const results: { a: number; b: number }[] = [];
    for await (const row of stream) {
      results.push(row);
      if (results.length >= 2) {
        break;
      }
    }

    expect(results.length).toBe(2);
  });

  test("can iterate stream multiple times by creating new streams", async () => {
    const kysely = await setupDb();

    // First iteration
    const results1: { a: number; b: number }[] = [];
    for await (const row of kysely.selectFrom("t1").selectAll().stream()) {
      results1.push(row);
    }

    // Second iteration with a new stream
    const results2: { a: number; b: number }[] = [];
    for await (const row of kysely.selectFrom("t1").selectAll().stream()) {
      results2.push(row);
    }

    expect(results1).toEqual(results2);
    expect(results1.length).toBe(1);
  });
});
