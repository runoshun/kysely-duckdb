import { sql } from "kysely";
import * as types from "../src/helper/datatypes";
import { setupDb } from "./test_common";

test("inset into table", async () => {
  const kysely = await setupDb();

  const res = await kysely.insertInto("t1")
    .values([{
      a: 1,
      b: 2,
    }, {
      a: 2,
      b: 3,
    }])
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numInsertedOrUpdatedRows).toBe(BigInt(2));

  const selectRes = await kysely.selectFrom("t1").selectAll().execute();
  expect(selectRes.length).toBe(3);
});

test("insert into complex types", async () => {
  const kysely = await setupDb();

  const res = await kysely.insertInto("t2")
    .values([{
      int_list: types.list([3, 4, 5]),
      string_list: types.list(["d", "e", "f"]),
      m: types.map([[1, 2], [3, 4]]),
      st: types.struct({
        x: sql`${1}`,
        y: sql`${"aaa"}`,
      }),
      bs: types.bit("010101"),
      bl: types.blob(Buffer.from([0xBB, 0xCC])),
      bool: true,
      dt: types.date(new Date()),
      ts: types.timestamp(new Date()),
      tsz: types.timestamptz(new Date().toISOString().slice(0, -1) + "+03:00"),
      enm: "sad",
      delta: sql`INTERVAL 1 YEAR`,
    }])
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numInsertedOrUpdatedRows).toBe(BigInt(1));
});

test("update table", async () => {
  const kysely = await setupDb();

  const res = await kysely.updateTable("t1")
    .set({
      a: 10,
    })
    .where("a", "=", 1)
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numUpdatedRows).toBe(BigInt(1));
});

// Additional tests for edge cases and potential error scenarios

test("insert with missing fields", async () => {
  const kysely = await setupDb();

  const res = await kysely.insertInto("t1")
    .values([{
      a: 3,
    }])
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numInsertedOrUpdatedRows).toBe(BigInt(1));

  const selectRes = await kysely.selectFrom("t1").selectAll().execute();
  expect(selectRes.length).toBe(3);
});

test("insert with null values", async () => {
  const kysely = await setupDb();

  const res = await kysely.insertInto("t1")
    .values([{
      a: null,
      b: 4,
    }])
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numInsertedOrUpdatedRows).toBe(BigInt(1));

  const selectRes = await kysely.selectFrom("t1").selectAll().execute();
  expect(selectRes.length).toBe(3);
});

test("update with non-existing condition", async () => {
  const kysely = await setupDb();

  const res = await kysely.updateTable("t1")
    .set({
      a: 20,
    })
    .where("a", "=", 100)
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numUpdatedRows).toBe(BigInt(0));
});

test("update with null values", async () => {
  const kysely = await setupDb();

  const res = await kysely.updateTable("t1")
    .set({
      a: null,
    })
    .where("a", "=", 1)
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numUpdatedRows).toBe(BigInt(1));
});
