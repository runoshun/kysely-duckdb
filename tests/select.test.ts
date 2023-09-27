import { sql } from "kysely";
import * as types from "../src/helper/datatypes";
import { setupDb } from "./test_common";

test("select from json file", async () => {
  const kysely = await setupDb();
  const results = await kysely.selectFrom("person").select(["first_name"]).execute();
  expect(results).toEqual([{ first_name: "foo" }]);

  const results2 = await kysely.selectFrom("person").select(["gender"]).execute();
  expect(results2).toEqual([{ gender: "man" }]);
});

test("select from table", async () => {
  const kysely = await setupDb();

  const results = await kysely.selectFrom("t1").selectAll().execute();
  expect(results).toEqual([{ a: 1, b: 2 }]);
});

test("select complex data types", async () => {
  const kysely = await setupDb();

  const results = await kysely.selectFrom("t2").selectAll().execute();
  expect(results.length).toBe(1);
  const row = results[0];
  expect(row.int_list).toEqual([1, 2, 3]);
  expect(row.string_list).toEqual(["a", "b", "c"]);
  expect(row.m).toEqual("{a=text, b=text}");
  expect(row.st).toEqual({ x: 1, y: "a" });
  expect(row.bs).toEqual("010101");
  expect(row.bl).toEqual(Buffer.from([0xAA]));
  expect(row.bool).toEqual(true);
  expect(row.dt).toEqual(new Date(1992, 8, 20));
  expect(row.ts).toEqual(new Date(1992, 8, 20, 11, 30, 0, 123));
  expect(row.enm).toEqual("sad");
  expect(row.delta).toEqual({ months: 12, days: 0, micros: 0 });
});

test("select complex data types with where", async () => {
  const kysely = await setupDb();

  const results = await kysely
    .selectFrom("t2")
    .selectAll()
    .where("bs", "=", types.bit("010101"))
    .where("bl", "=", types.blob(Buffer.from([0xAA])))
    .where("bool", "=", true)
    .where("dt", "=", types.date(new Date(1992, 8, 20)))
    .where("int_list", "=", types.list([1, 2, 3]))
    .where("string_list", "=", types.list(["a", "b", "c"]))
    .where("st", "=", types.struct({ x: sql.val(1), y: sql.val("a") }))
    .where("ts", "=", types.timestamp(new Date(1992, 8, 20, 11, 30, 0, 123)))
    .where("tsz", "=", types.timestamptz("1992-09-20 11:30:00.123+03:00"))
    .execute();
  expect(results.length).toBe(1);
});
