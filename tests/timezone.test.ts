import { sql } from "kysely";
import * as types from "../src/helper/datatypes";
import { setupDb } from "./test_common";

const originalTz = process.env.TZ;

afterAll(() => {
  process.env.TZ = originalTz;
});

test.each(["UTC", "Asia/Tokyo", "America/New_York"])(
  "date and timestamp helpers preserve local calendar fields in %s",
  async (tz) => {
    process.env.TZ = tz;
    const kysely = await setupDb();
    const dt = new Date(1992, 8, 20);
    const ts = new Date(1992, 8, 20, 11, 30, 0, 123);

    await kysely.insertInto("t2")
      .values({
        int_list: types.list([4, 5, 6]),
        string_list: types.list(["tz", "case", tz]),
        m: types.map([["tz", tz]]),
        st: types.struct({ x: sql.val(4), y: sql.val(tz) }),
        bs: types.bit("101010"),
        bl: types.blob(Buffer.from([0xEF])),
        bool: true,
        dt: types.date(dt),
        ts: types.timestamp(ts),
        tsz: types.timestamptz("1992-09-20 11:30:00.123+03:00"),
        enm: "happy",
        delta: sql`INTERVAL 3 DAY`,
      })
      .execute();

    const rows = await kysely.selectFrom("t2")
      .select(["dt", "ts"])
      .where("bs", "=", types.bit("101010"))
      .where("dt", "=", types.date(dt))
      .where("ts", "=", types.timestamp(ts))
      .execute();

    expect(rows).toEqual([{ dt, ts }]);
    await kysely.destroy();
  },
);
