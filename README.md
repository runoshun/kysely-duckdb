## Kysely Dialect for DuckDB

[![test](https://github.com/runoshun/kysely-duckdb/actions/workflows/test.yml/badge.svg)](https://github.com/runoshun/kysely-duckdb/actions/workflows/test.yml)

This dialect allows you to use [Kysely](https://kysely.dev/) with [DuckDB](https://duckdb.org/).
Please see following instructions and [API Reference](https://runoshun.github.io/kysely-duckdb/).

### Installation

```bash
$ npm install --save kysely @duckdb/node-api kysely-duckdb
```

### Usage

```ts
import { DuckDBInstance } from "@duckdb/node-api";
import { Kysely } from "kysely";
import { DuckDbDialect } from "kysely-duckdb";

const db = await DuckDBInstance.create(":memory:");
const duckdbDialect = new DuckDbDialect({
  database: db,
  tableMappings: {
    person:
      `read_json('./person.json', columns={"first_name": "STRING", "gender": "STRING", "last_name": "STRING"})`,
    // Schema-qualified keys work with .withSchema()
    "archive.person": `read_parquet('s3://bucket/archive/person.parquet')`,
  },
});
const kysely = new Kysely<DatabaseSchema>({ dialect: duckdbDialect });
const res = await kysely.selectFrom("person").selectAll().execute();
// Uses "archive.person" mapping
const archived = await kysely.withSchema("archive").selectFrom("person")
  .selectAll().execute();
```

### Configrations

The configuration object of `DuckDbDialect` can contain the following
properties:

- `database`: A `DuckDBInstance` instance or a function that returns a `Promise`
  of a `DuckDBInstance` instance.
- `tableMappings`: A mapping of table names in Kysely to DuckDB table
  expressions. This is useful if you want to use DuckDB's external data sources,
  such as JSON files or CSV files. Keys can be schema-qualified (e.g.,
  `"schema.table"`) to match `.withSchema()` queries. When a schema is specified
  but not found in mappings, the query bypasses mappings entirely (useful for
  attached databases).

When a schema is specified via `.withSchema()` but no matching schema-qualified
key exists in `tableMappings`, the query bypasses table mappings entirely. This
allows you to query attached databases directly while still using mappings for
local data sources.

### DuckDB DataTypes Supports (Experimental Feature)

DuckDB supports various data types like arrays, structs, blobs and more.
Kysely has not built in supports for these types, but it can handle almost
of these using [raw SQL](https://kysely.dev/docs/recipes/raw-sql) feature.

This package includes some shallow helper for these types.

```ts
import type { DuckDBNodeDataTypes } from "kysely-duckdb";
import { datatypes } from "kysely-dockdb";

// DuckDBNodeDataTypes: type mappings for table schema
export interface Database {
  t1: {
    int_list: number[];
    string_list: string[];
    map1: DuckDBNodeDataTypes["MAP"]; // `map` is alias of string now. The returned value from duckdb is like '{a=1,b=2}'
    struct1: {
      x: number;
      y: string;
    };
    bitstring1: DuckDBNodeDataTypes["BIT"];
    blob1: DuckDBNodeDataTypes["BLOB"];
    bool1: DuckDBNodeDataTypes["BOOLEAN"];
    date1: DuckDBNodeDataTypes["DATE"];
    timestamp1: DuckDBNodeDataTypes["TIMESTAMP"];
    timestamptz1: DuckDBNodeDataTypes["TIMESTAMPTZ"];
    interval1: DuckDBNodeDataTypes["INTERVAL"];
  };
}

...

// datatypes: type constructors
const kysely = new Kysely<Database>({dialect: duckDbDialect});
await kysely
  .insertInto("t1")
  .values([{
    int_list: datatypes.list([3, 4, 5]),
    string_list: datatypes.list(["d", "e", "f"]),
    map1: types.map([[1, 2], [3, 4]]),
    struct1: datatypes.struct({
      x: sql`${1}`,
      y: sql`${"aaa"}`,
    }),
    bitstring1: datatypes.bit("010101"),
    blob1: datatypes.blob(Buffer.from([0xBB, 0xCC])),
    bool1: true,
    date1: datatypes.date(new Date()),
    timestamp1: datatypes.timestamp(new Date()),
    timestamptz1: datatypes.timestamptz(new Date().toISOString().slice(0, -1) + "+03:00"),
    interval1: sql`INTERVAL 1 YEAR`,
  }])
  .execute();
```
