## Kysely Dialect for DuckDB

[![test](https://github.com/runoshun/kysely-duckdb/actions/workflows/test.yml/badge.svg)](https://github.com/runoshun/kysely-duckdb/actions/workflows/test.yml)

This dialect allows you to use [Kysely](https://kysely.dev/) with [DuckDB](https://duckdb.org/).
Please see following instructions and [API Reference](https://runoshun.github.io/kysely-duckdb/).

### Installation

```bash
$ npm install --save kysely @duckdb/node-api kysely-duckdb
```

For the WASM dialect, install the WASM runtime dependencies instead of
`@duckdb/node-api`:

```bash
$ npm install --save kysely kysely-duckdb @duckdb/duckdb-wasm@1.32.0 web-worker
```

`@duckdb/duckdb-wasm@1.32.0` is the stable runtime version tested by this
package for the Node-compatible WASM smoke path.

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

### WASM Usage

The WASM dialect is exported from a separate subpath so the default node-api
entry point does not import `@duckdb/duckdb-wasm` runtime code.

```ts
import { Kysely } from "kysely";
import { createDuckDbWasmDatabase, DuckDbWasmDialect } from "kysely-duckdb/wasm-node";

interface Database {
  person: {
    id: number;
    first_name: string;
  };
}

const duckdb = await createDuckDbWasmDatabase();
const kysely = new Kysely<Database>({
  dialect: new DuckDbWasmDialect({
    database: duckdb,
    tableMappings: {},
  }),
});

await kysely.schema
  .createTable("person")
  .addColumn("id", "integer")
  .addColumn("first_name", "varchar")
  .execute();

await kysely.insertInto("person").values({ id: 1, first_name: "Ada" }).execute();
const rows = await kysely.selectFrom("person").selectAll().execute();

await kysely.destroy();
```

`createDuckDbWasmDatabase` creates an in-memory `AsyncDuckDB` using
`@duckdb/duckdb-wasm`'s Node worker files and `web-worker`. If you already have
an instantiated `AsyncDuckDB`, import `DuckDbWasmDialect` from
`kysely-duckdb/wasm` and pass it as `database`.

Browser bundlers must provide DuckDB WASM and worker URLs using
`@duckdb/duckdb-wasm`'s bundler-specific setup. The Node-compatible WASM smoke
path is tested in this package; full browser E2E, OPFS persistence, and external
file registration are not covered yet.

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

The configuration object of `DuckDbWasmDialect` has the same `tableMappings`
option, plus:

- `database`: An already instantiated `AsyncDuckDB` instance or a function that returns one.
- `onCreateConnection`: An optional callback that receives each `AsyncDuckDBConnection`.

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
