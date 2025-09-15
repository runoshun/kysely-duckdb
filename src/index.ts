import {
  type DatabaseIntrospector,
  type Dialect,
  type DialectAdapter,
  type Driver,
  Kysely,
  type QueryCompiler,
  type Simplify,
} from "kysely";

import { DuckDbAdapter } from "./adapter";
import { DuckDbNodeDriver, type DuckDbNodeDriverConfig } from "./driver-node";
import { DuckDbIntrospector } from "./introspector";
import { DuckDbQueryCompiler, type DuckDbQueryCompilerConfigs } from "./query-compiler";

export type DuckDbDialectConfig = Simplify<DuckDbNodeDriverConfig & DuckDbQueryCompilerConfigs>;

/**
 * Kysely dialect for duckdb.
 *
 * ## Quick Start and Usage Example
 * Please see also [Kysely Docs](https://kysely.dev/docs/intro) and [Duckdb Docs](https://duckdb.org/docs/)
 *
 * ### Install
 * ```bash
 * $ npm install --save kysely @duckdb/node-api kysely-duckdb
 * ```
 *
 * ### Basic Usage Example
 * reding data from json file.
 * ```ts
 * import { DuckDBInstance } from "@duckdb/node-api";
 * import { Kysely } from "kysely";
 * import { DuckDbDialect } from "kysely-duckdb"
 *
 * interface PersonTable {
 *   first_name: string,
 *   gender: string,
 *   last_name: string,
 * };
 * interface DatabaseSchema {
 *   person: PersonTable,
 * };
 *
 * const db = await DuckDBInstance.create(":memory:");
 * const duckdbDialect = new DuckDbDialect({
 *   database: db,
 *   tableMappings: {
 *     person:
 *       `read_json('./person.json', columns={"first_name": "STRING", "gender": "STRING", "last_name": "STRING"})`,
 *   },
 * });
 * const kysely = new Kysely<DatabaseSchema>({ dialect: duckdbDialect });
 *
 * const res = await kysely.selectFrom("person").selectAll().execute();
 * ```
 */
export class DuckDbDialect implements Dialect {
  /**
   * @param config configulations for DuckDbDialect
   */
  constructor(private readonly config: DuckDbDialectConfig) {
  }
  createQueryCompiler(): QueryCompiler {
    return new DuckDbQueryCompiler(this.config);
  }
  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new DuckDbIntrospector(db);
  }
  createDriver(): Driver {
    return new DuckDbNodeDriver(this.config);
  }
  createAdapter(): DialectAdapter {
    return new DuckDbAdapter();
  }
}

export * as datatypes from "./helper/datatypes";
export type { DuckDBNodeDataTypes } from "./helper/datatypes";
