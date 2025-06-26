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
import { DuckDbWasmDriver, type DuckDbWasmDriverConfig } from "./driver-wasm";
import { DuckDbNodeDriver, type DuckDbNodeDriverConfig } from "./driver-node";
import { DuckDbIntrospector } from "./introspector";
import { DuckDbQueryCompiler, type DuckDbQueryCompilerConfigs } from "./query-compiler";

export type DuckDbDialectConfig = Simplify<(DuckDbWasmDriverConfig | DuckDbNodeDriverConfig) & DuckDbQueryCompilerConfigs>;

/**
 * Kysely dialect for duckdb.
 *
 * ## Quick Start and Usage Example
 * Please see also [Kysely Docs](https://kysely.dev/docs/intro) and [Duckdb Docs](https://duckdb.org/docs/)
 *
 * ### Install
 * ```bash
 * $ npm install --save kysely duckdb kysely-duckdb
 * ```
 *
 * ### Basic Usage Example
 * reding data from json file.
 * ```ts
 * import * as duckdb from "duckdb";
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
 * const db = new duckdb.Database(":memory:");
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
    // Check if the config contains a Node.js DuckDB database
    if ('database' in this.config && this.config.database && 
        typeof this.config.database === 'object' && 
        'connect' in this.config.database && 
        typeof this.config.database.connect === 'function') {
      // This is a Node.js DuckDB database
      return new DuckDbNodeDriver(this.config as DuckDbNodeDriverConfig & DuckDbQueryCompilerConfigs);
    } else {
      // This is a WASM DuckDB database
      return new DuckDbWasmDriver(this.config as DuckDbWasmDriverConfig & DuckDbQueryCompilerConfigs);
    }
  }
  createAdapter(): DialectAdapter {
    return new DuckDbAdapter();
  }
}

export * as datatypes from "./helper/datatypes";
export type { DuckDBNodeDataTypes } from "./helper/datatypes";
export { DuckDbNodeDriver, type DuckDbNodeDriverConfig } from "./driver-node";
export { DuckDbWasmDriver, type DuckDbWasmDriverConfig } from "./driver-wasm";
