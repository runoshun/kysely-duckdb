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
import { DuckDbIntrospector } from "./introspector";
import { DuckDbQueryCompiler, type DuckDbQueryCompilerConfigs } from "./query-compiler";

export type DuckDbWasmDialectConfig = Simplify<DuckDbWasmDriverConfig & DuckDbQueryCompilerConfigs>;

export class DuckDbWasmDialect implements Dialect {
  constructor(private readonly config: DuckDbWasmDialectConfig) {
  }

  createQueryCompiler(): QueryCompiler {
    return new DuckDbQueryCompiler(this.config);
  }

  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new DuckDbIntrospector(db);
  }

  createDriver(): Driver {
    return new DuckDbWasmDriver(this.config);
  }

  createAdapter(): DialectAdapter {
    return new DuckDbAdapter();
  }
}

export {
  AsyncDuckDB,
  ConsoleLogger,
  getJsDelivrBundles,
  selectBundle,
  VoidLogger,
} from "@duckdb/duckdb-wasm";
export type {
  AsyncDuckDBConnection,
  DuckDBBundle,
  DuckDBBundles,
  Logger,
} from "@duckdb/duckdb-wasm";
