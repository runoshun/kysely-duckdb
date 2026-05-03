import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import {
  AsyncDuckDB,
  selectBundle,
  VoidLogger,
  type DuckDBBundles,
  type Logger,
} from "@duckdb/duckdb-wasm";

export interface CreateDuckDbWasmDatabaseConfig {
  bundles?: DuckDBBundles;
  logger?: Logger;
}

export async function createDuckDbWasmDatabase(
  config: CreateDuckDbWasmDatabaseConfig = {},
): Promise<AsyncDuckDB> {
  const duckdbDist = dirname(require.resolve("@duckdb/duckdb-wasm/dist/duckdb-node.cjs"));
  const bundle = await selectBundle(config.bundles ?? createNodeBundles(duckdbDist));
  const worker = await createNodeWorker(bundle.mainWorker);
  const db = new AsyncDuckDB(config.logger ?? new VoidLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}

function createNodeBundles(duckdbDist: string): DuckDBBundles {
  return {
    mvp: {
      mainModule: join(duckdbDist, "duckdb-mvp.wasm"),
      mainWorker: pathToFileURL(join(duckdbDist, "duckdb-node-mvp.worker.cjs")).href,
    },
    eh: {
      mainModule: join(duckdbDist, "duckdb-eh.wasm"),
      mainWorker: pathToFileURL(join(duckdbDist, "duckdb-node-eh.worker.cjs")).href,
    },
  };
}

async function createNodeWorker(workerUrl: string): Promise<Worker> {
  const { default: WebWorker } = await import("web-worker");
  return new WebWorker(workerUrl) as Worker;
}

export { DuckDbWasmDialect } from "./wasm";
export type { DuckDbWasmDialectConfig } from "./wasm";
