/*
import duckdb from "@duckdb/duckdb-wasm";
import arrow from "apache-arrow";
import { CompiledQuery } from "kysely";
import type { DatabaseConnection, Driver, QueryResult } from "kysely";

export interface DuckDbWasmDriverConfig {
  database: (() => Promise<duckdb.AsyncDuckDB>) | duckdb.AsyncDuckDB;
  onCreateConnection?: (conection: duckdb.AsyncDuckDBConnection) => Promise<void>;
}

export class DuckDbWasmDriver implements Driver {
  readonly #config: DuckDbWasmDriverConfig;
  #db?: duckdb.AsyncDuckDB;

  constructor(config: DuckDbWasmDriverConfig) {
    this.#config = Object.freeze({ ...config });
  }

  async init(): Promise<void> {
    this.#db = (typeof this.#config.database === "function")
      ? await this.#config.database()
      : this.#config.database;
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    const conn = await this.#db!.connect();
    if (this.#config.onCreateConnection) {
      await this.#config.onCreateConnection(conn);
    }
    return new DuckDBConnection(conn);
  }

  async beginTransaction(connection: DatabaseConnection): Promise<void> {
    await connection.executeQuery(CompiledQuery.raw("BEGIN TRANSACTION"));
  }

  async commitTransaction(connection: DatabaseConnection): Promise<void> {
    await connection.executeQuery(CompiledQuery.raw("COMMIT"));
  }

  async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
    await connection.executeQuery(CompiledQuery.raw("ROLLBACK"));
  }

  async releaseConnection(connection: DatabaseConnection): Promise<void> {
    await (connection as DuckDBConnection).disconnect();
  }

  async destroy(): Promise<void> {
    await this.#db!.terminate();
  }
}

class DuckDBConnection implements DatabaseConnection {
  readonly #conn: duckdb.AsyncDuckDBConnection;

  constructor(conn: duckdb.AsyncDuckDBConnection) {
    this.#conn = conn;
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    const { sql, parameters } = compiledQuery;
    const stmt = await this.#conn.prepare(sql);

    const result = await stmt.query(...parameters);
    return this.formatToResult(result, sql);
  }

  async *streamQuery<R>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<R>> {
    const { sql, parameters } = compiledQuery;
    const stmt = await this.#conn.prepare(sql);

    const iter = await stmt.send(...parameters);
    const self = this;

    const gen = async function*() {
      for await (const result of iter) {
        yield self.formatToResult(result, sql);
      }
    };
    return gen();
  }

  private formatToResult<O>(result: arrow.Table | arrow.RecordBatch, sql: string): QueryResult<O> {
    const isSelect = result.schema.fields.length == 1
      && result.schema.fields[0].name == "Count"
      && result.numRows == 1
      && sql.toLowerCase().includes("select");

    if (isSelect) {
      return { rows: result.toArray() as O[] };
    } else {
      const row = result.get(0);
      const numAffectedRows = row == null ? undefined : BigInt(row["Count"]);

      return {
        numUpdatedOrDeletedRows: numAffectedRows,
        numAffectedRows,
        insertId: undefined,
        rows: [],
      };
    }
  }

  async disconnect(): Promise<void> {
    return this.#conn.close();
  }
}
*/
