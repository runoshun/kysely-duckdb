import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { CompiledQuery } from "kysely";
import type { DatabaseConnection, Driver, QueryResult } from "kysely";

export interface DuckDbWasmDriverConfig {
  /**
   * AsyncDuckDB instance or a function that returns a Promise of an AsyncDuckDB instance.
   * The database must already be instantiated before the dialect is initialized.
   */
  database: (() => Promise<AsyncDuckDB>) | AsyncDuckDB;
  /**
   * Called when a connection is created.
   * @param conection AsyncDuckDBConnection instance that is created.
   * @returns Promise<void>
   */
  onCreateConnection?: (conection: AsyncDuckDBConnection) => Promise<void>;
}

export class DuckDbWasmDriver implements Driver {
  readonly #config: DuckDbWasmDriverConfig;
  #db?: AsyncDuckDB;

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
    return new DuckDBWasmConnection(conn);
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
    await (connection as DuckDBWasmConnection).disconnect();
  }

  async destroy(): Promise<void> {
    await this.#db?.terminate();
  }
}

class DuckDBWasmConnection implements DatabaseConnection {
  readonly #conn: AsyncDuckDBConnection;

  constructor(conn: AsyncDuckDBConnection) {
    this.#conn = conn;
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    const { sql, parameters } = compiledQuery;
    const stmt = await this.#conn.prepare(sql);

    try {
      const result = await stmt.query(...parameters);
      return this.#formatToResult(result, this.#isMutationQuery(compiledQuery));
    } finally {
      await stmt.close();
    }
  }

  async *streamQuery<R>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<R>> {
    const { sql, parameters } = compiledQuery;
    const stmt = await this.#conn.prepare(sql);

    try {
      const reader = await stmt.send(...parameters);
      const isMutationQuery = this.#isMutationQuery(compiledQuery);

      for await (const result of reader) {
        yield this.#formatToResult(result, isMutationQuery);
      }
    } finally {
      await stmt.close();
    }
  }

  #isMutationQuery(compiledQuery: CompiledQuery): boolean {
    switch (compiledQuery.query.kind) {
      case "InsertQueryNode":
      case "UpdateQueryNode":
      case "DeleteQueryNode":
      case "MergeQueryNode":
        return true;
      case "SelectQueryNode":
        return false;
      default:
        return !compiledQuery.sql.trimStart().toLocaleLowerCase().startsWith("select");
    }
  }

  #formatToResult<O>(result: ArrowResult, isMutationQuery: boolean): QueryResult<O> {
    if (!isMutationQuery) {
      return { rows: result.toArray().map((row) => this.#convertValue(row)) as O[] };
    }

    const row = result.get(0) as Record<string, unknown> | null | undefined;
    const count = row == null ? undefined : row["Count"] ?? row["count"];
    const numAffectedRows = count == null ? undefined : BigInt(count as string | number | bigint | boolean);

    return {
      numChangedRows: numAffectedRows,
      numAffectedRows,
      insertId: undefined,
      rows: [],
    };
  }

  async disconnect(): Promise<void> {
    await this.#conn.close();
  }

  #convertValue(value: unknown): unknown {
    if (value == null) return value;
    if (value instanceof Date) return value;
    if (value instanceof Uint8Array) return Buffer.from(value);
    if (Array.isArray(value)) return value.map((item) => this.#convertValue(item));
    if (typeof value === "object") {
      const entries = Object.entries(value);
      if (entries.length === 0) return value;
      const obj: Record<string, unknown> = {};
      for (const [key, item] of entries) {
        obj[key] = this.#convertValue(item);
      }
      return obj;
    }
    return value;
  }
}

interface ArrowResult {
  readonly numRows: number;
  readonly schema: {
    readonly fields: ReadonlyArray<{
      readonly name: string;
    }>;
  };
  get(index: number): unknown;
  toArray(): unknown[];
}
