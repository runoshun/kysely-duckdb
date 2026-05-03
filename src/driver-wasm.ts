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
      return { rows: result.toArray().map((row) => this.#convertRow(row, result.schema.fields)) as O[] };
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

  #convertRow(value: unknown, fields: ReadonlyArray<ArrowField>): unknown {
    if (value == null || typeof value !== "object") return this.#convertValue(value);

    const fieldByName = new Map(fields.map((field) => [field.name, field]));
    const obj: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      obj[key] = this.#convertValue(item, fieldByName.get(key));
    }
    return obj;
  }

  #convertValue(value: unknown, field?: ArrowField): unknown {
    if (value == null) return value;
    if (value instanceof Date) return value;
    if (value instanceof Uint8Array) return Buffer.from(value);
    if (this.#isArrowVector(value)) {
      return Array.from(value.toArray()).map((item) => this.#convertValue(item));
    }
    if (field && this.#isTemporalField(field)) return this.#convertTemporalValue(value, field.type);
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

  #isArrowVector(value: unknown): value is { toArray(): unknown[] } {
    return typeof value === "object"
      && value !== null
      && "toArray" in value
      && typeof (value as { toArray?: unknown }).toArray === "function";
  }

  #isTemporalField(field: ArrowField): boolean {
    return field.type.typeId === ArrowTypeId.Date || field.type.typeId === ArrowTypeId.Timestamp;
  }

  #convertTemporalValue(value: unknown, type: ArrowType): unknown {
    if (typeof value !== "number" && typeof value !== "bigint") return value;

    const milliseconds = typeof value === "bigint" ? Number(value) : value;
    if (type.typeId === ArrowTypeId.Timestamp && type.timezone) {
      return new Date(milliseconds);
    }

    const utc = new Date(milliseconds);
    return new Date(
      utc.getUTCFullYear(),
      utc.getUTCMonth(),
      utc.getUTCDate(),
      type.typeId === ArrowTypeId.Timestamp ? utc.getUTCHours() : 0,
      type.typeId === ArrowTypeId.Timestamp ? utc.getUTCMinutes() : 0,
      type.typeId === ArrowTypeId.Timestamp ? utc.getUTCSeconds() : 0,
      type.typeId === ArrowTypeId.Timestamp ? utc.getUTCMilliseconds() : 0,
    );
  }

}

const enum ArrowTypeId {
  Date = 8,
  Timestamp = 10,
}

interface ArrowResult {
  readonly numRows: number;
  readonly schema: {
    readonly fields: ReadonlyArray<{
      readonly name: string;
      readonly type: ArrowType;
    }>;
  };
  get(index: number): unknown;
  toArray(): unknown[];
}

interface ArrowField {
  readonly name: string;
  readonly type: ArrowType;
}

interface ArrowType {
  readonly typeId: number;
  readonly timezone?: string | null;
}
