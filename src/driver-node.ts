import {
  DuckDBBitValue,
  DuckDBBlobValue,
  DuckDBConnection as NodeDuckDBConnection,
  DuckDBDateValue,
  DuckDBInstance,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
} from "@duckdb/node-api";
import { CompiledQuery } from "kysely";
import type { DatabaseConnection, Driver, QueryResult } from "kysely";

export interface DuckDbNodeDriverConfig {
  /**
   * DuckDBInstance instance or a function returns a Promise of DuckDBInstance instance.
   */
  database: (() => Promise<DuckDBInstance>) | DuckDBInstance;
  /**
   * called when a connection is created.
   * @param conection DuckDBConnection instance that is created.
   * @returns Promise<void>
   */
  onCreateConnection?: (conection: NodeDuckDBConnection) => Promise<void>;
}

export class DuckDbNodeDriver implements Driver {
  readonly #config: DuckDbNodeDriverConfig;
  #db?: DuckDBInstance;

  constructor(config: DuckDbNodeDriverConfig) {
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
    this.#db!.closeSync();
  }
}

class DuckDBConnection implements DatabaseConnection {
  readonly #conn: NodeDuckDBConnection;

  constructor(conn: NodeDuckDBConnection) {
    this.#conn = conn;
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    const { sql, parameters } = compiledQuery;
    const result = await this.#conn.run(sql, parameters as any);
    const rows = (await result.getRowObjects()).map((r) => this.#convertRow(r));
    return this.formatToResult(rows, sql);
  }

  async *streamQuery<R>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<R>> {
    const { sql, parameters } = compiledQuery;
    const result = await this.#conn.stream(sql, parameters as any);
    const columnNames = result.deduplicatedColumnNames();
    const self = this;
    const gen = async function*() {
      let isSelect: undefined | boolean = undefined;
      while (true) {
        const chunk = await result.fetchChunk();
        if (chunk == null || chunk.rowCount === 0) {
          break;
        }
        const rows = chunk.getRowObjects(columnNames).map((r) => self.#convertRow(r));
        for (const row of rows) {
          if (isSelect === undefined) {
            isSelect = self.isSelect([row], sql);
          }
          yield self.formatToResult([row], sql, isSelect);
        }
      }
    };
    return gen();
  }

  private isSelect(result: Record<string, unknown>[], sql: string): boolean {
    if (result.length === 0) {
      return sql.toLocaleLowerCase().includes("select");
    }

    // I can not detect correct query type easily..., use workaround in here.
    const firstKey = Object.keys(result[0])[0];
    const isInsertedRows = Object.keys(result[0]).length == 1
      && firstKey.toLowerCase() == "count"
      && result.length == 1;
    return !isInsertedRows;
  }

  private formatToResult<O>(result: Record<string, unknown>[], sql: string, isSelect?: boolean): QueryResult<O> {
    if (isSelect === undefined) {
      isSelect = this.isSelect(result, sql);
    }

    if (isSelect) {
      return { rows: result as O[] };
    } else {
      const row = result[0];
      const count = row == null ? undefined : (row as any)["Count"] ?? (row as any)["count"];
      const numAffectedRows = count == null ? undefined : BigInt(count);

      return {
        numChangedRows: numAffectedRows,
        numAffectedRows,
        insertId: undefined,
        rows: [],
      };
    }
  }

  async disconnect(): Promise<void> {
    this.#conn.closeSync();
  }

  #convertRow(row: Record<string, unknown>): Record<string, unknown> {
    const obj: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row)) {
      obj[k] = this.#convertValue(v as any);
    }
    return obj;
  }

  #convertValue(value: any): any {
    if (value == null) return value;
    if (value instanceof DuckDBBitValue) return value.toString();
    if (value instanceof DuckDBBlobValue) return Buffer.from(value.bytes);
    if (value instanceof DuckDBDateValue) {
      // DuckDB DATE represents a calendar date without time zone.
      // Use DuckDB's built-in decomposition to avoid string parsing.
      const { year, month, day } = value.toParts();
      return new Date(year, month - 1, day);
    }
    if (value instanceof DuckDBTimestampValue) {
      // DuckDB TIMESTAMP is timezone-naive. Use built-in parts and interpret as local time.
      const { date, time } = value.toParts();
      return new Date(
        date.year,
        date.month - 1,
        date.day,
        time.hour,
        time.min,
        time.sec,
        Math.floor(time.micros / 1000),
      );
    }
    if (value instanceof DuckDBTimestampTZValue) {
      // DuckDB TIMESTAMPTZ has an associated time zone. The driver returns ISO
      // strings with an offset; 'new Date(isoWithOffset)' yields the absolute
      // UTC moment, which is the desired behavior for JS Date.
      return new Date(value.toString());
    }
    if (value instanceof DuckDBIntervalValue) {
      return { months: value.months, days: value.days, micros: Number(value.micros) };
    }
    if (value instanceof DuckDBListValue) {
      return value.items.map((v) => this.#convertValue(v));
    }
    if (value instanceof DuckDBStructValue) {
      const obj: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value.entries)) {
        obj[k] = this.#convertValue(v);
      }
      return obj;
    }
    if (value instanceof DuckDBMapValue) {
      const entries = value.entries
        .map((e) => `${this.#convertValue(e.key)}=${this.#convertValue(e.value)}`)
        .join(", ");
      return `{${entries}}`;
    }
    if (value instanceof Uint8Array) {
      return Buffer.from(value);
    }
    if (Array.isArray(value)) {
      return value.map((v) => this.#convertValue(v));
    }
    return value;
  }
}
