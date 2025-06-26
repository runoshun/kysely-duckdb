import * as duckdb from "duckdb";
import { CompiledQuery } from "kysely";
import type { DatabaseConnection, Driver, QueryResult } from "kysely";

export interface DuckDbNodeDriverConfig {
  database: duckdb.Database;
  tableMappings?: { [tableName: string]: string };
  onCreateConnection?: (connection: duckdb.Connection) => Promise<void>;
}

export class DuckDbNodeDriver implements Driver {
  readonly #config: DuckDbNodeDriverConfig;
  #db: duckdb.Database;

  constructor(config: DuckDbNodeDriverConfig) {
    this.#config = Object.freeze({ ...config });
    this.#db = config.database;
  }

  async init(): Promise<void> {
    // Node.js DuckDB doesn't require async initialization
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    const conn = this.#db.connect();
    if (this.#config.onCreateConnection) {
      await this.#config.onCreateConnection(conn);
    }
    return new DuckDBNodeConnection(conn);
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
    await (connection as DuckDBNodeConnection).disconnect();
  }

  async destroy(): Promise<void> {
    // Node.js DuckDB doesn't require explicit cleanup
  }
}

class DuckDBNodeConnection implements DatabaseConnection {
  readonly #conn: duckdb.Connection;

  constructor(conn: duckdb.Connection) {
    this.#conn = conn;
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    const { sql, parameters } = compiledQuery;
    
    return new Promise((resolve, reject) => {
      const stmt = this.#conn.prepare(sql);
      
      stmt.all(...parameters, (err: Error | null, rows: any[]) => {
        if (err) {
          reject(err);
          return;
        }
        
        resolve(this.formatToResult(rows, sql));
      });
    });
  }

  async *streamQuery<R>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<R>> {
    // For Node.js DuckDB, we'll just execute the query and yield the result
    const result = await this.executeQuery<R>(compiledQuery);
    yield result;
  }

  private formatToResult<O>(rows: any[], sql: string): QueryResult<O> {
    const isSelect = sql.toLowerCase().trim().startsWith("select") || 
                    sql.toLowerCase().includes("returning");

    if (isSelect) {
      return { rows: rows as O[] };
    } else {
      // For non-select queries, rows should contain the affected row count
      // Node.js DuckDB returns different formats for different query types
      const numAffectedRows = rows.length > 0 && typeof rows[0] === 'object' && 'Count' in rows[0] 
        ? BigInt(rows[0].Count) 
        : BigInt(rows.length);

      return {
        numAffectedRows,
        insertId: undefined,
        rows: [],
      };
    }
  }

  async disconnect(): Promise<void> {
    return new Promise((resolve) => {
      this.#conn.close((err) => {
        if (err) {
          console.warn("Error closing DuckDB connection:", err);
        }
        resolve();
      });
    });
  }
}