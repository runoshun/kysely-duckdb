import duckdb from "duckdb";
import { CompiledQuery } from "kysely";
import type { DatabaseConnection, Driver, QueryResult } from "kysely";

export interface DuckDbNodeDriverConfig {
  /**
   * duckdb.Database instance or a function returns a Promise of duckdb.Database instance.
   */
  database: (() => Promise<duckdb.Database>) | duckdb.Database;
  /**
   * called when a connection is created.
   * @param conection duckdb.Connection instance that is created.
   * @returns Promise<void>
   */
  onCreateConnection?: (conection: duckdb.Connection) => Promise<void>;
}

export class DuckDbNodeDriver implements Driver {
  readonly #config: DuckDbNodeDriverConfig;
  #db?: duckdb.Database;

  constructor(config: DuckDbNodeDriverConfig) {
    this.#config = Object.freeze({ ...config });
  }

  async init(): Promise<void> {
    this.#db = (typeof this.#config.database === "function")
      ? await this.#config.database()
      : this.#config.database;
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    const conn = this.#db!.connect();
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
    await this.#db!.close();
  }
}

class DuckDBConnection implements DatabaseConnection {
  readonly #conn: duckdb.Connection;

  constructor(conn: duckdb.Connection) {
    this.#conn = conn;
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    const { sql, parameters } = compiledQuery;
    const stmt = this.#conn.prepare(sql);

    return new Promise((res, rej) => {
      stmt.all(...parameters, (err, result) => {
        if (err) {
          return rej(err);
        }
        return res(this.formatToResult(result, sql));
      });
    });
  }

  async *streamQuery<R>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<R>> {
    const { sql, parameters } = compiledQuery;
    const iter = this.#conn.stream(sql, ...parameters);
    const self = this;
    const gen = async function*() {
      let isSelect: undefined | boolean = undefined;
      for await (const result of iter) {
        if (isSelect === undefined) {
          isSelect = self.isSelect([result], sql);
        }

        yield self.formatToResult([result], sql, isSelect);
      }
    };
    return gen();
  }

  private isSelect(result: duckdb.TableData, sql: string): boolean {
    if (result.length === 0) {
      return sql.toLocaleLowerCase().includes("select");
    }

    // I can not detect correct query type easily..., use workaround in here.
    const isInsertedRows = Object.keys(result[0]).length == 1
      && Object.keys(result[0])[0] == "Count"
      && result.length == 1;
    return !isInsertedRows;
  }

  private formatToResult<O>(result: duckdb.TableData, sql: string, isSelect?: boolean): QueryResult<O> {
    if (isSelect === undefined) {
      isSelect = this.isSelect(result, sql);
    }

    if (isSelect) {
      return { rows: result as O[] };
    } else {
      const row = result[0];
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
  }
}
