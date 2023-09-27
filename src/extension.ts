import type { SelectQueryBuilder, Simplify } from "kysely";
import { CompiledQuery, Kysely } from "kysely";

type CompiledQuerySchema<T> = T extends SelectQueryBuilder<any, any, infer O> ? Simplify<O> : never;

/**
 * @alpha
 * Kysely extension methods.
 */
export class KyselyDuckDbExtension<DB> extends Kysely<DB> {
  /**
   * @param tables selectQueries for CTAS.
   * @returns Kysely instance with CTAS tables.
   * @example
   * ```ts
   * const db = new KyselyDuckDbExtension<{
   *   users: {
   *     id: number;
   *     name: string;
   *   };
   * };
   * const db2 = await db.createTablesAsSelect({
   *    userNames: db.selectFrom('users').select(['name']),
   * });
   *
   * // db2 is now a Kysely instance with CTAS tables.
   * console.log(db2.selectFrom('userNames').selectAll().execute());
   * ```
   */
  public async createTablesAsSelect<T extends Record<string, SelectQueryBuilder<DB, keyof DB, unknown>>>(
    tables: T,
  ): Promise<Kysely<DB & { [K in keyof T]: CompiledQuerySchema<T[K]>; }>> {
    const tableNames = Object.keys(tables) as (keyof T)[];

    for (const tableName of tableNames) {
      const table = tables[tableName].compile();
      const query = CompiledQuery.raw(
        `CREATE TABLE ${String(tableName)} AS (${table.sql})`,
        [...table.parameters],
      );
      await this.executeQuery(query);
    }

    return this as Kysely<DB & { [K in keyof T]: CompiledQuerySchema<T[K]>; }>;
  }
}
