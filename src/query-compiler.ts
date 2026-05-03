import { DefaultQueryCompiler, TableNode } from "kysely";

const ID_WRAP_REGEX = /"/g;

export interface DuckDbQueryCompilerConfigs {
  /**
   * Mappings of table name in kysely to duckdb table expressions.
   *
   * Duckdb can read external source(file, url or database) as table
   * like: `SELECT * FROM read_json_objects('path/to/file/*.json')`.
   * You can use raw duckdb table expression as table name, but it may be too
   * long, preserving too many implementation details.
   *
   * This mappings is used to replace table name string to duckdb table expression.
   *
   * Keys can be plain table names or schema-qualified names (e.g., "schema.table").
   * - Plain table names match only when no schema is specified in the query.
   * - Schema-qualified keys match when using `.withSchema("schema").selectFrom("table")`.
   * - When a schema is specified but not found in any mapping key, the table is
   *   resolved normally (useful for attached databases like Postgres via ATTACH).
   *
   * @example
   * ```ts
   * const dialect = new DuckDbDialect({
   *  database: db,
   *  tableMappings: {
   *    // Matches: db.selectFrom("person")
   *    person: 'read_json_object("s3://my-bucket/person.json")',
   *    // Matches: db.withSchema("archive").selectFrom("person")
   *    "archive.person": 'read_parquet("s3://my-bucket/archive/person.parquet")',
   *  }
   * });
   *
   * const db = new Kysely<Database>({ dialect });
   *
   * // Uses the "person" mapping (reads from JSON)
   * await db.selectFrom("person").selectAll().execute();
   *
   * // Uses the "archive.person" mapping (reads from Parquet)
   * await db.withSchema("archive").selectFrom("person").selectAll().execute();
   *
   * // No mapping for "neon.person", queries the attached database directly
   * await db.withSchema("neon").selectFrom("person").selectAll().execute();
   * ```
   */
  tableMappings: {
    [tableName: string]: string;
  };
}

export class DuckDbQueryCompiler extends DefaultQueryCompiler {
  #configs: DuckDbQueryCompilerConfigs;

  constructor(configs: DuckDbQueryCompilerConfigs) {
    super();
    this.#configs = configs;
  }

  protected override getCurrentParameterPlaceholder() {
    return "?";
  }

  protected override getLeftExplainOptionsWrapper(): string {
    return "";
  }

  protected override getRightExplainOptionsWrapper(): string {
    return "";
  }

  protected override getLeftIdentifierWrapper(): string {
    return '"';
  }

  protected override getRightIdentifierWrapper(): string {
    return '"';
  }

  protected override getAutoIncrement(): string {
    throw new Error("Can not use auto increment in DuckDB");
  }

  protected override sanitizeIdentifier(identifier: string): string {
    return identifier.replace(ID_WRAP_REGEX, '""');
  }

  protected visitTable(node: TableNode): void {
    const mappings = this.#configs.tableMappings;
    const table = node.table.identifier.name;
    const schema = node.table.schema?.name;

    if (schema) {
      // Schema is specified, try schema-qualified key first
      const key = `${schema}.${table}`;
      if (Object.hasOwn(mappings, key)) {
        this.append(mappings[key]);
        return;
      }

      // Schema specified but no matching schema-qualified key found,
      // skip tableMappings and use normal table resolution
      super.visitTable(node);
    } else {
      // No schema specified, use plain table name mapping
      if (Object.hasOwn(mappings, table)) {
        this.append(mappings[table]);
      } else {
        super.visitTable(node);
      }
    }
  }
}
