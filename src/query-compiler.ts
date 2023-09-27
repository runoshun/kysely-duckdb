import { DefaultQueryCompiler, TableNode, TupleNode, ValueListNode, WhereNode } from "kysely";

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
   * @example
   * ```ts
   * const dialect = new DuckDbDialect({
   *  database: db,
   *  tableMappings: {
   *    person: 'read_json_object("s3://my-bucket/person.json?s3_access_key_id=key&s3_secret_access_key=secret")'
   *  }
   * });
   *
   * const db = new Kysely<{
   *   person: { first_name: string, last_name: string }, // 'person' is defined in tableMappings
   *   pet: { name: string, species: 'cat' | 'dog' },     // 'pet' is *not* defined in tableMappings
   * >({ dialect });
   *
   * await db.selectFrom("person").selectAll().execute();
   * // => Executed query is: `SELECT * FROM read_json_object("s3://my-bucket/person.json?s3_access_key_id=key&s3_secret_access_key=secret");`
   * ```
   *
   * await db.selectFrom("pet").selectAll().execute();
   * // => Executed query is: `SELECT * FROM pet;`
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
    return "\"";
  }

  protected override getRightIdentifierWrapper(): string {
    return "\"";
  }

  protected override getAutoIncrement(): string {
    throw new Error("Can not use auto increment in DuckDB");
  }

  protected override sanitizeIdentifier(identifier: string): string {
    return identifier.replace(ID_WRAP_REGEX, "\"\"");
  }

  protected visitTable(node: TableNode): void {
    if (Object.hasOwn(this.#configs.tableMappings, node.table.identifier.name)) {
      this.append(this.#configs.tableMappings[node.table.identifier.name]);
    } else {
      super.visitTable(node);
    }
  }
}
