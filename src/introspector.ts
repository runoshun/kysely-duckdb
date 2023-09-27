import { DEFAULT_MIGRATION_LOCK_TABLE, DEFAULT_MIGRATION_TABLE, Kysely, sql } from "kysely";
import type {
  DatabaseIntrospector,
  DatabaseMetadata,
  DatabaseMetadataOptions,
  SchemaMetadata,
  TableMetadata,
} from "kysely";

export class DuckDbIntrospector implements DatabaseIntrospector {
  readonly #db: Kysely<any>;

  constructor(db: Kysely<any>) {
    this.#db = db;
  }

  async getSchemas(): Promise<SchemaMetadata[]> {
    let rawSchemas = await this.#db
      .selectFrom("information_schema.schemata")
      .select("schema_name")
      .$castTo<RawSchemaMetadata>()
      .execute();

    return rawSchemas.map((it) => ({ name: it.SCHEMA_NAME }));
  }

  async getTables(
    options: DatabaseMetadataOptions = { withInternalKyselyTables: false },
  ): Promise<TableMetadata[]> {
    let query = this.#db
      .selectFrom("information_schema.columns as columns")
      .innerJoin("information_schema.tables as tables", (b) =>
        b
          .onRef("columns.TABLE_CATALOG", "=", "tables.TABLE_CATALOG")
          .onRef("columns.TABLE_SCHEMA", "=", "tables.TABLE_SCHEMA")
          .onRef("columns.TABLE_NAME", "=", "tables.TABLE_NAME"))
      .select([
        "columns.COLUMN_NAME",
        "columns.COLUMN_DEFAULT",
        "columns.TABLE_NAME",
        "columns.TABLE_SCHEMA",
        "tables.TABLE_TYPE",
        "columns.IS_NULLABLE",
        "columns.DATA_TYPE",
      ])
      .where("columns.TABLE_SCHEMA", "=", sql`current_schema()`)
      .orderBy("columns.TABLE_NAME")
      .orderBy("columns.ORDINAL_POSITION")
      .$castTo<RawColumnMetadata>();

    if (!options.withInternalKyselyTables) {
      query = query
        .where("columns.TABLE_NAME", "!=", DEFAULT_MIGRATION_TABLE)
        .where("columns.TABLE_NAME", "!=", DEFAULT_MIGRATION_LOCK_TABLE);
    }

    console.log(query.compile());
    const rawColumns = await query.execute();
    return this.#parseTableMetadata(rawColumns);
  }

  async getMetadata(
    options?: DatabaseMetadataOptions,
  ): Promise<DatabaseMetadata> {
    return {
      tables: await this.getTables(options),
    };
  }

  #parseTableMetadata(columns: RawColumnMetadata[]): TableMetadata[] {
    console.log(columns);
    return columns.reduce<TableMetadata[]>((tables, it) => {
      let table = tables.find((tbl) => tbl.name === it.table_name);

      if (!table) {
        table = Object.freeze({
          name: it.table_name,
          isView: it.table_type === "view",
          schema: it.table_schema,
          columns: [],
        });

        tables.push(table);
      }

      table.columns.push(
        Object.freeze({
          name: it.column_name,
          dataType: it.data_type,
          isNullable: it.is_nullable === "YES",
          isAutoIncrementing: false,
          hasDefaultValue: it.column_default !== null,
        }),
      );

      return tables;
    }, []);
  }
}

interface RawSchemaMetadata {
  SCHEMA_NAME: string;
}

interface RawColumnMetadata {
  column_name: string;
  column_default: any;
  table_name: string;
  table_schema: string;
  table_type: string;
  is_nullable: "YES" | "NO";
  data_type: string;
  extra: string;
}
