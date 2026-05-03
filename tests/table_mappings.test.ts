import { DuckDBInstance } from "@duckdb/node-api";
import { Kysely } from "kysely";
import { DuckDbDialect } from "../src/index";

interface TestDatabase {
  users: { id: number; name: string };
  orders: { id: number; user_id: number };
}

const createTestDb = async (
  tableMappings: Record<string, string>,
): Promise<Kysely<TestDatabase>> => {
  const db = await DuckDBInstance.create(":memory:");
  const dialect = new DuckDbDialect({ database: db, tableMappings });
  return new Kysely<TestDatabase>({ dialect });
};

describe("tableMappings schema awareness", () => {
  describe("no schema specified in query", () => {
    test("uses mapping when table name matches", async () => {
      const kysely = await createTestDb({
        users: "read_parquet('s3://bucket/users.parquet')",
      });

      const query = kysely.selectFrom("users").selectAll().compile();
      expect(query.sql).toBe(
        `select * from read_parquet('s3://bucket/users.parquet')`,
      );
    });

    test("uses normal table when no mapping exists", async () => {
      const kysely = await createTestDb({});

      const query = kysely.selectFrom("users").selectAll().compile();
      expect(query.sql).toBe(`select * from "users"`);
    });
  });

  describe("schema specified in query (.withSchema)", () => {
    test("uses schema-qualified mapping when it exists", async () => {
      const kysely = await createTestDb({
        users: "read_parquet('s3://bucket/users.parquet')",
        "archive.users": "read_parquet('s3://bucket/archive/users.parquet')",
      });

      // Without schema - uses default mapping
      const defaultQuery = kysely.selectFrom("users").selectAll().compile();
      expect(defaultQuery.sql).toBe(
        `select * from read_parquet('s3://bucket/users.parquet')`,
      );

      // With schema - uses archive mapping
      const archiveQuery = kysely
        .withSchema("archive")
        .selectFrom("users")
        .selectAll()
        .compile();
      expect(archiveQuery.sql).toBe(
        `select * from read_parquet('s3://bucket/archive/users.parquet')`,
      );
    });

    test("bypasses mappings when schema not in any mapping key", async () => {
      const kysely = await createTestDb({
        users: "read_parquet('s3://bucket/users.parquet')",
        "archive.users": "read_parquet('s3://bucket/archive/users.parquet')",
      });

      // Query with 'external' schema - not in mappings, should use real table
      const query = kysely
        .withSchema("external")
        .selectFrom("users")
        .selectAll()
        .compile();
      expect(query.sql).toBe(`select * from "external"."users"`);
    });

    test("bypasses mappings when schema matches but table does not", async () => {
      const kysely = await createTestDb({
        "myschema.users": "read_parquet('s3://bucket/users.parquet')",
      });

      // Query orders table in myschema - no mapping for "myschema.orders"
      const query = kysely
        .withSchema("myschema")
        .selectFrom("orders")
        .selectAll()
        .compile();
      expect(query.sql).toBe(`select * from "myschema"."orders"`);
    });
  });

  describe("multiple schemas with different mappings", () => {
    test("correctly routes to different mappings based on schema", async () => {
      const kysely = await createTestDb({
        users: "read_parquet('s3://default/users.parquet')",
        "prod.users": "read_parquet('s3://prod/users.parquet')",
        "staging.users": "read_parquet('s3://staging/users.parquet')",
      });

      const defaultQuery = kysely.selectFrom("users").selectAll().compile();
      expect(defaultQuery.sql).toBe(
        `select * from read_parquet('s3://default/users.parquet')`,
      );

      const prodQuery = kysely
        .withSchema("prod")
        .selectFrom("users")
        .selectAll()
        .compile();
      expect(prodQuery.sql).toBe(
        `select * from read_parquet('s3://prod/users.parquet')`,
      );

      const stagingQuery = kysely
        .withSchema("staging")
        .selectFrom("users")
        .selectAll()
        .compile();
      expect(stagingQuery.sql).toBe(
        `select * from read_parquet('s3://staging/users.parquet')`,
      );
    });
  });

  describe("edge cases", () => {
    test("schema-only mapping with no default mapping", async () => {
      const kysely = await createTestDb({
        "special.users": "read_parquet('s3://special/users.parquet')",
      });

      // Without schema - no mapping, uses real table
      const noSchemaQuery = kysely.selectFrom("users").selectAll().compile();
      expect(noSchemaQuery.sql).toBe(`select * from "users"`);

      // With schema - uses mapping
      const schemaQuery = kysely
        .withSchema("special")
        .selectFrom("users")
        .selectAll()
        .compile();
      expect(schemaQuery.sql).toBe(
        `select * from read_parquet('s3://special/users.parquet')`,
      );
    });

    test("mixed schema and non-schema mappings for different tables", async () => {
      const kysely = await createTestDb({
        users: "read_parquet('s3://bucket/users.parquet')",
        "data.orders": "read_parquet('s3://bucket/orders.parquet')",
      });

      // users without schema - uses mapping
      const usersQuery = kysely.selectFrom("users").selectAll().compile();
      expect(usersQuery.sql).toBe(
        `select * from read_parquet('s3://bucket/users.parquet')`,
      );

      // orders with data schema - uses mapping
      const ordersQuery = kysely
        .withSchema("data")
        .selectFrom("orders")
        .selectAll()
        .compile();
      expect(ordersQuery.sql).toBe(
        `select * from read_parquet('s3://bucket/orders.parquet')`,
      );
    });
  });
});
