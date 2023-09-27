import { type RawBuilder, sql } from "kysely";

export type DuckDBNodeDataTypes = {
  BIT: string;
  BLOB: Buffer;
  BOOLEAN: boolean;
  DATE: Date;
  ENUM: string;
  INTERVAL: { months: number; days: number; micros: number; };

  TINYINT: number;
  INT1: number;
  SMALLINT: number;
  INT2: number;
  SHORT: number;
  INTEGER: number;
  INT4: number;
  INT: number;
  SIGNED: number;
  BIGINT: number;
  INT8: number;
  LONG: number;
  HUGEINT: number;
  UTINYINT: number;
  USMALLINT: number;
  UINTEGER: number;
  UBIGINT: number;

  VARCHAR: string;
  CHAR: string;
  BPCHAR: string;
  TEXT: string;
  STRING: string;

  MAP: string;

  // duckdb 0.8.1
  TIMESTAMP: Date;
  DATETIME: Date;
  TIMESTAMPTZ: Date;
  TIMESTAMP_WITH_TIME_ZONE: Date;
};

// constructors
export const bit = (value: string): RawBuilder<string> => sql`${value}::BIT`;
export const blob = (buf: Buffer): RawBuilder<Buffer> => {
  const byteStr: string[] = [];
  for (const [_, c] of buf.entries()) {
    byteStr.push(`\\x${c.toString(16)}`);
  }
  return sql`${byteStr.join("")}::BLOB`;
};
export const date = (date: Date): RawBuilder<Date> => sql`${date.toISOString().substring(0, 10)}::DATE`;
// const interval = ...
export const list = <T>(values: T[]): RawBuilder<any[]> => sql`list_value(${sql.join(values)})`;
export const map = <K, V>(values: [K, V][]): RawBuilder<any> => {
  const toEntry = ([k, v]: [K, V]) => sql`(${k}, ${v})`;
  return sql`map_from_entries([${sql.join(values.map(toEntry))}])`;
};
export const struct = (values: Record<string, RawBuilder<any>>): RawBuilder<any> => {
  Object.keys(values).forEach(k => {
    if (k.includes("'") || k.includes("{") || k.includes("}")) {
      throw new Error(`Invalid Struct key found: ${k}`);
    }
  });
  return sql`{${sql.join(Object.entries(values).map(([key, value]) => sql`${sql.lit(key)}: ${value}`))}}`;
};
export const timestamp = <T extends Date | string>(value: T): RawBuilder<T> => {
  if (typeof value === "string") {
    return sql`${value}::TIMESTAMP`;
  } else {
    return sql`${value.toISOString().slice(0, -1)}::TIMESTAMP`;
  }
};

export const timestamptz = <T extends Date | string>(value: T): RawBuilder<Date> => {
  if (typeof value === "string") {
    return sql`${value}::TIMESTAMPTZ`;
  } else {
    return sql`${value.toISOString()}::TIMESTAMPTZ`;
  }
};
