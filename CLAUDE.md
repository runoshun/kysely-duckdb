# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Kysely dialect implementation for DuckDB WebAssembly. It provides a type-safe SQL query builder interface for DuckDB's WebAssembly version, enabling SQL queries in browser and Node.js environments.

## Development Commands

```bash
# Install dependencies (using pnpm)
pnpm install

# Run tests
pnpm test

# Build the project (outputs to dist/)
pnpm run build

# Type check without emitting
pnpm run check

# Run all checks (type check, tests, build, docs)
pnpm run all

# Generate documentation
pnpm run docs
```

## Architecture

The codebase follows the Kysely dialect pattern with four main components:

1. **DuckDbWasmDriver** (`src/driver-wasm.ts`): Manages connections to DuckDB WebAssembly instances
2. **DuckDbAdapter** (`src/adapter.ts`): Provides DuckDB-specific adaptations for Kysely
3. **DuckDbQueryCompiler** (`src/query-compiler.ts`): Compiles Kysely query AST to DuckDB SQL
4. **DuckDbIntrospector** (`src/introspector.ts`): Introspects database schema for type generation

The main entry point is `src/index.ts` which exports the `DuckDbDialect` class.

## Key Features & Patterns

### Table Mappings

The dialect supports mapping external data sources (JSON, CSV files) as tables through the `tableMappings` configuration option. See `tests/select.test.ts` for examples.

### DuckDB-Specific Data Types

Helper functions for DuckDB's advanced types are in `src/helper/datatypes.ts`:
- Arrays: `duckArray()`, `duckArrayAggregate()`
- Structs: `duckStruct()`, `duckRowStruct()`
- Maps: `duckMap()`, `duckMapFromEntries()`
- Unions: `duckUnion()`

### Testing Pattern

Tests use Vitest and follow a pattern of:
1. Creating a DuckDB instance with test data
2. Setting up Kysely with the dialect
3. Running queries and asserting results

See `tests/test_common.ts` for shared test utilities.

## Common Development Tasks

### Adding New DuckDB Functions

1. Add the function to `src/helper/functions.ts`
2. Follow the existing pattern using `sql` template literals
3. Add tests in the appropriate test file

### Modifying Query Compilation

1. Edit `src/query-compiler.ts`
2. Override the appropriate visit method from the base `DefaultQueryCompiler`
3. Test with complex queries to ensure correctness

### Working with WebAssembly Driver

The driver implementation in `src/driver-wasm.ts` handles:
- Connection lifecycle
- Query execution
- Result transformation from Arrow format

When modifying driver behavior, ensure compatibility with both browser and Node.js environments.
