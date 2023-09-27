/** @type {import('ts-jest').JestConfigWithTsJest} */
export default {
  transform: {
    '^.+\\.m?[tj]sx?$': ['ts-jest', {
      diagnostics: false,
    }],
  },
  preset: 'ts-jest/presets/default-esm',
  testEnvironment: 'node',
};