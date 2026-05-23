// Preflight validation before import example.
//
// Covered methods: validateCSV, validateJSONL, importCSV, clear.

import { check } from "k6";
import { openKv } from "k6/x/kv";

// Validation APIs are KV-instance methods for API consistency and metrics collection.
// They require openKv(), but do not read/write KV store contents.
const kv = openKv({
  backend: "memory",
  serialization: "json",
  trackKeys: true,
});

export const options = {
  vus: 1,
  iterations: 1,
};

export default async function () {
  // Syntax mode: checks CSV readability/header shape only.
  const csvSyntaxResult = await kv.validateCSV({
    fileName: "./examples/fixtures/users.csv",
    hasHeader: true,
  });

  // Import-shape mode: also validates key extraction rules used by importCSV().
  const csvImportShapeResult = await kv.validateCSV({
    fileName: "./examples/fixtures/users.csv",
    keyColumn: "id",
    hasHeader: true,
  });

  // Contract: valid=true means full-file validity only when checkedAll=true.
  const csvPrefixResult = await kv.validateCSV({
    fileName: "./examples/fixtures/users.csv",
    keyColumn: "id",
    hasHeader: true,
    limit: 1,
  });

  const jsonlResult = await kv.validateJSONL({
    fileName: "./examples/fixtures/users.jsonl",
  });

  // Prefix validation mode: checkedAll=false means valid=true applies to inspected prefix only.
  const jsonlPrefixResult = await kv.validateJSONL({
    fileName: "./examples/fixtures/users.jsonl",
    limit: 1,
  });

  // Empty files are valid preflight input, so assert rows/records > 0 when non-empty fixtures are required.
  check(csvSyntaxResult, {
    "validateCSV syntax mode is valid": (r) => r.valid === true && r.rows > 0 && r.checkedAll === true,
  });

  check(csvImportShapeResult, {
    "validateCSV import-shape mode is valid": (r) => r.valid === true && r.rows > 0 && r.checkedAll === true,
  });

  check(csvPrefixResult, {
    "validateCSV limit validates inspected prefix": (r) =>
      r.valid === true && r.rows === 1 && r.checkedAll === false,
  });

  check(jsonlResult, {
    "validateJSONL result is valid": (r) => r.valid === true && r.records > 0 && r.checkedAll === true,
  });

  check(jsonlPrefixResult, {
    "validateJSONL limit validates inspected prefix": (r) =>
      r.valid === true && r.records === 1 && r.checkedAll === false,
  });

  if (
    !csvSyntaxResult.valid ||
    !csvImportShapeResult.valid ||
    !csvPrefixResult.valid ||
    !jsonlResult.valid ||
    !jsonlPrefixResult.valid
  ) {
    throw new Error(
      "validation failed: " +
      `csvSyntax=${JSON.stringify(csvSyntaxResult)} ` +
      `csvImportShape=${JSON.stringify(csvImportShapeResult)} ` +
      `csvPrefix=${JSON.stringify(csvPrefixResult)} ` +
      `jsonl=${JSON.stringify(jsonlResult)} ` +
      `jsonlPrefix=${JSON.stringify(jsonlPrefixResult)}`
    );
  }

  await kv.clear();
  const importResult = await kv.importCSV({
    fileName: "./examples/fixtures/users.csv",
    keyColumn: "id",
    hasHeader: true,
  });

  check(importResult, {
    "importCSV imports validated rows": (r) => r.imported > 0,
  });
}

export function teardown() {
  kv.close();
}
