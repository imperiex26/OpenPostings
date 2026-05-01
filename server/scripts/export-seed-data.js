/**
 * One-time export script: dumps seed/reference tables from jobs.db into CSV files.
 *
 * Usage:
 *   node server/scripts/export-seed-data.js
 *
 * Reads DB_PATH from env (defaults to ../jobs.db relative to repo root).
 * Writes CSV files to server/data/.
 */

const path = require("path");
const fs = require("fs");
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");

const DB_PATH = process.env.DB_PATH || path.resolve(__dirname, "..", "..", "jobs.db");
const DATA_DIR = path.resolve(__dirname, "..", "data");

function escapeCsvField(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\r")) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function toCsvRow(fields) {
  return fields.map(escapeCsvField).join(",");
}

async function exportTable(db, tableName, columns, outputFile) {
  const rows = await db.all(`SELECT ${columns.join(", ")} FROM ${tableName} ORDER BY id ASC;`);
  const header = toCsvRow(columns);
  const lines = [header];
  for (const row of rows) {
    lines.push(toCsvRow(columns.map((col) => row[col])));
  }
  fs.writeFileSync(outputFile, lines.join("\n") + "\n", "utf8");
  console.log(`Exported ${rows.length} rows from ${tableName} -> ${path.basename(outputFile)}`);
}

async function main() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  console.log(`Opening database: ${DB_PATH}`);
  const db = await open({ filename: DB_PATH, driver: sqlite3.Database });

  // 1. companies (MUST include id for FK stability with applications.company_id)
  await exportTable(
    db,
    "companies",
    ["id", "company_name", "url_string", "ATS_name"],
    path.join(DATA_DIR, "companies.csv")
  );

  // 2. job_industry_categories
  await exportTable(
    db,
    "job_industry_categories",
    ["id", "industry_key", "industry_label", "priority"],
    path.join(DATA_DIR, "job_industry_categories.csv")
  );

  // 3. job_position_industry
  await exportTable(
    db,
    "job_position_industry",
    [
      "id", "job_title", "normalized_job_title", "industry_key", "industry_label",
      "matched_rules", "confidence_score", "rule_version"
    ],
    path.join(DATA_DIR, "job_position_industry.csv")
  );

  // 4. state_location_index
  await exportTable(
    db,
    "state_location_index",
    [
      "id", "location_type", "state_usps", "state_geoid", "location_geoid",
      "ansicode", "location_name", "search_location_name",
      "normalized_location_name", "normalized_search_location_name",
      "lsad_code", "funcstat", "aland", "awater", "aland_sqmi", "awater_sqmi",
      "intptlat", "intptlong", "source_file"
    ],
    path.join(DATA_DIR, "state_location_index.csv")
  );

  await db.close();
  console.log("\nDone. CSV files written to:", DATA_DIR);
}

main().catch((err) => {
  console.error("Export failed:", err);
  process.exit(1);
});
