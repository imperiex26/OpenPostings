const path = require("path");
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");
const { McpServer } = require("@modelcontextprotocol/sdk/server/mcp.js");
const { StdioServerTransport } = require("@modelcontextprotocol/sdk/server/stdio.js");
const z = require("zod");

const ROOT_DIR = path.resolve(__dirname, "..", "..");
const DB_PATH = process.env.DB_PATH || path.resolve(__dirname, "..", "jobs.db");

const GENERIC_TITLE_LIKE_PARTS = new Set([
  "and",
  "for",
  "with",
  "from",
  "the",
  "manager",
  "assistant",
  "associate",
  "specialist",
  "coordinator",
  "director",
  "officer",
  "analyst",
  "consultant",
  "lead",
  "senior",
  "junior",
  "staff",
  "team",
  "services",
  "service",
  "operations",
  "operation",
  "support"
]);
const WEAK_INDUSTRY_LIKE_PARTS = new Set([
  ...GENERIC_TITLE_LIKE_PARTS,
  "account",
  "accounts",
  "representative",
  "executive",
  "management",
  "area",
  "group",
  "international",
  "care",
  "inside",
  "outside",
  "hourly",
  "commission",
  "anywhere",
  "can",
  "small",
  "planning",
  "compliance",
  "core",
  "safety",
  "import",
  "export",
  "brand",
  "ambassador",
  "customer",
  "business",
  "field",
  "division",
  "product"
]);
const IT_SOFTWARE_INDUSTRY_KEY = "information_technology_software";
const SALES_BUSINESS_INDUSTRY_KEY = "sales_business_development";
const IT_TECH_ANCHOR_PARTS = new Set([
  "software",
  "developer",
  "development",
  "engineer",
  "engineering",
  "devops",
  "platform",
  "cloud",
  "security",
  "cybersecurity",
  "cyber",
  "infrastructure",
  "network",
  "systems",
  "system",
  "administrator",
  "database",
  "sql",
  "data",
  "analytics",
  "architect",
  "automation",
  "backend",
  "frontend",
  "fullstack",
  "application",
  "applications",
  "qa",
  "test",
  "testing",
  "machine",
  "learning",
  "mlops",
  "ai"
]);
const IT_HIGH_SIGNAL_ANCHOR_PARTS = new Set([
  "software",
  "developer",
  "development",
  "engineer",
  "engineering",
  "devops",
  "platform",
  "cloud",
  "security",
  "cybersecurity",
  "cyber",
  "infrastructure",
  "network",
  "systems",
  "system",
  "administrator",
  "database",
  "sql",
  "architect",
  "automation",
  "backend",
  "frontend",
  "fullstack",
  "mlops",
  "machine",
  "learning",
  "ai"
]);
const IT_SALES_GTM_ROLE_REGEX =
  /\b(account executive|account manager|business development|brand ambassador|go[\s-]?to[\s-]?market|gtm|inside sales|outside sales|sales representative|territory manager|partnerships?|sales(?!force\b))\b/i;
const SALES_EXCLUSIVE_ROLE_REGEX =
  /\b(account executive|account manager|business development|brand ambassador|inside sales|outside sales|sales representative|sales manager|sales director|sales consultant|sales specialist|sales associate|sales advisor|presales?|telesales|territory manager|channel sales|partner sales|salesperson|salesman|salesworker|sales(?!force\b))\b/i;

const STATE_CODE_TO_NAME = {
  AL: "alabama",
  AK: "alaska",
  AZ: "arizona",
  AR: "arkansas",
  CA: "california",
  CO: "colorado",
  CT: "connecticut",
  DE: "delaware",
  FL: "florida",
  GA: "georgia",
  HI: "hawaii",
  ID: "idaho",
  IL: "illinois",
  IN: "indiana",
  IA: "iowa",
  KS: "kansas",
  KY: "kentucky",
  LA: "louisiana",
  ME: "maine",
  MD: "maryland",
  MA: "massachusetts",
  MI: "michigan",
  MN: "minnesota",
  MS: "mississippi",
  MO: "missouri",
  MT: "montana",
  NE: "nebraska",
  NV: "nevada",
  NH: "new hampshire",
  NJ: "new jersey",
  NM: "new mexico",
  NY: "new york",
  NC: "north carolina",
  ND: "north dakota",
  OH: "ohio",
  OK: "oklahoma",
  OR: "oregon",
  PA: "pennsylvania",
  RI: "rhode island",
  SC: "south carolina",
  SD: "south dakota",
  TN: "tennessee",
  TX: "texas",
  UT: "utah",
  VT: "vermont",
  VA: "virginia",
  WA: "washington",
  WV: "west virginia",
  WI: "wisconsin",
  WY: "wyoming",
  DC: "district of columbia"
};

const MCP_SETTINGS_DEFAULTS = {
  enabled: false,
  preferred_agent_name: "OpenPostings Agent",
  agent_login_email: "",
  agent_login_password: "",
  mfa_login_email: "",
  mfa_login_notes: "",
  dry_run_only: true,
  require_final_approval: true,
  max_applications_per_run: 10,
  preferred_search: "",
  preferred_remote: "all",
  preferred_industries: [],
  preferred_states: [],
  preferred_counties: [],
  instructions_for_agent: ""
};
const ATS_FILTER_OPTIONS = new Set([
  "workday",
  "ashby",
  "greenhouse",
  "lever",
  "jobvite",
  "applicantpro",
  "applytojob",
  "theapplicantmanager",
  "recruitee",
  "ultipro",
  "taleo"
]);
const PHRASE_NGRAM_INDUSTRY_COVERAGE_THRESHOLD = 2;
const FALLBACK_WORD_INDUSTRY_COVERAGE_THRESHOLD = 2;
const MIN_INDUSTRY_FALLBACK_WORD_COUNT = 3;
const MIN_INDUSTRY_PHRASE_NGRAM_COUNT = 2;

let db;
let wordIndustryCoverageCache = null;
let phraseNgramIndustryCoverageCache = null;

function nowEpochSeconds() {
  return Math.floor(Date.now() / 1000);
}

function normalizeLikeText(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function parseNonNegativeInteger(value) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function normalizeBoolean(value, defaultValue = false) {
  if (typeof value === "boolean") return value;
  const normalized = normalizeLikeText(value);
  if (!normalized) return Boolean(defaultValue);
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function normalizeRemoteFilter(value) {
  const normalized = normalizeLikeText(value);
  if (normalized === "remote" || normalized === "non_remote") return normalized;
  return "all";
}

function ensureMcpAgentEnabled(settings) {
  if (normalizeBoolean(settings?.enabled, false)) return;
  throw new Error("MCP application agent is disabled in settings.");
}

function inferAtsFromJobPostingUrl(value) {
  const url = String(value || "").trim().toLowerCase();
  if (!url) return "";
  if (url.includes("myworkdayjobs.com")) return "workday";
  if (url.includes("jobs.ashbyhq.com")) return "ashby";
  if (url.includes("job-boards.greenhouse.io") || url.includes("boards.greenhouse.io")) return "greenhouse";
  if (url.includes("jobs.lever.co")) return "lever";
  if ((url.includes("jobs.jobvite.com/") || url.includes("careers.jobvite.com/")) && url.includes("/job/")) return "jobvite";
  if (url.includes(".applicantpro.com/jobs")) return "applicantpro";
  if (url.includes(".applytojob.com/apply")) return "applytojob";
  if (url.includes("theapplicantmanager.com/jobs")) return "theapplicantmanager";
  if (url.includes(".recruitee.com")) return "recruitee";
  if (url.includes("recruiting.ultipro.com/") && url.includes("/jobboard/")) return "ultipro";
  if (url.includes(".taleo.net/careersection/")) return "taleo";
  return "";
}

function normalizeAtsFilters(value) {
  const rawItems = Array.isArray(value) ? value : value ? [value] : [];
  const items = rawItems
    .map((item) => {
      const normalized = normalizeLikeText(item);
      if (normalized === "ashbyhq") return "ashby";
      if (normalized === "greenhouseio" || normalized === "greenhouse.io") return "greenhouse";
      if (normalized === "leverco" || normalized === "lever.co") return "lever";
      if (normalized === "jobvitecom" || normalized === "jobvite.com") return "jobvite";
      if (normalized === "applicantprocom" || normalized === "applicantpro.com") return "applicantpro";
      if (normalized === "applytojobcom" || normalized === "applytojob.com") return "applytojob";
      if (normalized === "theapplicantmanagercom" || normalized === "theapplicantmanager.com") {
        return "theapplicantmanager";
      }
      if (normalized === "recruiteecom" || normalized === "recruitee.com") return "recruitee";
      if (normalized === "ukg") return "ultipro";
      if (normalized === "taleonet" || normalized === "taleo.net") return "taleo";
      return normalized;
    })
    .filter((item) => ATS_FILTER_OPTIONS.has(item));
  return Array.from(new Set(items));
}

function normalizeCountyName(value) {
  return normalizeLikeText(value)
    .replace(/\b(county|parish|borough|census area|municipality)\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function parseCountyFilters(values) {
  const parsed = [];
  for (const rawValue of values || []) {
    const value = String(rawValue || "").trim();
    if (!value) continue;

    if (value.includes("|")) {
      const [stateRaw, countyRaw] = value.split("|");
      const stateCode = String(stateRaw || "").trim().toUpperCase();
      const countyLikePart = normalizeCountyName(countyRaw);
      if (!countyLikePart) continue;
      parsed.push({ stateCode, countyLikePart });
      continue;
    }

    const countyLikePart = normalizeCountyName(value);
    if (!countyLikePart) continue;
    parsed.push({ stateCode: "", countyLikePart });
  }
  return parsed;
}

function createLikeParts(value) {
  const normalized = normalizeLikeText(value);
  if (!normalized) return [];
  return normalized
    .split(/[^a-z0-9]+/)
    .map((part) => part.trim())
    .filter((part) => part.length >= 3 && !GENERIC_TITLE_LIKE_PARTS.has(part));
}

function buildWordNgrams(words, minSize = 2, maxSize = 3) {
  const source = Array.isArray(words) ? words : [];
  const ngrams = [];
  for (let size = minSize; size <= maxSize; size += 1) {
    if (source.length < size) continue;
    for (let index = 0; index <= source.length - size; index += 1) {
      const gram = source.slice(index, index + size).join(" ").trim();
      if (gram) ngrams.push(gram);
    }
  }
  return ngrams;
}

function hasStateLikeMatch(locationText, stateCode) {
  const code = String(stateCode || "").trim().toUpperCase();
  if (!code) return false;

  const upperLocation = String(locationText || "").toUpperCase();
  const codeRegex = new RegExp(`(^|[^A-Z])${code}([^A-Z]|$)`);
  if (codeRegex.test(upperLocation)) return true;

  const stateName = STATE_CODE_TO_NAME[code];
  if (!stateName) return false;
  return normalizeLikeText(locationText).includes(stateName);
}

function rowMatchesLocationFilters(locationText, selectedStateCodes, countyFilters) {
  const stateCodes = Array.isArray(selectedStateCodes) ? selectedStateCodes : [];
  const counties = Array.isArray(countyFilters) ? countyFilters : [];
  if (stateCodes.length === 0 && counties.length === 0) return true;

  const location = String(locationText || "").trim();
  if (!location) return false;
  const normalizedLocation = normalizeLikeText(location);

  if (stateCodes.length > 0) {
    const hasSelectedState = stateCodes.some((stateCode) => hasStateLikeMatch(location, stateCode));
    if (!hasSelectedState) return false;
  }

  if (counties.length > 0) {
    const matchesCounty = counties.some((countyFilter) => {
      const countyLikePart = String(countyFilter?.countyLikePart || "").trim();
      if (!countyLikePart) return false;

      if (countyFilter.stateCode && !hasStateLikeMatch(location, countyFilter.stateCode)) {
        return false;
      }

      return (
        normalizedLocation.includes(countyLikePart) ||
        normalizedLocation.includes(`${countyLikePart} county`) ||
        normalizedLocation.includes(`${countyLikePart} parish`) ||
        normalizedLocation.includes(`${countyLikePart} borough`) ||
        normalizedLocation.includes(`${countyLikePart} census area`)
      );
    });

    if (!matchesCounty) return false;
  }

  return true;
}

function isRemoteLocation(locationText) {
  const normalized = normalizeLikeText(locationText);
  if (!normalized) return false;
  return (
    normalized.includes("remote") ||
    normalized.includes("work from home") ||
    normalized.includes("wfh") ||
    normalized.includes("hybrid")
  );
}

function rowMatchesRemoteFilter(locationText, remoteFilter) {
  const normalized = normalizeRemoteFilter(remoteFilter);
  if (normalized === "all") return true;
  const isRemote = isRemoteLocation(locationText);
  if (normalized === "remote") return isRemote;
  if (normalized === "non_remote") return !isRemote;
  return true;
}

function inferWorkdayLocationFromJobUrl(jobPostingUrl) {
  try {
    const parsed = new URL(String(jobPostingUrl || ""));
    const pathParts = parsed.pathname
      .split("/")
      .map((part) => String(part || "").trim())
      .filter(Boolean);
    const jobIndex = pathParts.findIndex((part) => part.toLowerCase() === "job");
    if (jobIndex >= 0 && pathParts[jobIndex + 1]) {
      return decodeURIComponent(pathParts[jobIndex + 1]).replace(/-/g, " ").replace(/\s+/g, " ").trim();
    }
    return "";
  } catch {
    return "";
  }
}

function inferLocationFromJobUrl(jobPostingUrl) {
  const url = String(jobPostingUrl || "").trim();
  if (!url) return "";
  try {
    const parsed = new URL(url);
    if (parsed.hostname.endsWith("myworkdayjobs.com")) {
      return inferWorkdayLocationFromJobUrl(url);
    }
    return "";
  } catch {
    return "";
  }
}

async function ensureTables() {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS PersonalInformation (
      first_name TEXT NOT NULL,
      middle_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      email TEXT NOT NULL,
      phone_number TEXT NOT NULL,
      address TEXT NOT NULL,
      linkedin_url TEXT NOT NULL,
      github_url TEXT NOT NULL,
      portfolio_url TEXT NOT NULL,
      resume_file_path TEXT NOT NULL,
      projects_portfolio_file_path TEXT NOT NULL,
      certifications_folder_path TEXT NOT NULL,
      ethnicity TEXT NOT NULL,
      gender TEXT NOT NULL,
      age INTEGER NOT NULL,
      veteran_status TEXT NOT NULL,
      disability_status TEXT NOT NULL,
      education_level TEXT NOT NULL,
      years_of_experience INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS applications (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      position_name TEXT NOT NULL,
      application_date INTEGER NOT NULL,
      status TEXT
    );

    CREATE TABLE IF NOT EXISTS McpSettings (
      id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
      enabled INTEGER NOT NULL DEFAULT 0,
      preferred_agent_name TEXT NOT NULL DEFAULT 'OpenPostings Agent',
      agent_login_email TEXT NOT NULL DEFAULT '',
      agent_login_password TEXT NOT NULL DEFAULT '',
      mfa_login_email TEXT NOT NULL DEFAULT '',
      mfa_login_notes TEXT NOT NULL DEFAULT '',
      dry_run_only INTEGER NOT NULL DEFAULT 1,
      require_final_approval INTEGER NOT NULL DEFAULT 1,
      max_applications_per_run INTEGER NOT NULL DEFAULT 10,
      preferred_search TEXT NOT NULL DEFAULT '',
      preferred_remote TEXT NOT NULL DEFAULT 'all',
      preferred_industries TEXT NOT NULL DEFAULT '[]',
      preferred_states TEXT NOT NULL DEFAULT '[]',
      preferred_counties TEXT NOT NULL DEFAULT '[]',
      instructions_for_agent TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS application_attribution (
      application_id INTEGER NOT NULL PRIMARY KEY,
      applied_by_type TEXT NOT NULL,
      applied_by_label TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS posting_application_state (
      job_posting_url TEXT NOT NULL PRIMARY KEY,
      applied INTEGER NOT NULL DEFAULT 0,
      applied_by_type TEXT NOT NULL,
      applied_by_label TEXT NOT NULL,
      applied_at_epoch INTEGER,
      last_application_id INTEGER,
      ignored INTEGER NOT NULL DEFAULT 0,
      ignored_at_epoch INTEGER,
      ignored_by_label TEXT NOT NULL DEFAULT '',
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    INSERT INTO McpSettings (
      id,
      enabled,
      preferred_agent_name,
      agent_login_email,
      mfa_login_email,
      mfa_login_notes,
      dry_run_only,
      require_final_approval,
      max_applications_per_run,
      preferred_search,
      preferred_remote,
      preferred_industries,
      preferred_states,
      preferred_counties,
      instructions_for_agent
    ) VALUES (1, 0, 'OpenPostings Agent', '', '', '', 1, 1, 10, '', 'all', '[]', '[]', '[]', '')
    ON CONFLICT(id) DO NOTHING;
  `);

  const postingStateColumns = await db.all(`PRAGMA table_info('posting_application_state');`);
  const postingStateColumnNames = new Set(postingStateColumns.map((column) => String(column?.name || "")));
  const mcpSettingsColumns = await db.all(`PRAGMA table_info('McpSettings');`);
  const mcpSettingsColumnNames = new Set(mcpSettingsColumns.map((column) => String(column?.name || "")));

  if (!postingStateColumnNames.has("ignored")) {
    await db.exec(`
      ALTER TABLE posting_application_state
      ADD COLUMN ignored INTEGER NOT NULL DEFAULT 0;
    `);
  }
  if (!postingStateColumnNames.has("ignored_at_epoch")) {
    await db.exec(`
      ALTER TABLE posting_application_state
      ADD COLUMN ignored_at_epoch INTEGER;
    `);
  }
  if (!postingStateColumnNames.has("ignored_by_label")) {
    await db.exec(`
      ALTER TABLE posting_application_state
      ADD COLUMN ignored_by_label TEXT NOT NULL DEFAULT '';
    `);
  }
  if (!mcpSettingsColumnNames.has("agent_login_password")) {
    await db.exec(`
      ALTER TABLE McpSettings
      ADD COLUMN agent_login_password TEXT NOT NULL DEFAULT '';
    `);
  }
}

async function openDatabase() {
  db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database
  });
  await ensureTables();
}

async function getPersonalInformation() {
  const row = await db.get(
    `
      SELECT
        first_name,
        middle_name,
        last_name,
        email,
        phone_number,
        address,
        linkedin_url,
        github_url,
        portfolio_url,
        resume_file_path,
        projects_portfolio_file_path,
        certifications_folder_path,
        ethnicity,
        gender,
        age,
        veteran_status,
        disability_status,
        education_level,
        years_of_experience
      FROM PersonalInformation
      ORDER BY rowid ASC
      LIMIT 1;
    `
  );
  return row || {};
}

function parseJsonArray(value) {
  try {
    const parsed = JSON.parse(String(value || "[]"));
    if (!Array.isArray(parsed)) return [];
    return parsed.map((item) => String(item || "").trim()).filter(Boolean);
  } catch {
    return [];
  }
}

async function getMcpSettings() {
  const row = await db.get(
    `
      SELECT
        enabled,
        preferred_agent_name,
        agent_login_email,
        agent_login_password,
        mfa_login_email,
        mfa_login_notes,
        dry_run_only,
        require_final_approval,
        max_applications_per_run,
        preferred_search,
        preferred_remote,
        preferred_industries,
        preferred_states,
        preferred_counties,
        instructions_for_agent
      FROM McpSettings
      WHERE id = 1
      LIMIT 1;
    `
  );

  const agentLoginEmail = String(row?.agent_login_email || "");
  return {
    ...MCP_SETTINGS_DEFAULTS,
    enabled: Boolean(Number(row?.enabled || 0)),
    preferred_agent_name: String(row?.preferred_agent_name || MCP_SETTINGS_DEFAULTS.preferred_agent_name),
    agent_login_email: agentLoginEmail,
    agent_login_password: String(row?.agent_login_password || ""),
    mfa_login_email: agentLoginEmail,
    mfa_login_notes: String(row?.mfa_login_notes || ""),
    dry_run_only: row?.dry_run_only === undefined ? true : Boolean(Number(row?.dry_run_only)),
    require_final_approval:
      row?.require_final_approval === undefined ? true : Boolean(Number(row?.require_final_approval)),
    max_applications_per_run: parseNonNegativeInteger(row?.max_applications_per_run) || 10,
    preferred_search: String(row?.preferred_search || ""),
    preferred_remote: normalizeRemoteFilter(row?.preferred_remote),
    preferred_industries: parseJsonArray(row?.preferred_industries),
    preferred_states: parseJsonArray(row?.preferred_states).map((state) => state.toUpperCase()),
    preferred_counties: parseJsonArray(row?.preferred_counties),
    instructions_for_agent: String(row?.instructions_for_agent || "")
  };
}

async function buildIndustryMatchersByKey(industryKeys) {
  if (!Array.isArray(industryKeys) || industryKeys.length === 0) {
    return new Map();
  }

  const [wordIndustryCoverage, phraseNgramIndustryCoverage] = await Promise.all([
    getWordIndustryCoverageMap(),
    getPhraseNgramIndustryCoverageMap()
  ]);

  const placeholders = industryKeys.map(() => "?").join(", ");
  let rows = [];
  try {
    rows = await db.all(
      `
        SELECT industry_key, normalized_job_title
        FROM job_position_industry
        WHERE industry_key IN (${placeholders});
      `,
      industryKeys
    );
  } catch {
    return new Map();
  }

  const byIndustry = new Map();
  for (const key of industryKeys) {
    byIndustry.set(key, {
      exactTitles: new Set(),
      phraseNgrams: new Set(),
      fallbackWords: new Set(),
      wordCounts: new Map(),
      phraseCounts: new Map()
    });
  }

  for (const row of rows) {
    const key = String(row?.industry_key || "").trim();
    if (!key || !byIndustry.has(key)) continue;
    const normalizedTitle = normalizeLikeText(row?.normalized_job_title);
    const words = createLikeParts(normalizedTitle);
    const target = byIndustry.get(key);
    if (normalizedTitle) {
      target.exactTitles.add(normalizedTitle);
    }

    for (const word of new Set(words)) {
      target.wordCounts.set(word, (target.wordCounts.get(word) || 0) + 1);
    }

    for (const ngram of new Set(buildWordNgrams(words, 2, 3))) {
      target.phraseCounts.set(ngram, (target.phraseCounts.get(ngram) || 0) + 1);
    }
  }

  const finalized = new Map();
  for (const [industryKey, matcher] of byIndustry.entries()) {
    const fallbackWords = new Set();
    for (const [word, count] of matcher.wordCounts.entries()) {
      if (count < MIN_INDUSTRY_FALLBACK_WORD_COUNT) continue;
      if (isWeakFallbackWord(word, wordIndustryCoverage)) continue;
      fallbackWords.add(word);
    }

    const phraseNgrams = new Set();
    for (const [ngram, count] of matcher.phraseCounts.entries()) {
      if (count < MIN_INDUSTRY_PHRASE_NGRAM_COUNT) continue;
      if (isWeakPhraseNgram(ngram, phraseNgramIndustryCoverage)) continue;
      phraseNgrams.add(ngram);
    }

    finalized.set(industryKey, {
      exactTitles: matcher.exactTitles,
      phraseNgrams,
      fallbackWords
    });
  }

  return finalized;
}

async function getWordIndustryCoverageMap() {
  if (wordIndustryCoverageCache instanceof Map) {
    return wordIndustryCoverageCache;
  }

  let rows = [];
  try {
    rows = await db.all(
      `
        SELECT industry_key, normalized_job_title
        FROM job_position_industry;
      `
    );
  } catch {
    wordIndustryCoverageCache = new Map();
    return wordIndustryCoverageCache;
  }

  const wordIndustrySets = new Map();
  for (const row of rows) {
    const industryKey = String(row?.industry_key || "").trim();
    if (!industryKey) continue;

    const words = new Set(createLikeParts(row?.normalized_job_title));
    for (const word of words) {
      if (!wordIndustrySets.has(word)) {
        wordIndustrySets.set(word, new Set());
      }
      wordIndustrySets.get(word).add(industryKey);
    }
  }

  const coverageMap = new Map();
  for (const [word, keys] of wordIndustrySets.entries()) {
    coverageMap.set(word, keys.size);
  }

  wordIndustryCoverageCache = coverageMap;
  return coverageMap;
}

async function getPhraseNgramIndustryCoverageMap() {
  if (phraseNgramIndustryCoverageCache instanceof Map) {
    return phraseNgramIndustryCoverageCache;
  }

  let rows = [];
  try {
    rows = await db.all(
      `
        SELECT industry_key, normalized_job_title
        FROM job_position_industry;
      `
    );
  } catch {
    phraseNgramIndustryCoverageCache = new Map();
    return phraseNgramIndustryCoverageCache;
  }

  const ngramIndustrySets = new Map();
  for (const row of rows) {
    const industryKey = String(row?.industry_key || "").trim();
    if (!industryKey) continue;

    const words = createLikeParts(row?.normalized_job_title);
    const ngrams = new Set(buildWordNgrams(words, 2, 3));
    for (const ngram of ngrams) {
      if (!ngramIndustrySets.has(ngram)) {
        ngramIndustrySets.set(ngram, new Set());
      }
      ngramIndustrySets.get(ngram).add(industryKey);
    }
  }

  const coverageMap = new Map();
  for (const [ngram, keys] of ngramIndustrySets.entries()) {
    coverageMap.set(ngram, keys.size);
  }

  phraseNgramIndustryCoverageCache = coverageMap;
  return coverageMap;
}

function isWeakFallbackWord(word, wordIndustryCoverage) {
  if (!word) return true;
  if (WEAK_INDUSTRY_LIKE_PARTS.has(word)) return true;
  const industryCoverage = Number(wordIndustryCoverage?.get(word) || 0);
  return industryCoverage >= FALLBACK_WORD_INDUSTRY_COVERAGE_THRESHOLD;
}

function isWeakPhraseNgram(ngram, phraseNgramIndustryCoverage) {
  if (!ngram) return true;
  const parts = ngram.split(" ").map((part) => part.trim()).filter(Boolean);
  if (parts.length < 2) return true;
  if (parts.every((part) => WEAK_INDUSTRY_LIKE_PARTS.has(part))) return true;
  const industryCoverage = Number(phraseNgramIndustryCoverage?.get(ngram) || 0);
  return industryCoverage >= PHRASE_NGRAM_INDUSTRY_COVERAGE_THRESHOLD;
}

function rowMatchesIndustryLikeParts(positionName, selectedIndustryKeys, industryMatchersByKey) {
  if (!Array.isArray(selectedIndustryKeys) || selectedIndustryKeys.length === 0) return true;
  if (!(industryMatchersByKey instanceof Map) || industryMatchersByKey.size === 0) return false;

  const titleText = String(positionName || "");
  const selectedKeySet = new Set(
    selectedIndustryKeys.map((key) => String(key || "").trim().toLowerCase()).filter(Boolean)
  );
  const isSalesExclusiveRole = SALES_EXCLUSIVE_ROLE_REGEX.test(titleText);
  if (isSalesExclusiveRole && !selectedKeySet.has(SALES_BUSINESS_INDUSTRY_KEY)) {
    return false;
  }

  const normalizedPosition = normalizeLikeText(positionName);
  const postingWords = createLikeParts(positionName);
  if (postingWords.length === 0) return false;
  const postingWordSet = new Set(postingWords);
  const postingPhraseSet = new Set(buildWordNgrams(postingWords, 2, 3));

  for (const industryKey of selectedIndustryKeys) {
    const matcher = industryMatchersByKey.get(industryKey);
    const exactTitles = matcher?.exactTitles;
    const phraseNgrams = matcher?.phraseNgrams;
    const fallbackWords = matcher?.fallbackWords;
    const hasMatcherData =
      exactTitles instanceof Set || phraseNgrams instanceof Set || fallbackWords instanceof Set;
    if (!hasMatcherData) continue;

    if (exactTitles instanceof Set && normalizedPosition && exactTitles.has(normalizedPosition)) {
      if (industryKey === IT_SOFTWARE_INDUSTRY_KEY && IT_SALES_GTM_ROLE_REGEX.test(titleText)) {
        continue;
      }

      const hasStrongPhrase =
        phraseNgrams instanceof Set &&
        Array.from(postingPhraseSet).some((postingPhrase) => phraseNgrams.has(postingPhrase));
      const hasStrongWord =
        fallbackWords instanceof Set &&
        Array.from(postingWordSet).some((word) => fallbackWords.has(word));
      if (hasStrongPhrase || hasStrongWord) {
        return true;
      }
      if (
        industryKey === IT_SOFTWARE_INDUSTRY_KEY &&
        Array.from(postingWordSet).some((word) => IT_HIGH_SIGNAL_ANCHOR_PARTS.has(word))
      ) {
        return true;
      }
    }

    if (industryKey === IT_SOFTWARE_INDUSTRY_KEY) {
      if (IT_SALES_GTM_ROLE_REGEX.test(titleText)) continue;
      const hasTechAnchor = Array.from(postingWordSet).some((part) => IT_TECH_ANCHOR_PARTS.has(part));
      if (!hasTechAnchor) continue;
    }

    if (phraseNgrams instanceof Set && phraseNgrams.size > 0) {
      for (const postingPhrase of postingPhraseSet) {
        if (phraseNgrams.has(postingPhrase)) {
          return true;
        }
      }
    }

    if (fallbackWords instanceof Set && fallbackWords.size > 0) {
      for (const word of postingWordSet) {
        if (fallbackWords.has(word)) {
          if (
            industryKey !== IT_SOFTWARE_INDUSTRY_KEY ||
            postingWordSet.size === 1 ||
            IT_HIGH_SIGNAL_ANCHOR_PARTS.has(word)
          ) {
            return true;
          }
        }
      }
    }

    if (industryKey === IT_SOFTWARE_INDUSTRY_KEY) {
      for (const word of postingWordSet) {
        if (IT_HIGH_SIGNAL_ANCHOR_PARTS.has(word) && fallbackWords instanceof Set && fallbackWords.has(word)) {
          return true;
        }
      }
    }
  }
  return false;
}

async function getAppliedStateByUrl(urls) {
  const uniqueUrls = Array.from(new Set((urls || []).map((url) => String(url || "").trim()).filter(Boolean)));
  if (uniqueUrls.length === 0) return new Map();

  const map = new Map();
  const chunkSize = 800;
  for (let i = 0; i < uniqueUrls.length; i += chunkSize) {
    const chunk = uniqueUrls.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => "?").join(", ");
    const rows = await db.all(
      `
        SELECT
          job_posting_url,
          applied,
          applied_by_type,
          applied_by_label,
          applied_at_epoch,
          last_application_id,
          ignored,
          ignored_at_epoch,
          ignored_by_label
        FROM posting_application_state
        WHERE job_posting_url IN (${placeholders});
      `,
      chunk
    );

    for (const row of rows) {
      map.set(String(row?.job_posting_url || "").trim(), row);
    }
  }
  return map;
}

async function findCandidates(options = {}) {
  const settings = await getMcpSettings();
  ensureMcpAgentEnabled(settings);
  const atsFilters = normalizeAtsFilters(options.ats);
  const industries =
    Array.isArray(options.industries) && options.industries.length > 0
      ? options.industries
      : settings.preferred_industries || [];
  const states =
    Array.isArray(options.states) && options.states.length > 0 ? options.states : settings.preferred_states || [];
  const counties =
    Array.isArray(options.counties) && options.counties.length > 0
      ? options.counties
      : settings.preferred_counties || [];
  const remote = options.remote ? normalizeRemoteFilter(options.remote) : settings.preferred_remote;
  const search = String(options.search || settings.preferred_search || "").trim().toLowerCase();
  const includeApplied = normalizeBoolean(options.include_applied, false);
  const limit = Math.max(
    1,
    Math.min(2000, parseNonNegativeInteger(options.limit) || parseNonNegativeInteger(settings.max_applications_per_run) || 10)
  );

  const rows = await db.all(
    `
      SELECT id, company_name, position_name, job_posting_url, posting_date
      FROM Postings
      ORDER BY company_name ASC, position_name ASC;
    `
  );

  const industryMatchersByKey = await buildIndustryMatchersByKey(industries);
  const countyFilters = parseCountyFilters(counties);
  const searchTerms = search.split(/\s+/).filter(Boolean);
  const stateCodes = states.map((state) => String(state || "").trim().toUpperCase()).filter(Boolean);

  let items = rows
    .map((row) => ({
      ...row,
      location: inferLocationFromJobUrl(row?.job_posting_url),
      ats: inferAtsFromJobPostingUrl(row?.job_posting_url)
    }))
    .filter((row) => {
      const companyName = String(row?.company_name || "").toLowerCase();
      const positionName = String(row?.position_name || "").toLowerCase();
      const location = String(row?.location || "").toLowerCase();
      const ats = String(row?.ats || "").toLowerCase();

      const matchesSearch = searchTerms.every(
        (term) => companyName.includes(term) || positionName.includes(term) || location.includes(term)
      );
      if (!matchesSearch) return false;

      if (atsFilters.length > 0 && !atsFilters.includes(ats)) return false;

      const matchesIndustry = rowMatchesIndustryLikeParts(
        row?.position_name,
        industries,
        industryMatchersByKey
      );
      if (!matchesIndustry) return false;

      const matchesLocation = rowMatchesLocationFilters(row?.location, stateCodes, countyFilters);
      if (!matchesLocation) return false;

      const matchesRemote = rowMatchesRemoteFilter(row?.location, remote);
      if (!matchesRemote) return false;

      return true;
    });

  const appliedByUrl = await getAppliedStateByUrl(items.map((item) => item.job_posting_url));
  items = items.map((item) => {
    const state = appliedByUrl.get(String(item?.job_posting_url || "").trim());
    return {
      ...item,
      applied: Boolean(Number(state?.applied || 0)),
      ignored: Boolean(Number(state?.ignored || 0)),
      applied_by_label: String(state?.applied_by_label || ""),
      applied_at_epoch: Number(state?.applied_at_epoch || 0)
    };
  });

  items = items.filter((item) => !item.ignored);

  if (!includeApplied) {
    items = items.filter((item) => !item.applied);
  }

  items = items.slice(0, limit);
  return {
    filters: {
      search,
      ats: atsFilters,
      industries,
      states: stateCodes,
      counties,
      remote
    },
    count: items.length,
    items
  };
}

async function resolveCompanyIdForApplication(companyName) {
  const normalized = normalizeLikeText(companyName);
  if (!normalized) return null;

  return db.get(
    `
      SELECT id
      FROM companies
      WHERE LOWER(company_name) = ?
      ORDER BY id ASC
      LIMIT 1;
    `,
    [normalized]
  );
}

async function resolveCompanyIdFromPostingUrl(jobPostingUrl) {
  const normalizedUrl = String(jobPostingUrl || "").trim();
  if (!normalizedUrl) return null;

  const posting = await db.get(
    `
      SELECT company_name
      FROM Postings
      WHERE job_posting_url = ?
      LIMIT 1;
    `,
    [normalizedUrl]
  );

  const normalizedCompanyName = normalizeLikeText(posting?.company_name);
  if (!normalizedCompanyName) return null;

  return db.get(
    `
      SELECT id
      FROM companies
      WHERE LOWER(company_name) = ?
      ORDER BY id ASC
      LIMIT 1;
    `,
    [normalizedCompanyName]
  );
}

async function getApplicationById(applicationId) {
  const row = await db.get(
    `
      SELECT
        a.id,
        a.company_id,
        c.company_name,
        a.position_name,
        a.application_date,
        a.status,
        attr.applied_by_type,
        attr.applied_by_label
      FROM applications a
      LEFT JOIN companies c
        ON c.id = a.company_id
      LEFT JOIN application_attribution attr
        ON attr.application_id = a.id
      WHERE a.id = ?
      LIMIT 1;
    `,
    [applicationId]
  );
  if (!row) return null;

  return {
    id: Number(row?.id || 0),
    company_id: Number(row?.company_id || 0),
    company_name: String(row?.company_name || "").trim(),
    position_name: String(row?.position_name || "").trim(),
    job_posting_url: "",
    application_date: Number(row?.application_date || 0),
    status: String(row?.status || "applied").trim().toLowerCase() || "applied",
    applied_by_type: normalizeAppliedByType(row?.applied_by_type),
    applied_by_label: normalizeAppliedByLabel(row?.applied_by_label, row?.applied_by_type)
  };
}

async function getExistingAppliedApplicationByPostingUrl(jobPostingUrl) {
  const normalizedUrl = String(jobPostingUrl || "").trim();
  if (!normalizedUrl) return null;

  const state = await db.get(
    `
      SELECT last_application_id
      FROM posting_application_state
      WHERE job_posting_url = ?
        AND COALESCE(applied, 0) = 1
      LIMIT 1;
    `,
    [normalizedUrl]
  );
  const lastApplicationId = parseNonNegativeInteger(state?.last_application_id);
  if (!lastApplicationId) return null;

  const existing = await getApplicationById(lastApplicationId);
  if (!existing) return null;
  return {
    ...existing,
    job_posting_url: normalizedUrl
  };
}

function buildCoverLetterDraft(personalInformation, posting, instructions = "") {
  const firstName = String(personalInformation?.first_name || "").trim() || "Applicant";
  const lastName = String(personalInformation?.last_name || "").trim();
  const fullName = `${firstName}${lastName ? ` ${lastName}` : ""}`.trim();
  const yearsOfExperience = parseNonNegativeInteger(personalInformation?.years_of_experience);
  const positionName = String(posting?.position_name || "the role").trim();
  const companyName = String(posting?.company_name || "your company").trim();
  const linkedinUrl = String(personalInformation?.linkedin_url || "").trim();
  const githubUrl = String(personalInformation?.github_url || "").trim();
  const portfolioUrl = String(personalInformation?.portfolio_url || "").trim();
  const extraInstructions = String(instructions || "").trim();

  const details = [];
  if (yearsOfExperience > 0) details.push(`${yearsOfExperience}+ years of relevant experience`);
  if (linkedinUrl) details.push(`LinkedIn: ${linkedinUrl}`);
  if (githubUrl) details.push(`GitHub: ${githubUrl}`);
  if (portfolioUrl) details.push(`Portfolio: ${portfolioUrl}`);
  const detailSentence =
    details.length > 0
      ? `My background includes ${details.join(", ")}.`
      : "My background aligns with fast-paced, delivery-focused teams.";
  const instructionSentence = extraInstructions ? `I am especially aligned with: ${extraInstructions}.` : "";

  return `Dear Hiring Team,

I am excited to apply for the ${positionName} role at ${companyName}. ${detailSentence}

I am motivated by opportunities where I can contribute quickly and collaborate closely with a strong team. ${instructionSentence}

Thank you for your consideration.

Sincerely,
${fullName}`.trim();
}

function asToolResult(payload) {
  return {
    content: [{ type: "text", text: JSON.stringify(payload, null, 2) }],
    structuredContent: payload
  };
}

async function createApplicationFromAgent(input) {
  const companyName = String(input?.company_name || "").trim();
  const positionName = String(input?.position_name || "").trim();
  const jobPostingUrl = String(input?.job_posting_url || "").trim();
  const appliedByLabel = String(input?.applied_by_label || "").trim() || "AI agent applied on behalf of user";
  const applicationDate = parseNonNegativeInteger(input?.application_date) || nowEpochSeconds();
  const status = String(input?.status || "applied").trim().toLowerCase() || "applied";

  if (jobPostingUrl) {
    const existing = await getExistingAppliedApplicationByPostingUrl(jobPostingUrl);
    if (existing) return existing;
  }

  const companyFromPosting = await resolveCompanyIdFromPostingUrl(jobPostingUrl);
  const company = companyFromPosting || (companyName ? await resolveCompanyIdForApplication(companyName) : null);
  if (!company?.id) {
    throw new Error(
      jobPostingUrl
        ? `Unable to resolve company_id for job_posting_url='${jobPostingUrl}'`
        : `Unable to resolve company_id for company_name='${companyName}'`
    );
  }

  await db.exec("BEGIN TRANSACTION;");
  try {
    const result = await db.run(
      `
        INSERT INTO applications (
          company_id,
          position_name,
          application_date,
          status
        ) VALUES (?, ?, ?, ?);
      `,
      [company.id, positionName, applicationDate, status]
    );

    await db.run(
      `
        INSERT INTO application_attribution (
          application_id,
          applied_by_type,
          applied_by_label,
          updated_at
        ) VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(application_id) DO UPDATE SET
          applied_by_type = excluded.applied_by_type,
          applied_by_label = excluded.applied_by_label,
          updated_at = datetime('now');
      `,
      [result.lastID, "agent", appliedByLabel]
    );

    if (jobPostingUrl) {
      await db.run(
        `
          INSERT INTO posting_application_state (
            job_posting_url,
            applied,
            applied_by_type,
            applied_by_label,
            applied_at_epoch,
            last_application_id,
            updated_at
          ) VALUES (?, 1, 'agent', ?, ?, ?, datetime('now'))
          ON CONFLICT(job_posting_url) DO UPDATE SET
            applied = 1,
            applied_by_type = 'agent',
            applied_by_label = excluded.applied_by_label,
            applied_at_epoch = excluded.applied_at_epoch,
            last_application_id = excluded.last_application_id,
            updated_at = datetime('now');
        `,
        [jobPostingUrl, appliedByLabel, applicationDate, result.lastID]
      );
    }

    await db.exec("COMMIT;");
    return {
      id: result.lastID,
      company_id: company.id,
      company_name: companyName,
      position_name: positionName,
      job_posting_url: jobPostingUrl,
      application_date: applicationDate,
      status,
      applied_by_type: "agent",
      applied_by_label: appliedByLabel
    };
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }
}

async function main() {
  await openDatabase();

  const mcpServer = new McpServer({
    name: "openpostings-apply-agent",
    version: "1.0.0"
  });

  mcpServer.registerTool(
    "get_applicant_context",
    {
      description: "Read applicantee information and MCP settings used by the apply agent."
    },
    async () => {
      const personalInformation = await getPersonalInformation();
      const mcpSettings = await getMcpSettings();
      ensureMcpAgentEnabled(mcpSettings);
      const runbook = {
        summary:
          "Use existing browser/web tools to apply through each posting URL, using applicant data for form fields and MCP login credentials for account/MFA flows.",
        steps: [
          "Load context and candidate postings.",
          "Open posting URL in browser automation.",
          "Fill application form with applicant data.",
          "Use agent login email + password for account creation and sign-in when required.",
          "Use the same agent login email for MFA/approval flows when required.",
          "Generate and attach a posting-specific cover letter.",
          "Call record_application_result after submit."
        ]
      };
      return asToolResult({
        personal_information: personalInformation,
        mcp_settings: mcpSettings,
        runbook
      });
    }
  );

  mcpServer.registerTool(
    "find_posting_candidates",
    {
      description:
        "Find postings that match MCP preferences using search + industry/location/remote filters with like-parts industry matching.",
      inputSchema: {
        search: z.string().optional(),
        ats: z
          .union([
            z.enum([
              "workday",
              "ashby",
              "greenhouse",
              "lever",
              "jobvite",
              "applicantpro",
              "applytojob",
              "theapplicantmanager",
              "recruitee",
              "ultipro",
              "taleo"
            ]),
            z.array(
              z.enum([
                "workday",
                "ashby",
                "greenhouse",
                "lever",
                "jobvite",
                "applicantpro",
                "applytojob",
                "theapplicantmanager",
                "recruitee",
                "ultipro",
                "taleo"
              ])
            )
          ])
          .optional(),
        industries: z.array(z.string()).optional(),
        states: z.array(z.string()).optional(),
        counties: z.array(z.string()).optional(),
        remote: z.enum(["all", "remote", "non_remote"]).optional(),
        include_applied: z.boolean().optional(),
        limit: z.number().int().positive().max(2000).optional()
      }
    },
    async (args) => {
      const result = await findCandidates(args || {});
      return asToolResult(result);
    }
  );

  mcpServer.registerTool(
    "draft_cover_letter",
    {
      description: "Generate a cover letter draft for a posting using applicantee information.",
      inputSchema: {
        company_name: z.string().optional(),
        position_name: z.string().optional(),
        job_posting_url: z.string().optional(),
        instructions: z.string().optional()
      }
    },
    async (args) => {
      const mcpSettings = await getMcpSettings();
      ensureMcpAgentEnabled(mcpSettings);
      const personalInformation = await getPersonalInformation();
      let companyName = String(args?.company_name || "").trim();
      let positionName = String(args?.position_name || "").trim();
      const jobPostingUrl = String(args?.job_posting_url || "").trim();

      if (jobPostingUrl && (!companyName || !positionName)) {
        const posting = await db.get(
          `
            SELECT company_name, position_name
            FROM Postings
            WHERE job_posting_url = ?
            LIMIT 1;
          `,
          [jobPostingUrl]
        );
        companyName = companyName || String(posting?.company_name || "").trim();
        positionName = positionName || String(posting?.position_name || "").trim();
      }

      const draft = buildCoverLetterDraft(
        personalInformation,
        {
          company_name: companyName,
          position_name: positionName,
          job_posting_url: jobPostingUrl
        },
        args?.instructions
      );

      return asToolResult({
        posting: {
          company_name: companyName,
          position_name: positionName,
          job_posting_url: jobPostingUrl
        },
        draft
      });
    }
  );

  mcpServer.registerTool(
    "record_application_result",
    {
      description:
        "Write a completed agent-driven application result into applications and posting application state tables.",
      inputSchema: {
        job_posting_url: z.string(),
        company_name: z.string().optional(),
        position_name: z.string().optional(),
        status: z.string().optional(),
        application_date: z.number().int().nonnegative().optional(),
        agent_name: z.string().optional(),
        commit: z.boolean().optional(),
        approved_by_user: z.boolean().optional()
      }
    },
    async (args) => {
      const mcpSettings = await getMcpSettings();
      ensureMcpAgentEnabled(mcpSettings);
      const commit = normalizeBoolean(args?.commit, false);
      const approvedByUser = normalizeBoolean(args?.approved_by_user, false);
      const jobPostingUrl = String(args?.job_posting_url || "").trim();
      const agentName =
        String(args?.agent_name || mcpSettings.preferred_agent_name || MCP_SETTINGS_DEFAULTS.preferred_agent_name)
          .trim() || MCP_SETTINGS_DEFAULTS.preferred_agent_name;

      if (!jobPostingUrl) {
        throw new Error("job_posting_url is required.");
      }

      const posting = await db.get(
        `
          SELECT company_name, position_name
          FROM Postings
          WHERE job_posting_url = ?
          LIMIT 1;
        `,
        [jobPostingUrl]
      );

      const companyName = String(args?.company_name || posting?.company_name || "").trim();
      const positionName = String(args?.position_name || posting?.position_name || "").trim();
      const status = String(args?.status || "applied").trim().toLowerCase() || "applied";
      const applicationDate = parseNonNegativeInteger(args?.application_date) || nowEpochSeconds();
      const appliedByLabel = `${agentName} applied on behalf of user`;

      if (!companyName || !positionName) {
        throw new Error("company_name and position_name are required (or must be resolvable from job_posting_url).");
      }

      if (!commit || mcpSettings.dry_run_only) {
        return asToolResult({
          committed: false,
          dry_run: true,
          reason: mcpSettings.dry_run_only ? "MCP settings are currently dry_run_only=true." : "commit=false",
          payload: {
            company_name: companyName,
            position_name: positionName,
            job_posting_url: jobPostingUrl,
            status,
            application_date: applicationDate,
            applied_by_label: appliedByLabel
          }
        });
      }

      if (mcpSettings.require_final_approval && !approvedByUser) {
        throw new Error("Final approval is required. Set approved_by_user=true to commit.");
      }

      const application = await createApplicationFromAgent({
        company_name: companyName,
        position_name: positionName,
        job_posting_url: jobPostingUrl,
        status,
        application_date: applicationDate,
        applied_by_label: appliedByLabel
      });

      return asToolResult({
        committed: true,
        application
      });
    }
  );

  const transport = new StdioServerTransport();
  await mcpServer.connect(transport);
}

main().catch((error) => {
  console.error("[openpostings-apply-agent] MCP server failed:", error);
  process.exit(1);
});
