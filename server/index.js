const cors = require("cors");
const express = require("express");
const fs = require("fs");
const path = require("path");
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");

const PORT = Number(process.env.PORT || 8787);
const DB_PATH = process.env.DB_PATH || path.resolve(__dirname, "..", "jobs.db");
const SEED_DATA_DIR = path.resolve(__dirname, "data");
const SYNC_INTERVAL_MS = Number(process.env.SYNC_INTERVAL_MS || 30 * 60 * 1000);
const SYNC_CONCURRENCY = Number(process.env.SYNC_CONCURRENCY || 20);
const SYNC_POSTING_FLUSH_BATCH_SIZE = Number(process.env.SYNC_POSTING_FLUSH_BATCH_SIZE || 200);
const FETCH_TIMEOUT_MS = Number(process.env.FETCH_TIMEOUT_MS || 12000);
const POSTING_TTL_SECONDS = Number(process.env.POSTING_TTL_SECONDS || 24 * 60 * 60);
const FRESHNESS_WINDOW_SECONDS = Number(process.env.FRESHNESS_WINDOW_SECONDS || 48 * 60 * 60);
const WORKDAY_PAGE_SIZE = 20;
const ULTIPRO_PAGE_SIZE = 50;
const MAX_PAGES_PER_COMPANY = 25;
const LOCALE_SEGMENT_REGEX = /^[a-z]{2}(?:-[a-z]{2})?$/i;
const ASHBY_API_URL = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams";
const ASHBY_RATE_LIMIT_WAIT_MS = 60 * 1000;
const GREENHOUSE_API_URL_BASE = "https://boards-api.greenhouse.io/v1/boards";
const GREENHOUSE_RATE_LIMIT_WAIT_MS = 60 * 1000;
const LEVER_API_URL_BASE = "https://api.lever.co/v0/postings";
const LEVER_RATE_LIMIT_WAIT_MS = 60 * 1000;
const RECRUITEE_RATE_LIMIT_WAIT_MS = 60 * 1000;
const ULTIPRO_RATE_LIMIT_WAIT_MS = 60 * 1000;
const ICIMS_RATE_LIMIT_WAIT_MS = 60 * 1000;
const WORKABLE_API_URL_BASE = "https://apply.workable.com/api/v1/widget/accounts";
const WORKABLE_RATE_LIMIT_WAIT_MS = 60 * 1000;
const ORACLECLOUD_RATE_LIMIT_WAIT_MS = 60 * 1000;
const BAMBOOHR_RATE_LIMIT_WAIT_MS = 60 * 1000;
const ORACLECLOUD_PAGE_SIZE = 25;
const ORACLECLOUD_MAX_PAGES = 500;
const ORACLECLOUD_API_PATH = "/hcmRestApi/resources/11.13.18.05/recruitingCEJobRequisitions";
const ASHBY_QUERY = `
  query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
    jobBoard: jobBoardWithTeams(
      organizationHostedJobsPageName: $organizationHostedJobsPageName
    ) {
      teams {
        id
        name
        externalName
        parentTeamId
        __typename
      }
      jobPostings {
        id
        title
        teamId
        locationId
        locationName
        workplaceType
        employmentType
        secondaryLocations {
          ...JobPostingSecondaryLocationParts
          __typename
        }
        compensationTierSummary
        __typename
      }
      __typename
    }
  }

  fragment JobPostingSecondaryLocationParts on JobPostingSecondaryLocation {
    locationId
    locationName
    __typename
  }
`;

let db;
let wordIndustryCoverageCache = null;
let phraseNgramIndustryCoverageCache = null;
let syncPromise = null;
let postingLocationByJobUrl = new Map();
const syncStatus = {
  running: false,
  started_at: null,
  last_sync_at: null,
  last_sync_summary: null,
  last_error: null,
  progress: null
};
const PERSONAL_INFORMATION_FIELDS = [
  "first_name",
  "middle_name",
  "last_name",
  "email",
  "phone_number",
  "address",
  "linkedin_url",
  "github_url",
  "portfolio_url",
  "resume_file_path",
  "projects_portfolio_file_path",
  "certifications_folder_path",
  "ethnicity",
  "gender",
  "age",
  "veteran_status",
  "disability_status",
  "education_level",
  "years_of_experience"
];
const PERSONAL_INFORMATION_DEFAULTS = {
  first_name: "",
  middle_name: "",
  last_name: "",
  email: "",
  phone_number: "",
  address: "",
  linkedin_url: "",
  github_url: "",
  portfolio_url: "",
  resume_file_path: "",
  projects_portfolio_file_path: "",
  certifications_folder_path: "",
  ethnicity: "",
  gender: "",
  age: 0,
  veteran_status: "",
  disability_status: "",
  education_level: "",
  years_of_experience: 0
};
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
  AP: "andhra pradesh",
  AR: "arunachal pradesh",
  AS: "assam",
  BR: "bihar",
  CG: "chhattisgarh",
  GA: "goa",
  GJ: "gujarat",
  HR: "haryana",
  HP: "himachal pradesh",
  JH: "jharkhand",
  KA: "karnataka",
  KL: "kerala",
  MP: "madhya pradesh",
  MH: "maharashtra",
  MN: "manipur",
  ML: "meghalaya",
  MZ: "mizoram",
  NL: "nagaland",
  OD: "odisha",
  PB: "punjab",
  RJ: "rajasthan",
  SK: "sikkim",
  TN: "tamil nadu",
  TS: "telangana",
  TR: "tripura",
  UP: "uttar pradesh",
  UK: "uttarakhand",
  WB: "west bengal",
  AN: "andaman and nicobar islands",
  CH: "chandigarh",
  DN: "dadra and nagar haveli and daman and diu",
  DL: "delhi",
  JK: "jammu and kashmir",
  LA: "ladakh",
  LD: "lakshadweep",
  PY: "puducherry"
};
const APPLICATION_STATUS_OPTIONS = new Set([
  "applied",
  "interview scheduled",
  "awaiting response",
  "offer received",
  "withdrawn",
  "denied"
]);
const MCP_REMOTE_OPTIONS = new Set(["all", "remote", "non_remote"]);
const ATS_FILTER_OPTIONS = new Set([
  "workday",
  "ashby",
  "greenhouse",
  "lever",
  "recruitee",
  "ultipro",
  "icims",
  "oraclecloud",
  "workable",
  "bamboohr"
]);
const POSTING_SORT_OPTIONS = new Set(["recent", "company_asc"]);
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
  instructions_for_agent: ""
};
const PHRASE_NGRAM_INDUSTRY_COVERAGE_THRESHOLD = 2;
const FALLBACK_WORD_INDUSTRY_COVERAGE_THRESHOLD = 2;
const MIN_INDUSTRY_FALLBACK_WORD_COUNT = 3;
const MIN_INDUSTRY_PHRASE_NGRAM_COUNT = 2;

function nowEpochSeconds() {
  return Math.floor(Date.now() / 1000);
}

const INDIA_LOCATION_KEYWORDS = [
  "bengaluru", "bangalore", "mumbai", "hyderabad", "chennai",
  "pune", "gurugram", "gurgaon", "noida", "kolkata", "new delhi",
  "jaipur", "ahmedabad", "kochi", "thiruvananthapuram", "coimbatore",
  "lucknow", "chandigarh", "indore", "nagpur", "bhubaneswar",
  "visakhapatnam", "mysuru", "mysore", "mangalore", "mangaluru",
  "trivandrum", "pathankot", "vizag", "india"
];

function looksLikeIndiaLocation(locationStr) {
  if (!locationStr) return false;
  const lower = String(locationStr).toLowerCase();
  return INDIA_LOCATION_KEYWORDS.some((kw) => lower.includes(kw));
}

function parsePostingDateToEpoch(dateStr) {
  if (!dateStr || typeof dateStr !== "string") return null;
  const trimmed = dateStr.trim();
  if (!trimmed) return null;
  if (trimmed === "Posted Today") return nowEpochSeconds();
  if (/^(false|full.time|part.time|\[)/i.test(trimmed)) return null;
  const d = new Date(trimmed);
  if (isNaN(d.getTime())) return null;
  const epoch = Math.floor(d.getTime() / 1000);
  if (epoch > 1577836800 && epoch < 1893456000) return epoch;
  return null;
}

function normalizeLikeText(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function parseCsvParam(value) {
  return String(value || "")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function parseJsonArray(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  try {
    const parsed = JSON.parse(String(value || "[]"));
    if (!Array.isArray(parsed)) return [];
    return parsed.map((item) => String(item || "").trim()).filter(Boolean);
  } catch {
    return [];
  }
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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
  const codeRegex = new RegExp(`(^|[^A-Z])${escapeRegExp(code)}([^A-Z]|$)`);
  if (codeRegex.test(upperLocation)) return true;

  const stateName = STATE_CODE_TO_NAME[code];
  if (!stateName) return false;
  return normalizeLikeText(locationText).includes(stateName);
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
function rowMatchesLocationFilters(locationText, selectedStateCodes) {
  const stateCodes = Array.isArray(selectedStateCodes) ? selectedStateCodes : [];
  if (stateCodes.length === 0) return true;

  const location = String(locationText || "").trim();
  if (!location) return false;

  return stateCodes.some((stateCode) => hasStateLikeMatch(location, stateCode));
}

function rowMatchesRemoteFilter(locationText, remoteFilter) {
  const normalized = normalizeRemoteFilter(remoteFilter);
  if (!normalized || normalized === "all") return true;
  const isRemote = isRemoteLocation(locationText);
  if (normalized === "remote") return isRemote;
  if (normalized === "non_remote") return !isRemote;
  return true;
}

function normalizeRemoteFilter(value) {
  const normalized = String(value || "all")
    .trim()
    .toLowerCase();
  if (normalized === "remote" || normalized === "non_remote") return normalized;
  return "all";
}

function inferAtsFromJobPostingUrl(value) {
  const url = String(value || "").trim().toLowerCase();
  if (!url) return "";
  if (url.includes("myworkdayjobs.com")) return "workday";
  if (url.includes("jobs.ashbyhq.com")) return "ashby";
  if (url.includes("job-boards.greenhouse.io") || url.includes("boards.greenhouse.io")) return "greenhouse";
  if (url.includes("jobs.lever.co")) return "lever";
  if (url.includes(".recruitee.com")) return "recruitee";
  if (url.includes("recruiting.ultipro.com/") && url.includes("/jobboard/")) return "ultipro";
  if (url.includes(".icims.com/jobs/")) return "icims";
  if (url.includes("oraclecloud.com/")) return "oraclecloud";
  if (url.includes("apply.workable.com/")) return "workable";
  if (url.includes(".bamboohr.com/careers")) return "bamboohr";
  return "";
}

function normalizeAtsFilterValue(value) {
  const normalized = normalizeLikeText(value);
  if (normalized === "ashbyhq") return "ashby";
  if (normalized === "greenhouseio" || normalized === "greenhouse.io") return "greenhouse";
  if (normalized === "leverco" || normalized === "lever.co") return "lever";
  if (normalized === "recruiteecom" || normalized === "recruitee.com") return "recruitee";
  if (normalized === "ukg") return "ultipro";
  if (normalized === "icimscom" || normalized === "icims.com") return "icims";
  if (normalized === "workable.com") return "workable";
  if (normalized === "bamboohr.com" || normalized === "bamboohrcom") return "bamboohr";
  return normalized;
}

function normalizeAtsFilters(value) {
  const items = normalizeStringArray(Array.isArray(value) ? value : [value])
    .map((item) => normalizeAtsFilterValue(item))
    .filter((item) => ATS_FILTER_OPTIONS.has(item));
  return Array.from(new Set(items));
}

function normalizePostingSort(value) {
  const normalized = normalizeLikeText(value);
  if (normalized === "company_asc" || normalized === "alphabetical") {
    return "company_asc";
  }
  if (POSTING_SORT_OPTIONS.has(normalized)) {
    return normalized;
  }
  return "recent";
}

function getPostingsOrderByClause(sortBy) {
  if (sortBy === "company_asc") {
    return "company_name ASC, position_name ASC";
  }
  return "COALESCE(last_seen_epoch, 0) DESC, id DESC";
}

function shuffleArrayInPlace(values) {
  const items = Array.isArray(values) ? values : [];
  for (let i = items.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [items[i], items[j]] = [items[j], items[i]];
  }
  return items;
}

function normalizeStringArray(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function normalizeApplicationStatus(value) {
  const normalized = normalizeLikeText(value);
  if (APPLICATION_STATUS_OPTIONS.has(normalized)) {
    return normalized;
  }
  return "applied";
}

function normalizeAppliedByType(value) {
  const normalized = normalizeLikeText(value);
  if (normalized === "ai" || normalized === "agent") return normalized;
  return "manual";
}

function normalizeAppliedByLabel(value, appliedByType = "manual") {
  const explicit = String(value || "").trim();
  if (explicit) return explicit;
  if (appliedByType === "ai" || appliedByType === "agent") {
    return "AI agent applied on behalf of user";
  }
  return "Manually applied by user";
}

function normalizeIgnoredByLabel(value) {
  const explicit = String(value || "").trim();
  if (explicit) return explicit;
  return "Ignored by user";
}

function normalizeBoolean(value, defaultValue = false) {
  if (typeof value === "boolean") return value;
  const normalized = normalizeLikeText(value);
  if (!normalized) return Boolean(defaultValue);
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function normalizeMcpRemotePreference(value) {
  const normalized = normalizeLikeText(value);
  if (MCP_REMOTE_OPTIONS.has(normalized)) return normalized;
  return "all";
}

function normalizeMcpSettingsInput(value = {}) {
  const source = value && typeof value === "object" ? value : {};
  const agentLoginEmail = String(source.agent_login_email ?? MCP_SETTINGS_DEFAULTS.agent_login_email).trim();

  return {
    enabled: normalizeBoolean(source.enabled, MCP_SETTINGS_DEFAULTS.enabled),
    preferred_agent_name: String(source.preferred_agent_name ?? MCP_SETTINGS_DEFAULTS.preferred_agent_name).trim() ||
      MCP_SETTINGS_DEFAULTS.preferred_agent_name,
    agent_login_email: agentLoginEmail,
    agent_login_password: String(source.agent_login_password ?? MCP_SETTINGS_DEFAULTS.agent_login_password),
    mfa_login_email: agentLoginEmail,
    mfa_login_notes: String(source.mfa_login_notes ?? MCP_SETTINGS_DEFAULTS.mfa_login_notes).trim(),
    dry_run_only: normalizeBoolean(source.dry_run_only, MCP_SETTINGS_DEFAULTS.dry_run_only),
    require_final_approval: normalizeBoolean(
      source.require_final_approval,
      MCP_SETTINGS_DEFAULTS.require_final_approval
    ),
    max_applications_per_run:
      parseNonNegativeInteger(source.max_applications_per_run) || MCP_SETTINGS_DEFAULTS.max_applications_per_run,
    preferred_search: String(source.preferred_search ?? MCP_SETTINGS_DEFAULTS.preferred_search).trim(),
    preferred_remote: normalizeMcpRemotePreference(source.preferred_remote),
    preferred_industries: parseJsonArray(source.preferred_industries),
    preferred_states: parseJsonArray(source.preferred_states).map((state) => state.toUpperCase()),
    instructions_for_agent: String(source.instructions_for_agent ?? MCP_SETTINGS_DEFAULTS.instructions_for_agent).trim()
  };
}

function ensureMcpAgentEnabled(settings) {
  if (normalizeBoolean(settings?.enabled, false)) return;
  const error = new Error("MCP application agent is disabled in settings.");
  error.statusCode = 403;
  throw error;
}

function createDefaultPersonalInformation() {
  return { ...PERSONAL_INFORMATION_DEFAULTS };
}

function parseNonNegativeInteger(value) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function normalizePersonalInformationInput(value) {
  const source = value && typeof value === "object" ? value : {};
  const normalized = createDefaultPersonalInformation();
  const numericFields = new Set(["age", "years_of_experience"]);
  const textFields = PERSONAL_INFORMATION_FIELDS.filter((field) => !numericFields.has(field));

  for (const field of textFields) {
    normalized[field] = String(source[field] ?? "").trim();
  }

  normalized.age = parseNonNegativeInteger(source.age);
  normalized.years_of_experience = parseNonNegativeInteger(source.years_of_experience);

  return normalized;
}

function parseUrl(urlString) {
  if (!urlString) return null;
  try {
    return new URL(urlString);
  } catch {
    return null;
  }
}

function pickCompanyId(pathParts, subdomain) {
  if (!Array.isArray(pathParts) || pathParts.length === 0) return subdomain;

  const [first = "", second = ""] = pathParts;
  if (first && LOCALE_SEGMENT_REGEX.test(first) && second) {
    return second;
  }

  return first || subdomain;
}

function parseWorkdayCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const [subdomain = ""] = parsed.hostname.split(".");
  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  const companyIdRaw = pickCompanyId(pathParts, subdomain);
  const companyIdApi = companyIdRaw.toLowerCase();

  if (!subdomain || !companyIdApi) return null;

  return {
    subdomain: subdomain.toLowerCase(),
    companyIdRaw,
    companyIdApi,
    companyBaseUrl: `${parsed.origin}/${companyIdRaw}`,
    cxsUrl: `${parsed.origin}/wday/cxs/${subdomain.toLowerCase()}/${companyIdApi}/jobs`
  };
}

function isPostedToday(postedOn) {
  if (typeof postedOn !== "string") return false;
  return postedOn.trim().toLowerCase() === "posted today";
}

function buildJobUrl(companyBaseUrl, externalPath) {
  if (typeof externalPath !== "string" || !externalPath.trim()) return "";
  const normalizedPath = externalPath.startsWith("/") ? externalPath : `/${externalPath}`;
  return `${companyBaseUrl}${normalizedPath}`;
}

function formatLocationSegment(rawLocation) {
  if (typeof rawLocation !== "string") return null;
  const trimmed = rawLocation.trim();
  if (!trimmed) return null;

  const doubleDashToken = "__DOUBLE_DASH__";
  return trimmed
    .replace(/--+/g, doubleDashToken)
    .replace(/-/g, " ")
    .replace(new RegExp(doubleDashToken, "g"), "- ")
    .replace(/\s+/g, " ")
    .trim();
}

function inferWorkdayLocationFromJobUrl(jobPostingUrl) {
  try {
    const parsed = new URL(String(jobPostingUrl || ""));
    const pathParts = parsed.pathname
      .split("/")
      .map((part) => String(part || "").trim())
      .filter(Boolean);
    const jobIndex = pathParts.findIndex((part) => part.toLowerCase() === "job");
    if (jobIndex >= 0 && pathParts[jobIndex + 1] && pathParts[jobIndex + 2]) {
      const rawLocation = decodeURIComponent(pathParts[jobIndex + 1]);
      return formatLocationSegment(rawLocation);
    }
    return null;
  } catch {
    return null;
  }
}

function inferPostingLocationFromJobUrl(jobPostingUrl) {
  const url = String(jobPostingUrl || "").trim();
  if (!url) return null;

  try {
    const parsed = new URL(url);
    if (parsed.hostname.endsWith("myworkdayjobs.com")) {
      return inferWorkdayLocationFromJobUrl(url);
    }
    if (parsed.hostname === "jobs.ashbyhq.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "job-boards.greenhouse.io" || parsed.hostname === "boards.greenhouse.io") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "jobs.lever.co") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".recruitee.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "recruiting.ultipro.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".icims.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.includes("oraclecloud.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".bamboohr.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    return null;
  } catch {
    return null;
  }
}

function extractAshbyLocationName(posting) {
  const names = [];
  const primary = String(posting?.locationName || "").trim();
  if (primary) names.push(primary);

  const secondary = Array.isArray(posting?.secondaryLocations) ? posting.secondaryLocations : [];
  for (const location of secondary) {
    const name = String(location?.locationName || "").trim();
    if (!name) continue;
    if (names.some((existing) => existing.toLowerCase() === name.toLowerCase())) continue;
    names.push(name);
  }

  return names.length > 0 ? names.join(", ") : null;
}

function parseAshbyCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;
  const [organizationHostedJobsPageName = ""] = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (!organizationHostedJobsPageName) return null;

  return {
    organizationHostedJobsPageName,
    organizationHostedJobsPageNameLower: organizationHostedJobsPageName.toLowerCase()
  };
}

function parseGreenhouseCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;
  const [boardToken = ""] = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (!boardToken) return null;

  return {
    boardToken,
    boardTokenLower: boardToken.toLowerCase()
  };
}

function parseLeverCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;
  const [organization = ""] = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (!organization) return null;

  return {
    organization,
    organizationLower: organization.toLowerCase()
  };
}

function parseWorkableCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "apply.workable.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  const slug = String(pathParts[0] || "").trim();
  if (!slug || slug === "j") return null;

  return {
    slug,
    slugLower: slug.toLowerCase()
  };
}

function parseBambooHrCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  const suffix = ".bamboohr.com";
  if (!host.endsWith(suffix)) return null;

  const companySubdomain = String(host.slice(0, -suffix.length) || "").trim();
  if (!companySubdomain || companySubdomain.includes(".") || companySubdomain === "www") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length > 0 && String(pathParts[0] || "").toLowerCase() !== "careers") return null;

  const baseOrigin = `${parsed.protocol}//${parsed.host}`;
  return {
    companySubdomain,
    companySubdomainLower: companySubdomain.toLowerCase(),
    baseOrigin,
    boardUrl: `${baseOrigin}/careers`,
    apiUrl: `${baseOrigin}/careers/list`
  };
}

function parseIcimsCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".icims.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  const searchUrl = new URL(parsed.toString());
  searchUrl.pathname = "/jobs/search";
  if (!searchUrl.searchParams.has("ss")) {
    searchUrl.searchParams.set("ss", "1");
  }
  searchUrl.searchParams.delete("in_iframe");

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    origin: `${parsed.protocol}//${parsed.host}`,
    searchUrl: searchUrl.toString()
  };
}

function parseRecruiteeCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;
  if (!String(parsed.hostname || "").toLowerCase().endsWith(".recruitee.com")) return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  const normalizedPathParts = pathParts[0]?.toLowerCase() === "o" ? [] : pathParts;
  const basePath = normalizedPathParts.length > 0 ? `/${normalizedPathParts.join("/")}` : "";
  const baseUrl = `${parsed.origin}${basePath}`.replace(/\/+$/, "");
  const [subdomain = ""] = parsed.hostname.split(".");

  return {
    baseUrl: baseUrl || parsed.origin,
    subdomain: String(subdomain || "").toLowerCase()
  };
}

function parseUltiProCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "recruiting.ultipro.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);

  const jobBoardIndex = pathParts.findIndex((part) => part.toLowerCase() === "jobboard");
  if (jobBoardIndex <= 0 || jobBoardIndex + 1 >= pathParts.length) return null;

  const tenant = pathParts[jobBoardIndex - 1];
  const boardId = pathParts[jobBoardIndex + 1];
  if (!tenant || !boardId) return null;

  return {
    tenant,
    tenantLower: tenant.toLowerCase(),
    boardId,
    baseBoardUrl: `${parsed.protocol}//${parsed.host}/${tenant}/JobBoard/${boardId}`
  };
}

function parseOracleCloudCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.includes("oraclecloud.com")) return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);

  const sitesIndex = pathParts.findIndex((p) => p.toLowerCase() === "sites");
  if (sitesIndex < 0 || !pathParts[sitesIndex + 1]) return null;

  const siteNumber = pathParts[sitesIndex + 1];

  return {
    host: parsed.host,
    origin: parsed.origin,
    siteNumber,
    apiUrl: `${parsed.origin}${ORACLECLOUD_API_PATH}`,
    jobBaseUrl: `${parsed.origin}/hcmUI/CandidateExperience/en/sites/${siteNumber}/job`
  };
}

function decodeHtmlEntities(value) {
  return String(value || "")
    .replace(/&#(\d+);/g, (_match, codePoint) => String.fromCharCode(Number(codePoint)))
    .replace(/&#x([0-9a-f]+);/gi, (_match, codePoint) => String.fromCharCode(parseInt(codePoint, 16)))
    .replace(/&quot;/g, "\"")
    .replace(/&#34;/g, "\"")
    .replace(/&#x22;/gi, "\"")
    .replace(/&apos;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/gi, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function cleanIcimsText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function ensureIcimsIframeUrl(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return String(urlString || "").trim();
  parsed.searchParams.set("in_iframe", "1");
  return parsed.toString();
}

function extractIcimsIframeUrlFromHtml(pageHtml, baseUrl) {
  const source = String(pageHtml || "");
  const patterns = [
    /icimsFrame\.src\s*=\s*'([^']+)'/i,
    /icimsFrame\.src\s*=\s*"([^"]+)"/i,
    /<iframe[^>]*id=["']icims_content_iframe["'][^>]*src=["']([^"']+)["']/i
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    const rawValue = String(match?.[1] || "").trim();
    if (!rawValue) continue;

    let candidate = decodeHtmlEntities(rawValue).replace(/\\\//g, "/");
    if (!candidate) continue;

    if (candidate.startsWith("//")) {
      const parsedBase = parseUrl(baseUrl);
      const protocol = String(parsedBase?.protocol || "https:");
      candidate = `${protocol}${candidate}`;
    } else if (!/^https?:\/\//i.test(candidate)) {
      try {
        candidate = new URL(candidate, baseUrl).toString();
      } catch {
        continue;
      }
    }

    return ensureIcimsIframeUrl(candidate);
  }

  return ensureIcimsIframeUrl(baseUrl);
}

function extractIcimsLocationFromHtml(sourceHtml) {
  const source = String(sourceHtml || "");
  const patterns = [
    /field-label">Location\s*<\/span>\s*<\/dt>\s*<dd[^>]*class=["'][^"']*iCIMS_JobHeaderData[^"']*["'][^>]*>\s*<span[^>]*>([\s\S]*?)<\/span>/i,
    /glyphicons-map-marker[^>]*>[\s\S]*?<\/dt>\s*<dd[^>]*class=["'][^"']*iCIMS_JobHeaderData[^"']*["'][^>]*>\s*<span[^>]*>([\s\S]*?)<\/span>/i
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    if (!match?.[1]) continue;
    const location = cleanIcimsText(match[1]);
    if (location) return location;
  }

  return null;
}

function extractIcimsPostingDateFromHtml(sourceHtml) {
  const source = String(sourceHtml || "");
  const match = source.match(
    /field-label">[^<]*Posted\s*Date[^<]*<\/span>\s*<span[^>]*?(?:title=["']([^"']+)["'])?[^>]*>\s*([^<]*)/i
  ) || source.match(
    /field-label">Date Posted\s*<\/span>\s*<span[^>]*?(?:title=["']([^"']+)["'])?[^>]*>\s*([^<]*)/i
  );
  const withTitle = String(match?.[1] || "").trim();
  if (withTitle) return withTitle;
  const fallback = cleanIcimsText(match?.[2] || "");
  return fallback || null;
}

function parseIcimsPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();
  const cardPattern = /<li[^>]*class=["'][^"']*iCIMS_JobCardItem[^"']*["'][^>]*>([\s\S]*?)<\/li>/gi;

  let cardMatch = cardPattern.exec(source);
  while (cardMatch) {
    const cardHtml = String(cardMatch[1] || "");
    const anchorPattern = /<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;

    let linkHref = "";
    let linkBody = "";
    let anchorMatch = anchorPattern.exec(cardHtml);
    while (anchorMatch) {
      const href = String(anchorMatch[1] || "").trim();
      if (/\/jobs\/\d+/i.test(href)) {
        linkHref = href;
        linkBody = String(anchorMatch[2] || "");
        break;
      }
      anchorMatch = anchorPattern.exec(cardHtml);
    }

    if (!linkHref) {
      cardMatch = cardPattern.exec(source);
      continue;
    }

    const absoluteUrl = new URL(linkHref, `${config.origin}/`).toString();
    if (!absoluteUrl || seenUrls.has(absoluteUrl) || absoluteUrl.toLowerCase().includes("/jobs/intro")) {
      cardMatch = cardPattern.exec(source);
      continue;
    }

    const titleMatch = linkBody.match(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/i);
    const positionName = cleanIcimsText(titleMatch?.[1] || linkBody) || "Untitled Position";

    postings.push({
      company_name: companyNameForPostings,
      position_name: positionName,
      job_posting_url: absoluteUrl,
      posting_date: extractIcimsPostingDateFromHtml(cardHtml),
      location: extractIcimsLocationFromHtml(cardHtml)
    });
    seenUrls.add(absoluteUrl);
    cardMatch = cardPattern.exec(source);
  }

  if (postings.length > 0) return postings;

  const fallbackLinkPattern = /<a[^>]*href=["']([^"']*\/jobs\/\d+[^"']*)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let fallbackMatch = fallbackLinkPattern.exec(source);
  while (fallbackMatch) {
    const href = String(fallbackMatch[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.origin}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl) || absoluteUrl.toLowerCase().includes("/jobs/intro")) {
      fallbackMatch = fallbackLinkPattern.exec(source);
      continue;
    }

    const linkBody = String(fallbackMatch[2] || "");
    const titleMatch = linkBody.match(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/i);
    const positionName = cleanIcimsText(titleMatch?.[1] || linkBody) || "Untitled Position";

    const contextStart = Math.max(0, Number(fallbackMatch.index || 0) - 800);
    const contextEnd = Math.min(source.length, Number(fallbackMatch.index || 0) + String(fallbackMatch[0] || "").length + 2200);
    const contextHtml = source.slice(contextStart, contextEnd);

    postings.push({
      company_name: companyNameForPostings,
      position_name: positionName,
      job_posting_url: absoluteUrl,
      posting_date: extractIcimsPostingDateFromHtml(contextHtml),
      location: extractIcimsLocationFromHtml(contextHtml)
    });
    seenUrls.add(absoluteUrl);
    fallbackMatch = fallbackLinkPattern.exec(source);
  }

  return postings;
}

function extractIcimsNextPageUrlFromHtml(pageHtml, currentUrl) {
  const source = String(pageHtml || "");
  const patterns = [
    /<link[^>]*rel=["']next["'][^>]*href=["']([^"']+)["']/i,
    /<link[^>]*href=["']([^"']+)["'][^>]*rel=["']next["'][^>]*>/i
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    const rawValue = String(match?.[1] || "").trim();
    if (!rawValue) continue;

    let candidate = decodeHtmlEntities(rawValue).replace(/\\\//g, "/");
    if (!candidate) continue;

    if (candidate.startsWith("//")) {
      const parsedCurrent = parseUrl(currentUrl);
      const protocol = String(parsedCurrent?.protocol || "https:");
      candidate = `${protocol}${candidate}`;
    } else if (!/^https?:\/\//i.test(candidate)) {
      try {
        candidate = new URL(candidate, currentUrl).toString();
      } catch {
        continue;
      }
    }

    const normalizedCandidate = ensureIcimsIframeUrl(candidate);
    if (normalizedCandidate && normalizedCandidate !== String(currentUrl || "").trim()) {
      return normalizedCandidate;
    }
  }

  return null;
}

function extractRecruiteePropsFromHtml(pageHtml) {
  const source = String(pageHtml || "");
  const patterns = [
    /data-component=(?:"|')PublicApp(?:"|')[^>]*data-props=(?:"|')([^"']+)(?:"|')/is,
    /data-props=(?:"|')([^"']+)(?:"|')[^>]*data-component=(?:"|')PublicApp(?:"|')/is
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    const encodedProps = String(match?.[1] || "");
    if (!encodedProps) continue;

    const decodedProps = decodeHtmlEntities(encodedProps);
    try {
      const parsedProps = JSON.parse(decodedProps);
      if (parsedProps && typeof parsedProps === "object") return parsedProps;
    } catch {
      // Continue with the next extraction pattern.
    }
  }

  return null;
}

function pickRecruiteeTranslation(translations, preferredLangCode = "") {
  const byLang = translations && typeof translations === "object" ? translations : {};
  const candidates = [];
  const preferred = String(preferredLangCode || "").trim();

  if (preferred && byLang[preferred] && typeof byLang[preferred] === "object") {
    candidates.push(byLang[preferred]);
  }
  if (byLang.en && typeof byLang.en === "object") {
    candidates.push(byLang.en);
  }
  for (const value of Object.values(byLang)) {
    if (value && typeof value === "object") candidates.push(value);
  }

  for (const candidate of candidates) {
    if (candidate && typeof candidate === "object") return candidate;
  }

  return {};
}

function extractRecruiteeTitle(offer, preferredLangCode = "") {
  const translation = pickRecruiteeTranslation(offer?.translations, offer?.primaryLangCode || preferredLangCode);
  const title = String(translation?.title || translation?.name || offer?.slug || "").trim();
  return title || "Untitled Position";
}

function buildRecruiteeLocationLabel(location, preferredLangCode = "") {
  const translation = pickRecruiteeTranslation(location?.translations, preferredLangCode);
  const name = String(translation?.name || translation?.city || location?.name || "").trim();
  const country = String(translation?.country || "").trim();
  if (name && country) return `${name}, ${country}`;
  return name || country || null;
}

function buildAshbyJobUrl(organizationHostedJobsPageName, jobId) {
  if (!organizationHostedJobsPageName || !jobId) return "";
  return `https://jobs.ashbyhq.com/${organizationHostedJobsPageName}/${jobId}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWorkdayPage(cxsUrl, limit, offset) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(cxsUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        appliedFacets: {},
        limit,
        offset,
        searchText: ""
      }),
      signal: controller.signal
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Workday request failed (${res.status}): ${body.slice(0, 180)}`);
    }

    return res.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchAshbyJobBoard(organizationHostedJobsPageName) {
  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const res = await fetch(ASHBY_API_URL, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          operationName: "ApiJobBoardWithTeams",
          variables: {
            organizationHostedJobsPageName
          },
          query: ASHBY_QUERY
        }),
        signal: controller.signal
      });

      if (res.status === 429) {
        await sleep(ASHBY_RATE_LIMIT_WAIT_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Ashby request failed (${res.status}): ${body.slice(0, 180)}`);
      }

      const data = await res.json();
      if (Array.isArray(data?.errors) && data.errors.length > 0) {
        const firstError = String(data.errors[0]?.message || "Unknown Ashby GraphQL error");
        throw new Error(`Ashby GraphQL error: ${firstError}`);
      }

      return data;
    } finally {
      clearTimeout(timeout);
    }
  }
}

const ASHBY_DETAIL_QUERY = `
  query JobPostingDetail($organizationHostedJobsPageName: String!, $jobPostingId: String!) {
    jobPosting(
      organizationHostedJobsPageName: $organizationHostedJobsPageName,
      jobPostingId: $jobPostingId
    ) {
      publishedDate
    }
  }
`;

async function fetchAshbyPublishedDate(organizationHostedJobsPageName, jobPostingId) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(ASHBY_API_URL, {
      method: "POST",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: JSON.stringify({
        operationName: "JobPostingDetail",
        variables: { organizationHostedJobsPageName, jobPostingId },
        query: ASHBY_DETAIL_QUERY
      }),
      signal: controller.signal
    });
    if (!res.ok) return null;
    const data = await res.json();
    return String(data?.data?.jobPosting?.publishedDate || "").trim() || null;
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchGreenhouseJobBoard(boardToken) {
  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const encodedBoardToken = encodeURIComponent(boardToken);
      const res = await fetch(`${GREENHOUSE_API_URL_BASE}/${encodedBoardToken}/jobs?content=true`, {
        method: "GET",
        headers: {
          Accept: "application/json"
        },
        signal: controller.signal
      });

      if (res.status === 429) {
        await sleep(GREENHOUSE_RATE_LIMIT_WAIT_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Greenhouse request failed (${res.status}): ${body.slice(0, 180)}`);
      }

      return res.json();
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function fetchLeverJobBoard(organization) {
  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const encodedOrganization = encodeURIComponent(organization);
      const res = await fetch(`${LEVER_API_URL_BASE}/${encodedOrganization}?mode=json`, {
        method: "GET",
        headers: {
          Accept: "application/json"
        },
        signal: controller.signal
      });

      if (res.status === 429) {
        await sleep(LEVER_RATE_LIMIT_WAIT_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Lever request failed (${res.status}): ${body.slice(0, 180)}`);
      }

      return res.json();
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function fetchRecruiteePublicApp(baseUrl) {
  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const res = await fetch(baseUrl, {
        method: "GET",
        headers: {
          Accept: "text/html,application/xhtml+xml"
        },
        signal: controller.signal
      });

      if (res.status === 429) {
        await sleep(RECRUITEE_RATE_LIMIT_WAIT_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Recruitee request failed (${res.status}): ${body.slice(0, 180)}`);
      }

      const pageHtml = await res.text();
      const props = extractRecruiteePropsFromHtml(pageHtml);
      if (!props) {
        throw new Error("Recruitee payload not found in PublicApp data-props");
      }
      return props;
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function fetchRecruiteePublishedDate(jobPageUrl) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(jobPageUrl, {
      method: "GET",
      headers: { Accept: "text/html,application/xhtml+xml" },
      signal: controller.signal
    });
    if (!res.ok) return null;
    const html = await res.text();
    const ldMatch = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
    if (!ldMatch) return null;
    const ld = JSON.parse(ldMatch[1]);
    return String(ld?.datePosted || "").trim() || null;
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchIcimsPage(urlString) {
  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const res = await fetch(urlString, {
        method: "GET",
        headers: {
          Accept: "text/html,application/xhtml+xml"
        },
        signal: controller.signal
      });

      if (res.status === 429) {
        await sleep(ICIMS_RATE_LIMIT_WAIT_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`iCIMS page request failed (${res.status}): ${body.slice(0, 180)}`);
      }

      return res.text();
    } finally {
      clearTimeout(timeout);
    }
  }
}

function buildUltiProSearchPayload(top, skip) {
  return {
    opportunitySearch: {
      Top: Number(top || ULTIPRO_PAGE_SIZE),
      Skip: Number(skip || 0),
      QueryString: "",
      OrderBy: [
        {
          Value: "postedDateDesc",
          PropertyName: "PostedDate",
          Ascending: false
        }
      ],
      Filters: [
        { t: "TermsSearchFilterDto", fieldName: 4, extra: null, values: [] },
        { t: "TermsSearchFilterDto", fieldName: 5, extra: null, values: [] },
        { t: "TermsSearchFilterDto", fieldName: 6, extra: null, values: [] },
        { t: "TermsSearchFilterDto", fieldName: 37, extra: null, values: [] }
      ]
    },
    matchCriteria: {
      PreferredJobs: [],
      Educations: [],
      LicenseAndCertifications: [],
      Skills: [],
      hasNoLicenses: false,
      SkippedSkills: []
    }
  };
}

async function fetchUltiProSearchResults(config, top, skip) {
  const tenantEncoded = encodeURIComponent(String(config?.tenant || "").trim());
  const boardIdEncoded = encodeURIComponent(String(config?.boardId || "").trim());
  const apiUrl = `https://recruiting.ultipro.com/${tenantEncoded}/JobBoard/${boardIdEncoded}/JobBoardView/LoadSearchResults`;
  const payload = buildUltiProSearchPayload(top, skip);

  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const res = await fetch(apiUrl, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      });

      if (res.status === 429) {
        await sleep(ULTIPRO_RATE_LIMIT_WAIT_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`UltiPro request failed (${res.status}): ${body.slice(0, 180)}`);
      }

      return res.json();
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function collectTodayPostingsForWorkdayCompany(company) {
  const config = parseWorkdayCompany(company.url_string);
  if (!config) return [];

  const collected = [];
  let offset = 0;

  for (let page = 0; page < MAX_PAGES_PER_COMPANY; page += 1) {
    const response = await fetchWorkdayPage(config.cxsUrl, WORKDAY_PAGE_SIZE, offset);
    const postings = Array.isArray(response?.jobPostings) ? response.jobPostings : [];
    if (postings.length === 0) break;

    let todaysOnPage = 0;
    for (const posting of postings) {
      if (!isPostedToday(posting?.postedOn)) continue;
      todaysOnPage += 1;
      const jobUrl = buildJobUrl(config.companyBaseUrl, posting?.externalPath);
      if (!jobUrl) continue;

      collected.push({
        company_name: company.company_name,
        position_name: String(posting?.title || "").trim() || "Untitled Position",
        job_posting_url: jobUrl,
        posting_date: String(posting?.postedOn || "").trim() || null,
        location: inferWorkdayLocationFromJobUrl(jobUrl)
      });
    }

    if (todaysOnPage === 0 || postings.length < WORKDAY_PAGE_SIZE) break;
    offset += WORKDAY_PAGE_SIZE;
  }

  return collected;
}

async function collectPostingsForAshbyCompany(company) {
  const config = parseAshbyCompany(company.url_string);
  if (!config) return [];

  const response = await fetchAshbyJobBoard(config.organizationHostedJobsPageName);
  const jobPostings = Array.isArray(response?.data?.jobBoard?.jobPostings)
    ? response.data.jobBoard.jobPostings
    : [];

  const collected = [];
  const indiaPostingIndices = [];

  for (const posting of jobPostings) {
    const jobId = String(posting?.id || "").trim();
    if (!jobId) continue;

    const jobUrl = buildAshbyJobUrl(config.organizationHostedJobsPageName, jobId);
    if (!jobUrl) continue;

    const location = extractAshbyLocationName(posting);
    const entry = {
      company_name: company.company_name,
      position_name: String(posting?.title || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: null,
      location
    };
    collected.push(entry);

    if (looksLikeIndiaLocation(location)) {
      indiaPostingIndices.push({ index: collected.length - 1, jobId });
    }
  }

  if (indiaPostingIndices.length > 0) {
    const DETAIL_BATCH_SIZE = 5;
    for (let i = 0; i < indiaPostingIndices.length; i += DETAIL_BATCH_SIZE) {
      const batch = indiaPostingIndices.slice(i, i + DETAIL_BATCH_SIZE);
      const results = await Promise.allSettled(
        batch.map((item) =>
          fetchAshbyPublishedDate(config.organizationHostedJobsPageName, item.jobId)
        )
      );
      for (let j = 0; j < results.length; j += 1) {
        if (results[j].status === "fulfilled" && results[j].value) {
          collected[batch[j].index].posting_date = results[j].value;
        }
      }
    }
  }

  return collected;
}

function extractGreenhouseLocationName(posting) {
  const nestedLocation = String(posting?.location?.name || "").trim();
  if (nestedLocation) return nestedLocation;

  const flatLocation = String(posting?.location || "").trim();
  return flatLocation || null;
}

async function collectPostingsForGreenhouseCompany(company) {
  const config = parseGreenhouseCompany(company.url_string);
  if (!config) return [];

  const response = await fetchGreenhouseJobBoard(config.boardToken);
  const jobPostings = Array.isArray(response?.jobs) ? response.jobs : [];
  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings =
    normalizedCompanyName && normalizedCompanyName.toLowerCase() !== "job-boards"
      ? normalizedCompanyName
      : config.boardTokenLower;

  const collected = [];
  for (const posting of jobPostings) {
    const jobUrl = String(posting?.absolute_url || "").trim();
    if (!jobUrl) continue;

    collected.push({
      company_name: companyNameForPostings,
      position_name: String(posting?.title || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: String(posting?.updated_at || posting?.first_published || "").trim() || null,
      location: extractGreenhouseLocationName(posting)
    });
  }

  return collected;
}

function extractLeverLocationName(posting) {
  const allLocations = Array.isArray(posting?.categories?.allLocations) ? posting.categories.allLocations : [];
  const normalizedAllLocations = allLocations
    .map((entry) => String(entry || "").trim())
    .filter(Boolean);
  if (normalizedAllLocations.length > 0) {
    return normalizedAllLocations.join(" / ");
  }

  const location = String(posting?.categories?.location || "").trim();
  return location || null;
}

async function collectPostingsForLeverCompany(company) {
  const config = parseLeverCompany(company.url_string);
  if (!config) return [];

  const response = await fetchLeverJobBoard(config.organization);
  const jobPostings = Array.isArray(response) ? response : [];
  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings =
    normalizedCompanyName && normalizedCompanyName.toLowerCase() !== "jobs"
      ? normalizedCompanyName
      : config.organizationLower;

  const collected = [];
  for (const posting of jobPostings) {
    const jobUrl = String(posting?.hostedUrl || "").trim();
    if (!jobUrl) continue;

    const createdAt = Number(posting?.createdAt || 0);
    const postingDate =
      Number.isFinite(createdAt) && createdAt > 0 ? new Date(createdAt).toISOString() : null;

    collected.push({
      company_name: companyNameForPostings,
      position_name: String(posting?.text || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location: extractLeverLocationName(posting)
    });
  }

  return collected;
}

async function fetchIcimsPostingDate(jobUrl) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(jobUrl, {
      method: "GET",
      headers: { Accept: "text/html,application/xhtml+xml" },
      signal: controller.signal
    });
    if (!res.ok) return null;
    const html = await res.text();
    const jsonLdMatch = html.match(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
    if (!jsonLdMatch) return null;
    const data = JSON.parse(jsonLdMatch[1]);
    const datePosted = String(data?.datePosted || "").trim();
    return datePosted || null;
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function collectPostingsForIcimsCompany(company) {
  const config = parseIcimsCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;

  const wrapperHtml = await fetchIcimsPage(config.searchUrl);
  let pageUrl = extractIcimsIframeUrlFromHtml(wrapperHtml, config.searchUrl);
  const collected = [];
  const seenPostingUrls = new Set();
  const seenPageUrls = new Set();

  for (let page = 0; page < MAX_PAGES_PER_COMPANY; page += 1) {
    const normalizedPageUrl = ensureIcimsIframeUrl(pageUrl);
    if (!normalizedPageUrl || seenPageUrls.has(normalizedPageUrl)) break;
    seenPageUrls.add(normalizedPageUrl);

    const pageHtml = await fetchIcimsPage(normalizedPageUrl);
    const batch = parseIcimsPostingsFromHtml(companyNameForPostings, config, pageHtml);
    for (const posting of batch) {
      const postingUrl = String(posting?.job_posting_url || "").trim();
      if (!postingUrl || seenPostingUrls.has(postingUrl)) continue;
      seenPostingUrls.add(postingUrl);
      collected.push(posting);
    }

    const nextPageUrl = extractIcimsNextPageUrlFromHtml(pageHtml, normalizedPageUrl);
    if (!nextPageUrl) break;
    pageUrl = nextPageUrl;
  }

  const indiaPostingsWithoutDate = [];
  for (let i = 0; i < collected.length; i += 1) {
    if (!collected[i].posting_date && looksLikeIndiaLocation(collected[i].location)) {
      indiaPostingsWithoutDate.push({ index: i, jobUrl: collected[i].job_posting_url });
    }
  }

  if (indiaPostingsWithoutDate.length > 0) {
    const DETAIL_BATCH_SIZE = 5;
    for (let i = 0; i < indiaPostingsWithoutDate.length; i += DETAIL_BATCH_SIZE) {
      const batch = indiaPostingsWithoutDate.slice(i, i + DETAIL_BATCH_SIZE);
      const results = await Promise.allSettled(
        batch.map((item) => fetchIcimsPostingDate(item.jobUrl))
      );
      for (let j = 0; j < results.length; j += 1) {
        if (results[j].status === "fulfilled" && results[j].value) {
          collected[batch[j].index].posting_date = results[j].value;
        }
      }
    }
  }

  return collected;
}

async function collectPostingsForRecruiteeCompany(company) {
  const config = parseRecruiteeCompany(company.url_string);
  if (!config) return [];

  const response = await fetchRecruiteePublicApp(config.baseUrl);
  const appConfig = response?.appConfig && typeof response.appConfig === "object" ? response.appConfig : {};
  const preferredLangCode = String(appConfig?.primaryLangCode || "").trim();
  const offers = Array.isArray(appConfig?.offers) ? appConfig.offers : [];
  const locations = Array.isArray(appConfig?.locations) ? appConfig.locations : [];

  const locationById = new Map();
  for (const location of locations) {
    const id = String(location?.id ?? "").trim();
    if (!id) continue;
    const label = buildRecruiteeLocationLabel(location, preferredLangCode);
    if (label) locationById.set(id, label);
  }

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings =
    normalizedCompanyName && normalizedCompanyName.toLowerCase() !== "recruitee"
      ? normalizedCompanyName
      : config.subdomain;

  const collected = [];
  const indiaPostingIndices = [];

  for (const offer of offers) {
    const slug = String(offer?.slug || "").trim();
    const jobUrl = slug ? `${config.baseUrl}/o/${slug}` : config.baseUrl;
    if (!jobUrl) continue;

    const publishedValue =
      offer?.publishedAt ?? offer?.published_at ?? offer?.createdAt ?? offer?.created_at ?? offer?.updatedAt;
    let postingDate = null;
    if (typeof publishedValue === "string" && publishedValue.trim()) {
      postingDate = publishedValue.trim();
    } else if (typeof publishedValue === "number" && Number.isFinite(publishedValue) && publishedValue > 0) {
      postingDate = new Date(publishedValue).toISOString();
    }

    const locationIds = Array.isArray(offer?.locationIds) ? offer.locationIds : [];
    const locationNames = locationIds
      .map((locationId) => locationById.get(String(locationId ?? "").trim()) || "")
      .filter(Boolean);

    const location = locationNames.length > 0 ? locationNames.join(" / ") : null;
    collected.push({
      company_name: companyNameForPostings,
      position_name: extractRecruiteeTitle(offer, preferredLangCode),
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location
    });

    if (!postingDate && looksLikeIndiaLocation(location)) {
      indiaPostingIndices.push(collected.length - 1);
    }
  }

  if (indiaPostingIndices.length > 0) {
    const DETAIL_BATCH_SIZE = 5;
    for (let i = 0; i < indiaPostingIndices.length; i += DETAIL_BATCH_SIZE) {
      const batch = indiaPostingIndices.slice(i, i + DETAIL_BATCH_SIZE);
      const results = await Promise.allSettled(
        batch.map((idx) => fetchRecruiteePublishedDate(collected[idx].job_posting_url))
      );
      for (let j = 0; j < results.length; j += 1) {
        if (results[j].status === "fulfilled" && results[j].value) {
          collected[batch[j]].posting_date = results[j].value;
        }
      }
    }
  }

  return collected;
}

function extractUltiProLocationName(opportunity) {
  const locations = Array.isArray(opportunity?.Locations) ? opportunity.Locations : [];
  const values = [];
  const seen = new Set();

  for (const location of locations) {
    const item = location && typeof location === "object" ? location : {};
    const address = item.Address && typeof item.Address === "object" ? item.Address : {};
    const city = String(address.City || "").trim();
    const state = String(address?.State?.Code || "").trim();
    const country = String(address?.Country?.Name || "").trim();
    const fallback = String(item.LocalizedDescription || item.LocalizedName || "").trim();

    const cityState = [city, state].filter(Boolean).join(", ");
    let label = "";
    if (cityState && country) {
      label = `${cityState}, ${country}`;
    } else if (cityState) {
      label = cityState;
    } else if (fallback) {
      label = fallback;
    } else if (country) {
      label = country;
    }

    const normalized = label.toLowerCase();
    if (!label || seen.has(normalized)) continue;
    seen.add(normalized);
    values.push(label);
  }

  return values.length > 0 ? values.join(" / ") : null;
}

async function collectPostingsForUltiProCompany(company) {
  const config = parseUltiProCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.tenantLower;
  const postings = [];
  const seenIds = new Set();
  let skip = 0;

  for (let page = 0; page < MAX_PAGES_PER_COMPANY; page += 1) {
    const response = await fetchUltiProSearchResults(config, ULTIPRO_PAGE_SIZE, skip);
    const opportunities = Array.isArray(response?.opportunities) ? response.opportunities : [];
    if (opportunities.length === 0) break;

    for (const opportunity of opportunities) {
      const opportunityId = String(opportunity?.Id || "").trim();
      if (!opportunityId || seenIds.has(opportunityId)) continue;

      postings.push({
        company_name: companyNameForPostings,
        position_name: String(opportunity?.Title || "").trim() || "Untitled Position",
        job_posting_url: `${config.baseBoardUrl}/OpportunityDetail?opportunityId=${encodeURIComponent(opportunityId)}`,
        posting_date: String(opportunity?.PostedDate || "").trim() || null,
        location: extractUltiProLocationName(opportunity)
      });
      seenIds.add(opportunityId);
    }

    const totalCount = Number(response?.totalCount);
    if (opportunities.length < ULTIPRO_PAGE_SIZE) break;
    if (Number.isFinite(totalCount) && skip + ULTIPRO_PAGE_SIZE >= totalCount) break;
    skip += ULTIPRO_PAGE_SIZE;
  }

  return postings;
}

async function fetchWorkableJobBoard(slug) {
  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const encodedSlug = encodeURIComponent(slug);
      const res = await fetch(`${WORKABLE_API_URL_BASE}/${encodedSlug}`, {
        method: "GET",
        headers: {
          Accept: "application/json"
        },
        signal: controller.signal
      });

      if (res.status === 429) {
        await sleep(WORKABLE_RATE_LIMIT_WAIT_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Workable request failed (${res.status}): ${body.slice(0, 180)}`);
      }

      return res.json();
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function fetchOracleCloudPage(apiUrl, siteNumber, limit, offset) {
  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const url =
        `${apiUrl}?finder=findReqs;siteNumber=${encodeURIComponent(siteNumber)}` +
        `,limit=${limit},offset=${offset}` +
        `&expand=requisitionList&onlyData=true`;
      const res = await fetch(url, {
        method: "GET",
        headers: { Accept: "application/json" },
        signal: controller.signal
      });

      if (res.status === 429) {
        await sleep(ORACLECLOUD_RATE_LIMIT_WAIT_MS);
        continue;
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`OracleCloud request failed (${res.status}): ${body.slice(0, 180)}`);
      }

      return res.json();
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function collectPostingsForOracleCloudCompany(company) {
  const config = parseOracleCloudCompany(company.url_string);
  if (!config) return [];

  const collected = [];
  const seenIds = new Set();

  for (let page = 0; page < ORACLECLOUD_MAX_PAGES; page += 1) {
    const offset = page * ORACLECLOUD_PAGE_SIZE;
    const response = await fetchOracleCloudPage(
      config.apiUrl,
      config.siteNumber,
      ORACLECLOUD_PAGE_SIZE,
      offset
    );

    const searchItem = Array.isArray(response?.items) ? response.items[0] : null;
    if (!searchItem) break;

    const requisitions = Array.isArray(searchItem.requisitionList)
      ? searchItem.requisitionList
      : [];
    if (requisitions.length === 0) break;

    for (const req of requisitions) {
      const reqId = String(req?.Id || "").trim();
      if (!reqId || seenIds.has(reqId)) continue;
      seenIds.add(reqId);

      const jobUrl = `${config.jobBaseUrl}/${reqId}`;
      const location = String(req?.PrimaryLocation || "").trim() || null;

      collected.push({
        company_name: company.company_name,
        position_name: String(req?.Title || "").trim() || "Untitled Position",
        job_posting_url: jobUrl,
        posting_date: String(req?.PostedDate || "").trim() || null,
        location
      });
    }

    const totalJobs = Number(searchItem?.TotalJobsCount || 0);
    if (offset + requisitions.length >= totalJobs) break;
  }

  return collected;
}

function extractWorkableLocationName(job) {
  const locations = Array.isArray(job?.locations) ? job.locations : [];
  if (locations.length > 0) {
    const parts = locations.map((loc) => {
      const city = String(loc?.city || "").trim();
      const region = String(loc?.region || "").trim();
      const country = String(loc?.country || "").trim();
      return [city, region, country].filter(Boolean).join(", ");
    }).filter(Boolean);
    if (parts.length > 0) return parts.join(" / ");
  }

  const city = String(job?.city || "").trim();
  const state = String(job?.state || "").trim();
  const country = String(job?.country || "").trim();
  const values = [city, state, country].filter(Boolean);
  return values.length > 0 ? values.join(", ") : null;
}

async function collectPostingsForWorkableCompany(company) {
  const config = parseWorkableCompany(company.url_string);
  if (!config) return [];

  const response = await fetchWorkableJobBoard(config.slug);
  const jobPostings = Array.isArray(response?.jobs) ? response.jobs : [];
  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || String(response?.name || "").trim() || config.slugLower;

  const collected = [];
  for (const job of jobPostings) {
    const shortcode = String(job?.shortcode || "").trim();
    if (!shortcode) continue;

    const jobUrl = String(job?.url || job?.shortlink || "").trim()
      || `https://apply.workable.com/${encodeURIComponent(config.slug)}/j/${encodeURIComponent(shortcode)}/`;

    collected.push({
      company_name: companyNameForPostings,
      position_name: String(job?.title || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: String(job?.published_on || job?.created_at || "").trim() || null,
      location: extractWorkableLocationName(job)
    });
  }

  return collected;
}

async function fetchBambooHrJobBoard(config) {
  while (true) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      const res = await fetch(config.apiUrl, {
        method: "GET",
        headers: { Accept: "application/json, text/plain, */*" },
        signal: controller.signal,
        redirect: "manual"
      });

      if (res.status === 301 || res.status === 302) {
        const redirectUrl = String(res.headers.get("location") || "").toLowerCase();
        if (!redirectUrl.includes(".bamboohr.com")) {
          throw new Error(`BambooHR redirected to non-BambooHR host: ${redirectUrl.slice(0, 120)}`);
        }
      }
      if (res.status === 429) {
        await sleep(BAMBOOHR_RATE_LIMIT_WAIT_MS);
        continue;
      }
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`BambooHR API request failed (${res.status}): ${body.slice(0, 180)}`);
      }
      return res.json();
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function fetchBambooHrPostingDate(config, jobId) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const detailUrl = `${config.boardUrl}/${encodeURIComponent(jobId)}/detail`;
    const res = await fetch(detailUrl, {
      method: "GET",
      headers: { Accept: "application/json, text/plain, */*" },
      signal: controller.signal
    });
    if (!res.ok) return null;
    const data = await res.json();
    const datePosted = String(data?.result?.jobOpening?.datePosted || "").trim();
    return datePosted || null;
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

function extractBambooHrLocationName(item) {
  const loc = item?.location && typeof item.location === "object" ? item.location : {};
  const atsLoc = item?.atsLocation && typeof item.atsLocation === "object" ? item.atsLocation : {};

  const city = String(loc?.city || atsLoc?.city || "").trim();
  const state = String(loc?.state || atsLoc?.state || atsLoc?.province || "").trim();
  const country = String(atsLoc?.country || "").trim();
  const parts = [city, state, country].filter(Boolean);
  if (parts.length > 0) return parts.join(", ");

  if (item?.isRemote) return "Remote";
  return null;
}

async function collectPostingsForBambooHrCompany(company) {
  const config = parseBambooHrCompany(company.url_string);
  if (!config) return [];

  const responseJson = await fetchBambooHrJobBoard(config);
  const result = Array.isArray(responseJson?.result) ? responseJson.result : [];
  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.companySubdomainLower;

  const collected = [];
  const indiaPostingIndices = [];

  for (const item of result) {
    const postingId = String(item?.id || "").trim();
    if (!postingId) continue;

    const jobUrl = `${config.boardUrl}/${encodeURIComponent(postingId)}`;
    const location = extractBambooHrLocationName(item);

    const entry = {
      company_name: companyNameForPostings,
      position_name: String(item?.jobOpeningName || item?.title || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: null,
      location
    };
    collected.push(entry);

    if (looksLikeIndiaLocation(location)) {
      indiaPostingIndices.push({ index: collected.length - 1, jobId: postingId });
    }
  }

  if (indiaPostingIndices.length > 0) {
    const DETAIL_BATCH_SIZE = 5;
    for (let i = 0; i < indiaPostingIndices.length; i += DETAIL_BATCH_SIZE) {
      const batch = indiaPostingIndices.slice(i, i + DETAIL_BATCH_SIZE);
      const results = await Promise.allSettled(
        batch.map((item) => fetchBambooHrPostingDate(config, item.jobId))
      );
      for (let j = 0; j < results.length; j += 1) {
        if (results[j].status === "fulfilled" && results[j].value) {
          collected[batch[j].index].posting_date = results[j].value;
        }
      }
    }
  }

  return collected;
}

async function collectPostingsForCompany(company) {
  const atsName = String(company?.ATS_name || "").trim().toLowerCase();
  if (atsName === "workday") {
    return collectTodayPostingsForWorkdayCompany(company);
  }
  if (atsName === "ashbyhq") {
    return collectPostingsForAshbyCompany(company);
  }
  if (atsName === "greenhouseio" || atsName === "greenhouse.io" || atsName === "greenhouse") {
    return collectPostingsForGreenhouseCompany(company);
  }
  if (atsName === "leverco" || atsName === "lever.co" || atsName === "lever") {
    return collectPostingsForLeverCompany(company);
  }
  if (atsName === "icims" || atsName === "icims.com" || atsName === "icimscom") {
    return collectPostingsForIcimsCompany(company);
  }
  if (atsName === "recruiteecom" || atsName === "recruitee.com" || atsName === "recruitee") {
    return collectPostingsForRecruiteeCompany(company);
  }
  if (atsName === "ultipro" || atsName === "ukg") {
    return collectPostingsForUltiProCompany(company);
  }
  if (atsName === "oraclecloud") {
    return collectPostingsForOracleCloudCompany(company);
  }
  if (atsName === "workable") {
    return collectPostingsForWorkableCompany(company);
  }
  if (atsName === "bamboohr") {
    return collectPostingsForBambooHrCompany(company);
  }
  return [];
}

async function ensureCompaniesTableSchema() {
  const tableInfo = await db.all(`PRAGMA table_info('companies');`);
  const columns = new Set(tableInfo.map((column) => String(column?.name || "")));
}

// --------------- CSV seed utilities ---------------

function parseCsvLine(line) {
  const fields = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        fields.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

function loadCsvFile(filename) {
  const filePath = path.join(SEED_DATA_DIR, filename);
  if (!fs.existsSync(filePath)) {
    console.warn(`[seed] data file not found: ${filePath}`);
    return { columns: [], rows: [] };
  }
  const content = fs.readFileSync(filePath, "utf8");
  const lines = content.split("\n").filter((line) => line.trim() !== "");
  if (lines.length === 0) return { columns: [], rows: [] };
  const columns = parseCsvLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const values = parseCsvLine(lines[i]);
    const row = {};
    for (let j = 0; j < columns.length; j++) {
      row[columns[j]] = values[j] !== undefined ? values[j] : "";
    }
    rows.push(row);
  }
  return { columns, rows };
}

async function seedTableFromCsv(tableName, csvFilename, createTableSql, options = {}) {
  // Ensure table exists
  await db.exec(createTableSql);

  // Check if already populated
  const result = await db.get(`SELECT COUNT(*) as c FROM ${tableName};`);
  const count = Number(result?.c || 0);
  if (count > 0) {
    console.log(`[seed] ${tableName}: already has ${count} rows, skipping.`);
    return;
  }

  const { columns, rows } = loadCsvFile(csvFilename);
  if (rows.length === 0) {
    console.warn(`[seed] ${tableName}: no data in ${csvFilename}, skipping.`);
    return;
  }

  console.log(`[seed] ${tableName}: loading ${rows.length} rows from ${csvFilename}...`);

  const CHUNK_SIZE = options.chunkSize || 200;
  const insertColumns = columns;

  await db.exec("BEGIN TRANSACTION;");
  try {
    for (let offset = 0; offset < rows.length; offset += CHUNK_SIZE) {
      const chunk = rows.slice(offset, offset + CHUNK_SIZE);
      const placeholders = chunk
        .map(() => "(" + insertColumns.map(() => "?").join(", ") + ")")
        .join(", ");
      const params = [];
      for (const row of chunk) {
        for (const col of insertColumns) {
          let val = row[col];
          if (val === "" || val === undefined) {
            val = null;
          } else if (options.numericColumns?.has(col)) {
            val = Number(val);
          }
          params.push(val);
        }
      }
      await db.run(
        `INSERT INTO ${tableName} (${insertColumns.join(", ")}) VALUES ${placeholders};`,
        params
      );
    }
    await db.exec("COMMIT;");
    console.log(`[seed] ${tableName}: loaded ${rows.length} rows.`);
  } catch (error) {
    await db.exec("ROLLBACK;");
    console.error(`[seed] ${tableName}: failed to load:`, error);
    throw error;
  }
}

async function seedCompanies() {
  await seedTableFromCsv(
    "companies",
    "companies.csv",
    `CREATE TABLE IF NOT EXISTS companies (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      company_name TEXT NOT NULL,
      url_string TEXT NOT NULL,
      ATS_name TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_url_string ON companies(url_string);
    CREATE INDEX IF NOT EXISTS idx_companies_company_name ON companies(company_name);
    CREATE INDEX IF NOT EXISTS idx_companies_ats_name ON companies(ATS_name);`,
    { numericColumns: new Set(["id"]) }
  );
}

async function seedJobIndustryCategories() {
  await seedTableFromCsv(
    "job_industry_categories",
    "job_industry_categories.csv",
    `CREATE TABLE IF NOT EXISTS job_industry_categories (
      id INTEGER NOT NULL PRIMARY KEY,
      industry_key TEXT NOT NULL,
      industry_label TEXT NOT NULL,
      priority INTEGER,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );`,
    { numericColumns: new Set(["id", "priority"]) }
  );
}

async function seedJobPositionIndustry() {
  await seedTableFromCsv(
    "job_position_industry",
    "job_position_industry.csv",
    `CREATE TABLE IF NOT EXISTS job_position_industry (
      id INTEGER NOT NULL PRIMARY KEY,
      job_title TEXT,
      normalized_job_title TEXT,
      industry_key TEXT,
      industry_label TEXT,
      matched_rules TEXT,
      confidence_score REAL,
      rule_version TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );`,
    { numericColumns: new Set(["id", "confidence_score"]), chunkSize: 500 }
  );
}

async function seedStateLocationIndex() {
  await seedTableFromCsv(
    "state_location_index",
    "state_location_index.csv",
    `CREATE TABLE IF NOT EXISTS state_location_index (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      location_type TEXT NOT NULL CHECK (location_type IN ('city', 'state')),
      state_usps TEXT NOT NULL,
      state_geoid TEXT,
      location_geoid TEXT NOT NULL,
      ansicode TEXT,
      location_name TEXT NOT NULL,
      search_location_name TEXT NOT NULL,
      normalized_location_name TEXT NOT NULL,
      normalized_search_location_name TEXT NOT NULL,
      lsad_code TEXT,
      funcstat TEXT,
      aland INTEGER,
      awater INTEGER,
      aland_sqmi REAL,
      awater_sqmi REAL,
      intptlat REAL,
      intptlong REAL,
      source_file TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(location_type, location_geoid)
    );`,
    {
      numericColumns: new Set([
        "id", "aland", "awater", "aland_sqmi", "awater_sqmi", "intptlat", "intptlong"
      ])
    }
  );
}

// --------------- end CSV seed utilities ---------------

async function initDb() {
  db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database
  });

  await db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA cache_size = -64000;
    PRAGMA mmap_size = 268435456;
    PRAGMA temp_store = MEMORY;
    PRAGMA busy_timeout = 5000;
    PRAGMA page_size = 4096;
  `);

  // Seed reference tables from CSV files (creates tables if needed, skips if populated)
  await seedCompanies();
  await seedJobIndustryCategories();
  await seedJobPositionIndustry();
  await seedStateLocationIndex();

  await ensurePostingsTable();
  await ensurePersonalInformationTable();
  await ensureApplicationsTable();
  await ensureCompaniesTableSchema();
}

async function createCanonicalPostingsTable() {
  await db.exec(`
    CREATE TABLE Postings (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      company_name TEXT NOT NULL,
      position_name TEXT NOT NULL,
      job_posting_url TEXT NOT NULL UNIQUE,
      posting_date TEXT,
      last_seen_epoch INTEGER,
      location TEXT,
      first_seen_epoch INTEGER,
      posting_date_epoch INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_postings_company_name
      ON Postings(company_name);

    CREATE INDEX IF NOT EXISTS idx_postings_position_name
      ON Postings(position_name);

    CREATE INDEX IF NOT EXISTS idx_postings_last_seen_epoch
      ON Postings(last_seen_epoch);

    CREATE INDEX IF NOT EXISTS idx_postings_posting_date_epoch
      ON Postings(posting_date_epoch);
  `);
}

async function ensurePostingsTable() {
  const tableInfo = await db.all(`PRAGMA table_info('Postings');`);

  if (!Array.isArray(tableInfo) || tableInfo.length === 0) {
    await createCanonicalPostingsTable();
    return;
  }

  const requiredColumns = new Set(["id", "company_name", "position_name", "job_posting_url", "posting_date"]);
  const existingColumns = new Set(tableInfo.map((column) => String(column.name)));
  const requiredPresent = Array.from(requiredColumns).every((column) => existingColumns.has(column));

  let incompatibleExtraRequiredColumns = false;
  for (const column of tableInfo) {
    const name = String(column.name);
    if (requiredColumns.has(name)) continue;
    if (Number(column.notnull) === 1 && column.dflt_value === null) {
      incompatibleExtraRequiredColumns = true;
      break;
    }
  }

  if (!requiredPresent || incompatibleExtraRequiredColumns) {
    await db.exec(`DROP TABLE IF EXISTS Postings;`);
    await createCanonicalPostingsTable();
    return;
  }

  if (!existingColumns.has("last_seen_epoch")) {
    await db.exec(`ALTER TABLE Postings ADD COLUMN last_seen_epoch INTEGER;`);
    await db.run(`UPDATE Postings SET last_seen_epoch = ? WHERE last_seen_epoch IS NULL;`, [nowEpochSeconds()]);
  }

  if (!existingColumns.has("location")) {
    await db.exec(`ALTER TABLE Postings ADD COLUMN location TEXT;`);
  }

  if (!existingColumns.has("first_seen_epoch")) {
    await db.exec(`ALTER TABLE Postings ADD COLUMN first_seen_epoch INTEGER;`);
    await db.run(`UPDATE Postings SET first_seen_epoch = last_seen_epoch WHERE first_seen_epoch IS NULL;`);
  }

  if (!existingColumns.has("posting_date_epoch")) {
    await db.exec(`ALTER TABLE Postings ADD COLUMN posting_date_epoch INTEGER;`);
  }

  await db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_postings_job_posting_url
      ON Postings(job_posting_url);

    CREATE INDEX IF NOT EXISTS idx_postings_company_name
      ON Postings(company_name);

    CREATE INDEX IF NOT EXISTS idx_postings_position_name
      ON Postings(position_name);

    CREATE INDEX IF NOT EXISTS idx_postings_last_seen_epoch
      ON Postings(last_seen_epoch);

    CREATE INDEX IF NOT EXISTS idx_postings_location
      ON Postings(location);

    CREATE INDEX IF NOT EXISTS idx_postings_first_seen_epoch
      ON Postings(first_seen_epoch);

    CREATE INDEX IF NOT EXISTS idx_postings_posting_date_epoch
      ON Postings(posting_date_epoch);
  `);
}

async function ensurePersonalInformationTable() {
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
      years_of_experience INTEGER NOT NULL
    );
  `);

  const tableInfo = await db.all(`PRAGMA table_info('PersonalInformation');`);
  const existingColumns = new Set(tableInfo.map((column) => String(column?.name || "")));

  if (!existingColumns.has("years_of_experience")) {
    await db.exec(`
      ALTER TABLE PersonalInformation
      ADD COLUMN years_of_experience INTEGER NOT NULL DEFAULT 0;
    `);
  }
}

async function ensureApplicationsTable() {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS applications (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      position_name TEXT NOT NULL,
      application_date INTEGER NOT NULL,
      status TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_applications_company_id
      ON applications(company_id);

    CREATE INDEX IF NOT EXISTS idx_applications_application_date
      ON applications(application_date);

    CREATE INDEX IF NOT EXISTS idx_applications_status
      ON applications(status);

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

    CREATE INDEX IF NOT EXISTS idx_posting_application_state_applied
      ON posting_application_state(applied);

    CREATE INDEX IF NOT EXISTS idx_posting_application_state_ignored
      ON posting_application_state(ignored);

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
      instructions_for_agent TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);

  await db.run(
    `
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
        instructions_for_agent
      ) VALUES (1, 0, ?, '', '', '', 1, 1, 10, '', 'all', '[]', '[]', '')
      ON CONFLICT(id) DO NOTHING;
    `,
    [MCP_SETTINGS_DEFAULTS.preferred_agent_name]
  );

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
  await db.exec(`
    CREATE INDEX IF NOT EXISTS idx_posting_application_state_ignored
      ON posting_application_state(ignored);
  `);

  if (!mcpSettingsColumnNames.has("agent_login_password")) {
    await db.exec(`
      ALTER TABLE McpSettings
      ADD COLUMN agent_login_password TEXT NOT NULL DEFAULT '';
    `);
  }
}

async function getMcpSettings() {
  const row = await db.get(
    `
      SELECT
        id,
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
        instructions_for_agent
      FROM McpSettings
      WHERE id = 1
      LIMIT 1;
    `
  );

  const settings = normalizeMcpSettingsInput({
    ...MCP_SETTINGS_DEFAULTS,
    enabled: Boolean(Number(row?.enabled || 0)),
    preferred_agent_name: row?.preferred_agent_name,
    agent_login_email: row?.agent_login_email,
    agent_login_password: row?.agent_login_password,
    mfa_login_email: row?.mfa_login_email,
    mfa_login_notes: row?.mfa_login_notes,
    dry_run_only: Boolean(Number(row?.dry_run_only ?? 1)),
    require_final_approval: Boolean(Number(row?.require_final_approval ?? 1)),
    max_applications_per_run: row?.max_applications_per_run,
    preferred_search: row?.preferred_search,
    preferred_remote: row?.preferred_remote,
    preferred_industries: parseJsonArray(row?.preferred_industries),
    preferred_states: parseJsonArray(row?.preferred_states),
    instructions_for_agent: row?.instructions_for_agent
  });

  return settings;
}

async function upsertMcpSettings(input) {
  const normalized = normalizeMcpSettingsInput(input);
  await db.run(
    `
      INSERT INTO McpSettings (
        id,
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
        instructions_for_agent,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(id) DO UPDATE SET
        enabled = excluded.enabled,
        preferred_agent_name = excluded.preferred_agent_name,
        agent_login_email = excluded.agent_login_email,
        agent_login_password = excluded.agent_login_password,
        mfa_login_email = excluded.mfa_login_email,
        mfa_login_notes = excluded.mfa_login_notes,
        dry_run_only = excluded.dry_run_only,
        require_final_approval = excluded.require_final_approval,
        max_applications_per_run = excluded.max_applications_per_run,
        preferred_search = excluded.preferred_search,
        preferred_remote = excluded.preferred_remote,
        preferred_industries = excluded.preferred_industries,
        preferred_states = excluded.preferred_states,
        instructions_for_agent = excluded.instructions_for_agent,
        updated_at = datetime('now');
    `,
    [
      1,
      normalized.enabled ? 1 : 0,
      normalized.preferred_agent_name,
      normalized.agent_login_email,
      normalized.agent_login_password,
      normalized.mfa_login_email,
      normalized.mfa_login_notes,
      normalized.dry_run_only ? 1 : 0,
      normalized.require_final_approval ? 1 : 0,
      normalized.max_applications_per_run,
      normalized.preferred_search,
      normalized.preferred_remote,
      JSON.stringify(normalized.preferred_industries || []),
      JSON.stringify(normalized.preferred_states || []),
      normalized.instructions_for_agent
    ]
  );

  return getMcpSettings();
}

async function markPostingAppliedState(payload) {
  const jobPostingUrl = String(payload?.job_posting_url || "").trim();
  if (!jobPostingUrl) return;

  const applied = normalizeBoolean(payload?.applied, true);
  const appliedByType = normalizeAppliedByType(payload?.applied_by_type);
  const appliedByLabel = normalizeAppliedByLabel(payload?.applied_by_label, appliedByType);
  const appliedAtEpoch = parseNonNegativeInteger(payload?.applied_at_epoch) || nowEpochSeconds();
  const lastApplicationId = parseNonNegativeInteger(payload?.last_application_id) || null;

  await db.run(
    `
      INSERT INTO posting_application_state (
        job_posting_url,
        applied,
        applied_by_type,
        applied_by_label,
        applied_at_epoch,
        last_application_id,
        ignored,
        ignored_at_epoch,
        ignored_by_label,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, '', datetime('now'))
      ON CONFLICT(job_posting_url) DO UPDATE SET
        applied = excluded.applied,
        applied_by_type = excluded.applied_by_type,
        applied_by_label = excluded.applied_by_label,
        applied_at_epoch = excluded.applied_at_epoch,
        last_application_id = excluded.last_application_id,
        ignored = 0,
        ignored_at_epoch = NULL,
        ignored_by_label = '',
        updated_at = datetime('now');
    `,
    [jobPostingUrl, applied ? 1 : 0, appliedByType, appliedByLabel, appliedAtEpoch, lastApplicationId]
  );
}

async function setPostingIgnoredState(payload) {
  const jobPostingUrl = String(payload?.job_posting_url || "").trim();
  if (!jobPostingUrl) {
    throw new Error("job_posting_url is required");
  }

  const ignored = normalizeBoolean(payload?.ignored, true);
  const ignoredAtEpoch = parseNonNegativeInteger(payload?.ignored_at_epoch) || nowEpochSeconds();
  const ignoredByLabel = normalizeIgnoredByLabel(payload?.ignored_by_label);

  await db.run(
    `
      INSERT INTO posting_application_state (
        job_posting_url,
        applied,
        applied_by_type,
        applied_by_label,
        applied_at_epoch,
        last_application_id,
        ignored,
        ignored_at_epoch,
        ignored_by_label,
        updated_at
      ) VALUES (?, 0, 'manual', '', NULL, NULL, ?, ?, ?, datetime('now'))
      ON CONFLICT(job_posting_url) DO UPDATE SET
        ignored = excluded.ignored,
        ignored_at_epoch = CASE
          WHEN excluded.ignored = 1 THEN excluded.ignored_at_epoch
          ELSE NULL
        END,
        ignored_by_label = CASE
          WHEN excluded.ignored = 1 THEN excluded.ignored_by_label
          ELSE ''
        END,
        updated_at = datetime('now');
    `,
    [jobPostingUrl, ignored ? 1 : 0, ignoredAtEpoch, ignoredByLabel]
  );

  const row = await db.get(
    `
      SELECT
        job_posting_url,
        applied,
        ignored,
        ignored_at_epoch,
        ignored_by_label
      FROM posting_application_state
      WHERE job_posting_url = ?
      LIMIT 1;
    `,
    [jobPostingUrl]
  );

  return {
    job_posting_url: jobPostingUrl,
    applied: Boolean(Number(row?.applied || 0)),
    ignored: Boolean(Number(row?.ignored || 0)),
    ignored_at_epoch: Number(row?.ignored_at_epoch || 0),
    ignored_by_label: String(row?.ignored_by_label || "")
  };
}

async function enrichPostingsWithApplicationState(items) {
  const rows = Array.isArray(items) ? items : [];
  const urls = rows
    .map((row) => String(row?.job_posting_url || "").trim())
    .filter(Boolean);
  if (urls.length === 0) return rows;

  const uniqueUrls = Array.from(new Set(urls));
  const placeholders = uniqueUrls.map(() => "?").join(", ");
  const stateRows = await db.all(
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
    uniqueUrls
  );

  const byUrl = new Map();
  for (const row of stateRows) {
    byUrl.set(String(row?.job_posting_url || "").trim(), row);
  }

  return rows.map((item) => {
    const key = String(item?.job_posting_url || "").trim();
    const state = byUrl.get(key);
    const applied = Boolean(Number(state?.applied || 0));
    const ignored = Boolean(Number(state?.ignored || 0));
    const appliedByType = applied ? normalizeAppliedByType(state?.applied_by_type) : "";
    return {
      ...item,
      applied,
      ignored,
      applied_by_type: appliedByType,
      applied_by_label: applied ? normalizeAppliedByLabel(state?.applied_by_label, appliedByType) : "",
      applied_at_epoch: Number(state?.applied_at_epoch || 0),
      last_application_id: Number(state?.last_application_id || 0),
      ignored_at_epoch: Number(state?.ignored_at_epoch || 0),
      ignored_by_label: ignored ? normalizeIgnoredByLabel(state?.ignored_by_label) : ""
    };
  });
}

async function listPostingsWithFilters(options = {}) {
  const search = String(options?.search || "").trim();
  const limit = Math.max(1, Math.min(2000, Number(options?.limit || 500)));
  const offset = Math.max(0, Number(options?.offset || 0));
  const sortBy = normalizePostingSort(options?.sort_by);
  const orderByClause = getPostingsOrderByClause(sortBy);
  const atsFilters = normalizeAtsFilters(options?.ats || []);
  const industryKeys = normalizeStringArray(options?.industries).map((key) => normalizeLikeText(key));
  const stateCodes = normalizeStringArray(options?.states).map((state) => state.toUpperCase());
  const remoteFilter = normalizeRemoteFilter(options?.remote);
  const includeApplied = normalizeBoolean(options?.include_applied, true);
  const includeIgnored = normalizeBoolean(options?.include_ignored, false);
  const hasStructuredFilters =
    atsFilters.length > 0 ||
    industryKeys.length > 0 ||
    stateCodes.length > 0 ||
    remoteFilter !== "all";

  function buildIndiaClause(col) {
    const cities = [
      "bengaluru", "bangalore", "mumbai", "hyderabad", "chennai",
      "pune", "gurugram", "gurgaon", "noida", "kolkata", "new delhi",
      "jaipur", "ahmedabad", "kochi", "thiruvananthapuram", "coimbatore",
      "lucknow", "chandigarh", "indore", "nagpur", "bhubaneswar",
      "visakhapatnam", "mysuru", "mysore", "mangalore", "mangaluru",
      "trivandrum", "pathankot", "vizag"
    ];
    const cityParts = cities.map((c) => `LOWER(${col}) LIKE '%${c}%'`);
    const indiaParts = [
      `LOWER(${col}) LIKE '% india'`,
      `LOWER(${col}) LIKE '% india %'`,
      `LOWER(${col}) LIKE '% india,%'`,
      `LOWER(${col}) LIKE 'india %'`,
      `LOWER(${col}) LIKE 'india,%'`,
      `LOWER(${col}) = 'india'`,
      `LOWER(${col}) LIKE '%,india'`,
      `LOWER(${col}) LIKE '%, india'`
    ];
    return `AND (${[...cityParts, ...indiaParts].join(" OR ")})`;
  }

  const freshnessCutoff = nowEpochSeconds() - FRESHNESS_WINDOW_SECONDS;

  let rows = [];
  if (!search && !hasStructuredFilters) {
    if (includeApplied && includeIgnored) {
      rows = await db.all(
        `
          SELECT id, company_name, position_name, job_posting_url, posting_date, last_seen_epoch, location
          FROM Postings
          WHERE location IS NOT NULL
            AND COALESCE(posting_date_epoch, first_seen_epoch, last_seen_epoch, 0) >= ?
            ${buildIndiaClause("location")}
          ORDER BY ${orderByClause}
          LIMIT ? OFFSET ?;
        `,
        [freshnessCutoff, limit, offset]
      );
    } else {
      rows = await db.all(
        `
          SELECT p.id, p.company_name, p.position_name, p.job_posting_url, p.posting_date, p.last_seen_epoch, p.location
          FROM Postings p
          LEFT JOIN posting_application_state s
            ON s.job_posting_url = p.job_posting_url
            AND (
              (${includeApplied ? 0 : 1} = 1 AND COALESCE(s.applied, 0) = 1)
              OR
              (${includeIgnored ? 0 : 1} = 1 AND COALESCE(s.ignored, 0) = 1)
            )
          WHERE s.job_posting_url IS NULL
            AND p.location IS NOT NULL
            AND COALESCE(p.posting_date_epoch, p.first_seen_epoch, p.last_seen_epoch, 0) >= ?
            ${buildIndiaClause("p.location")}
          ORDER BY ${orderByClause}
          LIMIT ? OFFSET ?;
        `,
        [freshnessCutoff, limit, offset]
      );
    }
  } else {
    rows = await db.all(
      `
        SELECT id, company_name, position_name, job_posting_url, posting_date, last_seen_epoch, location
        FROM Postings
        WHERE location IS NOT NULL
          AND COALESCE(posting_date_epoch, first_seen_epoch, last_seen_epoch, 0) >= ?
          ${buildIndiaClause("location")}
        ORDER BY ${orderByClause};
      `,
      [freshnessCutoff]
    );
  }

  const enrichedRows = rows.map((row) => ({
    ...row,
    location: row.location || inferPostingLocationFromJobUrl(row?.job_posting_url),
    ats: inferAtsFromJobPostingUrl(row?.job_posting_url)
  }));

  const searchTerms = search.toLowerCase().split(/\s+/).filter(Boolean);
  const industryMatchersByKey = await buildIndustryMatchersByKey(industryKeys);

  let items = enrichedRows;
  if (search || hasStructuredFilters) {
    items = enrichedRows.filter((row) => {
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
        industryKeys,
        industryMatchersByKey
      );
      if (!matchesIndustry) return false;

      const matchesLocation = rowMatchesLocationFilters(row?.location, stateCodes);
      if (!matchesLocation) return false;

      const matchesRemote = rowMatchesRemoteFilter(row?.location, remoteFilter);
      if (!matchesRemote) return false;

      return true;
    });
    items = items.slice(offset, offset + limit);
  }

  items = await enrichPostingsWithApplicationState(items);

  if (!includeApplied) {
    items = items.filter((item) => !item.applied);
  }
  if (!includeIgnored) {
    items = items.filter((item) => !item.ignored);
  }

  return {
    items,
    count: items.length,
    limit,
    offset,
    filters: {
      search,
      ats: atsFilters,
      sort_by: sortBy,
      industries: industryKeys,
      states: stateCodes,
      remote: remoteFilter,
      include_ignored: includeIgnored
    }
  };
}

function buildMcpRunbook(settings, personalInformation, candidates) {
  const preferredAgent = String(settings?.preferred_agent_name || "OpenPostings Agent").trim();
  const applicantFullName = [
    String(personalInformation?.first_name || "").trim(),
    String(personalInformation?.middle_name || "").trim(),
    String(personalInformation?.last_name || "").trim()
  ]
    .filter(Boolean)
    .join(" ");

  return {
    preferred_agent_name: preferredAgent,
    summary:
      "Use your existing browser/web automation tools to open each job URL, complete the application form, and submit only when allowed by settings and credentials.",
    steps: [
      "Read applicantee information and MCP settings from this payload.",
      "For each candidate posting, open job_posting_url and validate role relevance before applying.",
      "Fill application fields using applicantee information. Keep applicant email separate from agent login email.",
      "If an account or MFA is required, use agent_login_email + agent_login_password for account creation and sign-in flows.",
      "Use the same agent_login_email for MFA/approval flows when required.",
      "Draft a job-specific cover letter aligned to the posting requirements and applicant background.",
      "If dry_run_only is true, stop before final submit and return a dry-run result.",
      "When application is submitted, call record_application_result with commit=true to write outcomes."
    ],
    guardrails: {
      dry_run_only: Boolean(settings?.dry_run_only),
      require_final_approval: Boolean(settings?.require_final_approval)
    },
    applicant_display_name: applicantFullName || "Applicant",
    applicant_email: String(personalInformation?.email || "").trim(),
    agent_login_email: String(settings?.agent_login_email || "").trim(),
    agent_login_password: String(settings?.agent_login_password || ""),
    mfa_login_email: String(settings?.agent_login_email || "").trim(),
    mfa_login_notes: String(settings?.mfa_login_notes || "").trim(),
    custom_instructions: String(settings?.instructions_for_agent || "").trim(),
    candidate_count: Array.isArray(candidates) ? candidates.length : 0
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
  const educationLevel = String(personalInformation?.education_level || "").trim();
  const extraInstructions = String(instructions || "").trim();

  const profileDetails = [];
  if (yearsOfExperience > 0) profileDetails.push(`${yearsOfExperience}+ years of relevant experience`);
  if (educationLevel) profileDetails.push(`education in ${educationLevel}`);
  if (linkedinUrl) profileDetails.push(`LinkedIn: ${linkedinUrl}`);
  if (githubUrl) profileDetails.push(`GitHub: ${githubUrl}`);
  if (portfolioUrl) profileDetails.push(`Portfolio: ${portfolioUrl}`);

  const profileSentence =
    profileDetails.length > 0
      ? `My background includes ${profileDetails.join(", ")}.`
      : "I bring hands-on experience delivering high-quality work in fast-moving environments.";

  const instructionSentence = extraInstructions
    ? `I am especially aligned with these priorities: ${extraInstructions}.`
    : "";

  return `Dear Hiring Team,

I am excited to apply for the ${positionName} role at ${companyName}. ${profileSentence}

I am motivated by opportunities where I can contribute quickly, collaborate with a strong team, and improve outcomes for customers and the business. ${instructionSentence}

Thank you for your consideration. I would value the chance to discuss how I can support ${companyName}.

Sincerely,
${fullName}`.trim();
}

async function resolveCompanyIdForApplication(companyName) {
  const normalized = normalizeLikeText(companyName);
  if (!normalized) return null;

  return db.get(
    `
      SELECT id, company_name
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
      SELECT id, company_name
      FROM companies
      WHERE LOWER(company_name) = ?
      ORDER BY id ASC
      LIMIT 1;
    `,
    [normalizedCompanyName]
  );
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

  return getApplicationById(lastApplicationId);
}

function mapApplicationRow(row) {
  if (!row) return null;
  const status = normalizeApplicationStatus(row?.status);
  const appliedByType = normalizeAppliedByType(row?.applied_by_type);
  return {
    id: Number(row?.id || 0),
    company_id: Number(row?.company_id || 0),
    company_name: String(row?.company_name || "").trim(),
    position_name: String(row?.position_name || "").trim(),
    application_date: Number(row?.application_date || 0),
    status,
    applied_by_type: appliedByType,
    applied_by_label: normalizeAppliedByLabel(row?.applied_by_label, appliedByType)
  };
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
      WHERE a.id = ?;
    `,
    [applicationId]
  );

  return mapApplicationRow(row);
}

async function listApplications(options = {}) {
  const limit = Math.max(1, Math.min(2000, Number(options?.limit || 500)));
  const offset = Math.max(0, Number(options?.offset || 0));
  const status = normalizeLikeText(options?.status);

  let rows = [];
  if (status && status !== "all") {
    rows = await db.all(
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
        WHERE LOWER(COALESCE(a.status, '')) = ?
        ORDER BY a.application_date DESC, a.id DESC
        LIMIT ? OFFSET ?;
      `,
      [status, limit, offset]
    );
  } else {
    rows = await db.all(
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
        ORDER BY a.application_date DESC, a.id DESC
        LIMIT ? OFFSET ?;
      `,
      [limit, offset]
    );
  }

  const items = rows.map(mapApplicationRow).filter(Boolean);
  return {
    items,
    count: items.length,
    limit,
    offset
  };
}

async function createApplication(input) {
  const companyName = String(input?.company_name || "").trim();
  const positionName = String(input?.position_name || "").trim();
  const jobPostingUrl = String(input?.job_posting_url || "").trim();
  if (!companyName && !jobPostingUrl) {
    throw new Error("company_name or job_posting_url is required");
  }
  if (!positionName) {
    throw new Error("position_name is required");
  }

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

  const status = normalizeApplicationStatus(input?.status);
  const applicationDate = parseNonNegativeInteger(input?.application_date) || nowEpochSeconds();
  const appliedByType = normalizeAppliedByType(input?.applied_by_type);
  const appliedByLabel = normalizeAppliedByLabel(input?.applied_by_label, appliedByType);

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
      [result.lastID, appliedByType, appliedByLabel]
    );

    if (jobPostingUrl) {
      await markPostingAppliedState({
        job_posting_url: jobPostingUrl,
        applied: true,
        applied_by_type: appliedByType,
        applied_by_label: appliedByLabel,
        applied_at_epoch: applicationDate,
        last_application_id: result.lastID
      });
    }

    await db.exec("COMMIT;");
    return getApplicationById(result.lastID);
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }
}

async function updateApplicationStatus(applicationId, statusValue) {
  const status = normalizeApplicationStatus(statusValue);
  const result = await db.run(
    `
      UPDATE applications
      SET status = ?
      WHERE id = ?;
    `,
    [status, applicationId]
  );

  if (Number(result?.changes || 0) === 0) {
    return null;
  }

  return getApplicationById(applicationId);
}

async function deleteApplicationById(applicationId) {
  await db.exec("BEGIN TRANSACTION;");
  try {
    const trackedPostingRows = await db.all(
      `
        SELECT job_posting_url
        FROM posting_application_state
        WHERE last_application_id = ?;
      `,
      [applicationId]
    );
    const trackedPostingUrls = trackedPostingRows
      .map((row) => String(row?.job_posting_url || "").trim())
      .filter(Boolean);

    await db.run(`DELETE FROM application_attribution WHERE application_id = ?;`, [applicationId]);
    const result = await db.run(`DELETE FROM applications WHERE id = ?;`, [applicationId]);

    for (const jobPostingUrl of trackedPostingUrls) {
      const posting = await db.get(
        `
          SELECT company_name, position_name
          FROM Postings
          WHERE job_posting_url = ?
          LIMIT 1;
        `,
        [jobPostingUrl]
      );

      const companyName = normalizeLikeText(posting?.company_name);
      const positionName = normalizeLikeText(posting?.position_name);

      let replacement = null;
      if (companyName && positionName) {
        replacement = await db.get(
          `
            SELECT
              a.id,
              a.application_date,
              attr.applied_by_type,
              attr.applied_by_label
            FROM applications a
            INNER JOIN companies c
              ON c.id = a.company_id
            LEFT JOIN application_attribution attr
              ON attr.application_id = a.id
            WHERE LOWER(c.company_name) = ?
              AND LOWER(a.position_name) = ?
            ORDER BY a.application_date DESC, a.id DESC
            LIMIT 1;
          `,
          [companyName, positionName]
        );
      }

      if (replacement?.id) {
        const appliedByType = normalizeAppliedByType(replacement?.applied_by_type);
        const appliedByLabel = normalizeAppliedByLabel(replacement?.applied_by_label, appliedByType);
        await db.run(
          `
            UPDATE posting_application_state
            SET
              applied = 1,
              applied_by_type = ?,
              applied_by_label = ?,
              applied_at_epoch = ?,
              last_application_id = ?,
              updated_at = datetime('now')
            WHERE job_posting_url = ?;
          `,
          [
            appliedByType,
            appliedByLabel,
            parseNonNegativeInteger(replacement?.application_date) || nowEpochSeconds(),
            Number(replacement?.id),
            jobPostingUrl
          ]
        );
      } else {
        await db.run(
          `
            UPDATE posting_application_state
            SET
              applied = 0,
              applied_by_type = 'manual',
              applied_by_label = '',
              applied_at_epoch = NULL,
              last_application_id = NULL,
              updated_at = datetime('now')
            WHERE job_posting_url = ?;
          `,
          [jobPostingUrl]
        );
      }
    }

    await db.exec("COMMIT;");
    return Number(result?.changes || 0) > 0;
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }
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

  if (!row) {
    return createDefaultPersonalInformation();
  }

  return normalizePersonalInformationInput(row);
}

async function upsertPersonalInformation(value) {
  const normalized = normalizePersonalInformationInput(value);
  const values = PERSONAL_INFORMATION_FIELDS.map((field) => normalized[field]);
  const updateAssignments = PERSONAL_INFORMATION_FIELDS.map((field) => `${field} = ?`).join(", ");
  const existing = await db.get(
    `
      SELECT rowid
      FROM PersonalInformation
      ORDER BY rowid ASC
      LIMIT 1;
    `
  );

  await db.exec("BEGIN TRANSACTION;");
  try {
    if (existing?.rowid) {
      await db.run(
        `
          UPDATE PersonalInformation
          SET ${updateAssignments}
          WHERE rowid = ?;
        `,
        [...values, existing.rowid]
      );

      await db.run(`DELETE FROM PersonalInformation WHERE rowid <> ?;`, [existing.rowid]);
    } else {
      await db.run(
        `
          INSERT INTO PersonalInformation (${PERSONAL_INFORMATION_FIELDS.join(", ")})
          VALUES (${PERSONAL_INFORMATION_FIELDS.map(() => "?").join(", ")});
        `,
        values
      );
    }

    await db.exec("COMMIT;");
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }

  return normalized;
}

async function getCompaniesForSync() {
  return db.all(
    `
      SELECT id, company_name, url_string, ATS_name
      FROM companies
      WHERE ATS_name IN ('WorkDay', 'AshbyHQ', 'GreenHouse', 'LeverCO', 'icims', 'recruitee', 'ultipro', 'OracleCloud', 'workable', 'bamboohr')
      ORDER BY ATS_name ASC, company_name ASC;
    `
  );
}

async function upsertPostings(postings, lastSeenEpoch) {
  if (!Array.isArray(postings) || postings.length === 0) return;
  const seenEpoch = Number(lastSeenEpoch || nowEpochSeconds());
  const CHUNK_SIZE = 50;

  for (let offset = 0; offset < postings.length; offset += CHUNK_SIZE) {
    const chunk = postings.slice(offset, offset + CHUNK_SIZE);
    const placeholders = chunk.map(() => "(?, ?, ?, ?, ?, ?, ?, ?)").join(", ");
    const params = [];
    for (const posting of chunk) {
      const location = String(posting.location || "").trim() || null;
      const postingDateStr = posting.posting_date || null;
      const postingDateEpoch = parsePostingDateToEpoch(postingDateStr);
      params.push(
        String(posting.company_name || "").trim(),
        String(posting.position_name || "").trim() || "Untitled Position",
        String(posting.job_posting_url || "").trim(),
        postingDateStr,
        seenEpoch,
        location,
        seenEpoch,
        postingDateEpoch
      );
    }

    await db.run(
      `
        INSERT INTO Postings (
          company_name, position_name, job_posting_url, posting_date,
          last_seen_epoch, location, first_seen_epoch, posting_date_epoch
        )
        VALUES ${placeholders}
        ON CONFLICT(job_posting_url) DO UPDATE SET
          company_name = excluded.company_name,
          position_name = excluded.position_name,
          posting_date = excluded.posting_date,
          last_seen_epoch = excluded.last_seen_epoch,
          location = COALESCE(excluded.location, Postings.location),
          posting_date_epoch = COALESCE(excluded.posting_date_epoch, Postings.posting_date_epoch);
      `,
      params
    );
  }
}

async function pruneExpiredPostings(referenceEpoch = nowEpochSeconds()) {
  const cutoffEpoch = Number(referenceEpoch) - POSTING_TTL_SECONDS;
  const result = await db.run(
    `
      DELETE FROM Postings
      WHERE COALESCE(last_seen_epoch, 0) < ?;
    `,
    [cutoffEpoch]
  );
  return Number(result?.changes || 0);
}

async function runWorkdaySyncInternal() {
  syncStatus.running = true;
  syncStatus.started_at = new Date().toISOString();
  syncStatus.progress = { current: 0, total: 0, company_name: "", total_collected: 0 };
  syncStatus.last_error = null;

  try {
    const companies = await getCompaniesForSync();
    shuffleArrayInPlace(companies);
    syncStatus.progress.total = companies.length;
    let totalPruned = await pruneExpiredPostings();
    const nextPostingLocationByJobUrl = new Map();

    const dedupedPostings = new Map();
    const errors = [];
    let completed = 0;
    const workerCount = Math.min(Math.max(1, SYNC_CONCURRENCY), companies.length);

    // Pending postings buffer for batched DB writes
    const pendingPostings = [];
    let flushPromise = Promise.resolve();
    const syncReferenceEpoch = nowEpochSeconds();

    const flushPendingPostings = async (force = false) => {
      if (pendingPostings.length === 0) return;
      if (!force && pendingPostings.length < SYNC_POSTING_FLUSH_BATCH_SIZE) return;
      const batch = pendingPostings.splice(0, pendingPostings.length);
      if (batch.length === 0) return;
      await upsertPostings(batch, syncReferenceEpoch);
    };

    const queueFlush = (force = false) => {
      flushPromise = flushPromise.then(() => flushPendingPostings(force));
      return flushPromise;
    };

    // Worker-stealing pattern: each worker pulls next company from shared index
    let nextCompanyIndex = 0;

    const runSyncWorker = async () => {
      while (true) {
        const idx = nextCompanyIndex;
        if (idx >= companies.length) break;
        nextCompanyIndex += 1;

        const company = companies[idx];
        try {
          const postings = await collectPostingsForCompany(company);
          for (const posting of postings) {
            if (dedupedPostings.has(posting.job_posting_url)) continue;
            dedupedPostings.set(posting.job_posting_url, posting);
            const location = String(posting?.location || "").trim();
            if (location) {
              nextPostingLocationByJobUrl.set(posting.job_posting_url, location);
              postingLocationByJobUrl.set(posting.job_posting_url, location);
            }
            pendingPostings.push(posting);
          }
          if (pendingPostings.length >= SYNC_POSTING_FLUSH_BATCH_SIZE) {
            await queueFlush();
          }
        } catch (err) {
          errors.push({
            company_name: company.company_name,
            message: String(err?.message || err)
          });
        }

        completed += 1;
        syncStatus.progress = {
          current: completed,
          total: companies.length,
          company_name: `${company.company_name} (${company.ATS_name})`,
          total_collected: dedupedPostings.size
        };
      }
    };

    await Promise.all(Array.from({ length: workerCount }, () => runSyncWorker()));

    // Flush any remaining postings
    await queueFlush(true);

    totalPruned += await pruneExpiredPostings();
    postingLocationByJobUrl = nextPostingLocationByJobUrl;

    syncStatus.last_sync_at = new Date().toISOString();
    syncStatus.last_sync_summary = {
      total_companies: companies.length,
      total_postings_stored: dedupedPostings.size,
      failed_companies: errors.length,
      expired_pruned: totalPruned,
      errors: errors.slice(0, 30)
    };
  } catch (error) {
    syncStatus.last_error = String(error?.message || error);
  } finally {
    syncStatus.running = false;
    syncStatus.progress = null;
  }
}

function runWorkdaySync() {
  if (syncPromise) return syncPromise;
  syncPromise = runWorkdaySyncInternal().finally(() => {
    syncPromise = null;
  });
  return syncPromise;
}

async function getCounts() {
  const companyRow = await db.get(`SELECT COUNT(*) AS count FROM companies;`);
  const postingRow = await db.get(`SELECT COUNT(*) AS count FROM Postings;`);
  const byAtsRows = await db.all(`
    SELECT ATS_name, COUNT(*) AS count
    FROM companies
    GROUP BY ATS_name;
  `);

  const companyCountByAts = {};
  for (const row of byAtsRows) {
    const key = String(row?.ATS_name || "").trim() || "Unknown";
    companyCountByAts[key] = Number(row?.count || 0);
  }

  return {
    company_count: Number(companyRow?.count || 0),
    posting_count: Number(postingRow?.count || 0),
    company_count_by_ats: companyCountByAts
  };
}

function createServer() {
  const app = express();
  app.use(cors());
  app.use(express.json());

  const handleSyncRequest = async (req, res) => {
    const wait = String(req.query.wait || "").toLowerCase();
    const shouldWait = wait === "1" || wait === "true";
    const wasRunning = Boolean(syncPromise);
    const promise = runWorkdaySync();

    if (shouldWait) {
      await promise;
      const counts = await getCounts();
      return res.json({
        ok: true,
        started: !wasRunning,
        running: syncStatus.running,
        ...syncStatus,
        ...counts
      });
    }

    return res.status(202).json({
      ok: true,
      started: !wasRunning,
      running: true
    });
  };

  app.get("/health", async (_req, res) => {
    const counts = await getCounts();
    res.json({
      ok: true,
      db_path: DB_PATH,
      ...counts
    });
  });

  app.get("/sync/status", async (_req, res) => {
    const counts = await getCounts();
    res.json({
      ...syncStatus,
      ...counts
    });
  });

  app.post("/sync/workday", handleSyncRequest);
  app.post("/sync/ats", handleSyncRequest);

  app.get("/postings/filter-options", async (req, res) => {
    const ats = [
      { value: "workday", label: "Workday" },
      { value: "ashby", label: "Ashby" },
      { value: "greenhouse", label: "Greenhouse" },
      { value: "lever", label: "Lever" },
      { value: "icims", label: "iCIMS" },
      { value: "recruitee", label: "Recruitee" },
      { value: "ultipro", label: "UltiPro" },
      { value: "oraclecloud", label: "Oracle Cloud" },
      { value: "workable", label: "Workable" },
      { value: "bamboohr", label: "BambooHR" }
    ];
    const sort_options = [
      { value: "recent", label: "Most Recently Seen" },
      { value: "company_asc", label: "Company (A-Z)" }
    ];

    let industries = [];
    try {
      industries = await db.all(
        `
          SELECT industry_key AS value, industry_label AS label
          FROM job_industry_categories
          ORDER BY industry_label ASC;
        `
      );
    } catch {
      industries = await db.all(
        `
          SELECT industry_key AS value, industry_label AS label
          FROM job_position_industry
          GROUP BY industry_key, industry_label
          ORDER BY industry_label ASC;
        `
      );
    }

    let states = [];
    try {
      const stateRows = await db.all(
        `
          SELECT DISTINCT state_usps
          FROM state_location_index
          WHERE state_usps IS NOT NULL AND TRIM(state_usps) <> ''
          ORDER BY state_usps ASC;
        `
      );
      states = stateRows.map((row) => {
        const code = String(row?.state_usps || "").trim().toUpperCase();
        const readableName = STATE_CODE_TO_NAME[code];
        return {
          value: code,
          label: readableName ? `${code} - ${readableName.replace(/\b\w/g, (c) => c.toUpperCase())}` : code
        };
      });
    } catch {
      states = [];
    }

    res.json({
      ats,
      sort_options,
      industries,
      states
    });
  });

  app.get("/settings/personal-information", async (_req, res) => {
    const item = await getPersonalInformation();
    res.json({ item });
  });

  app.put("/settings/personal-information", async (req, res) => {
    const item = await upsertPersonalInformation(req.body);
    res.json({
      ok: true,
      item
    });
  });

  app.get("/settings/mcp", async (_req, res) => {
    const item = await getMcpSettings();
    res.json({ item });
  });

  app.put("/settings/mcp", async (req, res) => {
    const item = await upsertMcpSettings(req.body || {});
    res.json({
      ok: true,
      item
    });
  });

  app.get("/mcp/candidates", async (req, res) => {
    const settings = await getMcpSettings();
    try {
      ensureMcpAgentEnabled(settings);
    } catch (error) {
      return res.status(Number(error?.statusCode || 403)).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
    const personalInformation = await getPersonalInformation();

    const useSettings = normalizeBoolean(req.query.use_settings, true);
    const overrideSearch = String(req.query.search || "").trim();
    const overrideAts = parseCsvParam(req.query.ats);
    const overrideIndustries = parseCsvParam(req.query.industries);
    const overrideStates = parseCsvParam(req.query.states);
    const overrideRemote = normalizeRemoteFilter(req.query.remote);
    const includeApplied = normalizeBoolean(req.query.include_applied, false);

    const preferredMax = Math.max(
      1,
      parseNonNegativeInteger(settings?.max_applications_per_run) || MCP_SETTINGS_DEFAULTS.max_applications_per_run
    );
    const requestedLimit = parseNonNegativeInteger(req.query.limit);
    const limit = Math.max(1, Math.min(2000, requestedLimit || preferredMax));

    const search = overrideSearch || (useSettings ? String(settings?.preferred_search || "").trim() : "");
    const ats = overrideAts.length > 0 ? overrideAts : [];
    const industries =
      overrideIndustries.length > 0
        ? overrideIndustries
        : useSettings
          ? normalizeStringArray(settings?.preferred_industries)
          : [];
    const states =
      overrideStates.length > 0
        ? overrideStates
        : useSettings
          ? normalizeStringArray(settings?.preferred_states)
          : [];
    const remote = req.query.remote ? overrideRemote : useSettings ? settings?.preferred_remote : "all";

    const result = await listPostingsWithFilters({
      search,
      limit,
      offset: 0,
      ats,
      industries,
      states,
      remote,
      include_applied: includeApplied
    });

    const candidates = (result?.items || []).slice(0, limit);
    const runbook = buildMcpRunbook(settings, personalInformation, candidates);

    res.json({
      ok: true,
      count: candidates.length,
      limit,
      filters: result.filters,
      settings,
      personal_information: personalInformation,
      runbook,
      candidates
    });
  });

  app.post("/mcp/cover-letter-draft", async (req, res) => {
    const settings = await getMcpSettings();
    try {
      ensureMcpAgentEnabled(settings);
    } catch (error) {
      return res.status(Number(error?.statusCode || 403)).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
    const personalInformation = await getPersonalInformation();
    const jobPostingUrl = String(req.body?.job_posting_url || "").trim();
    const requestCompanyName = String(req.body?.company_name || "").trim();
    const requestPositionName = String(req.body?.position_name || "").trim();

    let posting = {
      job_posting_url: jobPostingUrl,
      company_name: requestCompanyName,
      position_name: requestPositionName
    };

    if (jobPostingUrl && (!requestCompanyName || !requestPositionName)) {
      const row = await db.get(
        `
          SELECT company_name, position_name, job_posting_url
          FROM Postings
          WHERE job_posting_url = ?
          LIMIT 1;
        `,
        [jobPostingUrl]
      );
      posting = {
        job_posting_url: jobPostingUrl,
        company_name: requestCompanyName || String(row?.company_name || "").trim(),
        position_name: requestPositionName || String(row?.position_name || "").trim()
      };
    }

    const instructions = String(req.body?.instructions || settings?.instructions_for_agent || "").trim();
    const draft = buildCoverLetterDraft(personalInformation, posting, instructions);

    res.json({
      ok: true,
      posting,
      draft
    });
  });

  app.post("/mcp/applications/complete", async (req, res) => {
    try {
      const settings = await getMcpSettings();
      ensureMcpAgentEnabled(settings);
      const commit = normalizeBoolean(req.body?.commit, false);
      const approvedByUser = normalizeBoolean(req.body?.approved_by_user, false);
      const jobPostingUrl = String(req.body?.job_posting_url || "").trim();
      const agentName =
        String(req.body?.agent_name || settings?.preferred_agent_name || MCP_SETTINGS_DEFAULTS.preferred_agent_name)
          .trim() || MCP_SETTINGS_DEFAULTS.preferred_agent_name;

      let companyName = String(req.body?.company_name || "").trim();
      let positionName = String(req.body?.position_name || "").trim();

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

      if (!companyName || !positionName) {
        return res.status(400).json({
          ok: false,
          error: "company_name and position_name are required (or provide a valid job_posting_url)."
        });
      }

      if (commit && settings?.require_final_approval && !approvedByUser) {
        return res.status(400).json({
          ok: false,
          error: "Final approval is required by MCP settings. Set approved_by_user=true to commit."
        });
      }

      const payload = {
        company_name: companyName,
        position_name: positionName,
        job_posting_url: jobPostingUrl,
        application_date: parseNonNegativeInteger(req.body?.application_date) || nowEpochSeconds(),
        status: req.body?.status || "applied",
        applied_by_type: "agent",
        applied_by_label: `${agentName} applied on behalf of user`
      };

      const shouldDryRun = !commit || Boolean(settings?.dry_run_only);
      if (shouldDryRun) {
        return res.json({
          ok: true,
          committed: false,
          dry_run: true,
          payload
        });
      }

      const item = await createApplication(payload);
      return res.status(201).json({
        ok: true,
        committed: true,
        item
      });
    } catch (error) {
      return res.status(Number(error?.statusCode || 400)).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
  });

  app.get("/applications", async (req, res) => {
    const limit = Math.max(1, Math.min(2000, Number(req.query.limit || 500)));
    const offset = Math.max(0, Number(req.query.offset || 0));
    const status = String(req.query.status || "").trim();

    const payload = await listApplications({
      limit,
      offset,
      status
    });

    res.json({
      ...payload,
      status_options: Array.from(APPLICATION_STATUS_OPTIONS)
    });
  });

  app.post("/applications", async (req, res) => {
    try {
      const item = await createApplication(req.body || {});
      res.status(201).json({
        ok: true,
        item
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
  });

  app.patch("/applications/:id", async (req, res) => {
    const applicationId = Number(req.params.id);
    if (!Number.isFinite(applicationId) || applicationId <= 0) {
      return res.status(400).json({
        ok: false,
        error: "application id must be a positive number"
      });
    }

    const item = await updateApplicationStatus(applicationId, req.body?.status);
    if (!item) {
      return res.status(404).json({
        ok: false,
        error: "application not found"
      });
    }

    return res.json({
      ok: true,
      item
    });
  });

  app.delete("/applications/:id", async (req, res) => {
    const applicationId = Number(req.params.id);
    if (!Number.isFinite(applicationId) || applicationId <= 0) {
      return res.status(400).json({
        ok: false,
        error: "application id must be a positive number"
      });
    }

    const deleted = await deleteApplicationById(applicationId);
    if (!deleted) {
      return res.status(404).json({
        ok: false,
        error: "application not found"
      });
    }

    return res.json({
      ok: true,
      deleted: true
    });
  });

  app.post("/postings/ignore", async (req, res) => {
    try {
      const item = await setPostingIgnoredState(req.body || {});
      res.json({
        ok: true,
        item
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
  });

  app.get("/postings", async (req, res) => {
    const result = await listPostingsWithFilters({
      search: String(req.query.search || "").trim(),
      limit: Number(req.query.limit || 500),
      offset: Number(req.query.offset || 0),
      sort_by: String(req.query.sort_by || "").trim(),
      ats: parseCsvParam(req.query.ats),
      industries: parseCsvParam(req.query.industries),
      states: parseCsvParam(req.query.states),
      remote: req.query.remote,
      include_applied: normalizeBoolean(req.query.include_applied, true),
      include_ignored: normalizeBoolean(req.query.include_ignored, false)
    });

    res.json({
      items: result.items,
      count: result.count,
      limit: result.limit,
      offset: result.offset
    });
  });

  return app;
}

async function start() {
  await initDb();

  const app = createServer();
  app.listen(PORT, () => {
    console.log(`[OpenPostings API] listening on http://localhost:${PORT}`);
    console.log(`[OpenPostings API] using database ${DB_PATH}`);
  });

  runWorkdaySync().catch((error) => {
    console.error("[OpenPostings API] initial sync failed:", error);
  });

  setInterval(() => {
    runWorkdaySync().catch((error) => {
      console.error("[OpenPostings API] scheduled sync failed:", error);
    });
  }, SYNC_INTERVAL_MS);
}

start().catch((error) => {
  console.error("[OpenPostings API] startup failed:", error);
  process.exit(1);
});
