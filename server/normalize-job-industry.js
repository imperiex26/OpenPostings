const path = require("path");
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");

const ROOT_DIR = path.resolve(__dirname, "..", "..");
const DB_PATH = process.env.DB_PATH || path.resolve(__dirname, "..", "jobs.db");

const APPLY = process.argv.includes("--apply");

const RULES = [
  {
    id: "salesforce_platform_role",
    targetKey: "information_technology_software",
    test: (title) => /\bsalesforce\b/i.test(title)
  },
  {
    id: "sales_exclusive",
    targetKey: "sales_business_development",
    // Keep Salesforce platform roles out of Sales unless a true sales phrase is present.
    test: (title, normalized) => {
      if (
        /\b(account executive|account manager|business development|brand ambassador|inside sales|outside sales|sales representative|sales manager|sales director|sales consultant|sales specialist|sales associate|sales advisor|presales?|telesales|territory manager|channel sales|partner sales|go[\s-]?to[\s-]?market|gtm|revenue operations)\b/i.test(
          title
        )
      ) {
        return true;
      }
      return (
        /\bsales(?!force\b)\b/i.test(title) ||
        /\bsalesperson\b|\bsalesman\b|\bsalesworker\b/i.test(normalized)
      );
    }
  },
  {
    id: "hr_recruiting",
    targetKey: "human_resources_recruiting",
    test: (title) =>
      /\b(recruiter|recruiting|talent acquisition|human resources|hr generalist|hr manager|people operations|people partner|staffing specialist|sourcer)\b/i.test(
        title
      )
  },
  {
    id: "legal_compliance",
    targetKey: "legal_compliance",
    test: (title) =>
      /\b(attorney|counsel|paralegal|legal assistant|litigation|compliance officer|privacy counsel|contracts counsel|general counsel)\b/i.test(
        title
      )
  },
  {
    id: "finance_accounting",
    targetKey: "finance_accounting_banking_insurance",
    test: (title) =>
      /\b(accountant|accounting|accounts payable|accounts receivable|bookkeeper|controller|cpa\b|tax specialist|tax analyst|tax manager|payroll specialist|payroll analyst|fp&a|financial analyst|financial controller|treasury analyst|underwriter)\b/i.test(
        title
      )
  },
  {
    id: "marketing_media_design",
    targetKey: "marketing_advertising_media_design",
    test: (title) =>
      /\b(marketing manager|marketing specialist|digital marketing|content strategist|content marketing|social media|seo specialist|sem specialist|brand marketing|demand generation|public relations|copywriter)\b/i.test(
        title
      )
  },
  {
    id: "customer_service_call_center",
    targetKey: "customer_service_call_center",
    test: (title) =>
      /\b(call center|customer service representative|customer support representative|contact center|client service representative)\b/i.test(
        title
      )
  },
  {
    id: "behavioral_health_social_care",
    targetKey: "behavioral_health_social_care",
    test: (title) =>
      /\b(mental health|behavioral health|social worker|substance abuse counselor|counselor|case worker)\b/i.test(
        title
      )
  },
  {
    id: "healthcare_medical",
    targetKey: "healthcare_medical",
    test: (title) =>
      /\b(registered nurse|nurse practitioner|licensed practical nurse|lpn\b|rn\b|cna\b|medical assistant|physician|doctor|pharmacist|radiologic technologist|medical technologist|sonographer)\b/i.test(
        title
      )
  },
  {
    id: "education_training_library",
    targetKey: "education_training_library",
    test: (title) =>
      /\b(teacher|instructor|professor|tutor|librarian|adjunct faculty|teaching assistant)\b/i.test(title)
  },
  {
    id: "transportation_logistics",
    targetKey: "transportation_logistics_warehouse",
    test: (title) =>
      /\b(cdl\b|truck driver|delivery driver|forklift operator|warehouse associate|warehouse worker|logistics coordinator|supply chain analyst|dispatcher|route driver)\b/i.test(
        title
      )
  },
  {
    id: "cybersecurity_network_telecom",
    targetKey: "cybersecurity_network_telecom",
    test: (title) =>
      /\b(cybersecurity|information security|security analyst|soc analyst|network engineer|telecom engineer|network administrator)\b/i.test(
        title
      )
  },
  {
    id: "data_ai_analytics",
    targetKey: "data_ai_analytics",
    test: (title) =>
      /\b(data scientist|machine learning engineer|ml engineer|ai engineer|ai scientist|business intelligence|bi analyst|data engineer|data analyst|analytics engineer)\b/i.test(
        title
      )
  },
  {
    id: "information_technology_software",
    targetKey: "information_technology_software",
    test: (title) =>
      /\b(software engineer|software developer|full stack developer|frontend developer|backend developer|devops engineer|site reliability engineer|systems administrator|software architect|qa engineer|test automation engineer)\b/i.test(
        title
      )
  }
];

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function pickRule(jobTitle) {
  const title = String(jobTitle || "");
  const normalized = normalizeText(title);
  for (const rule of RULES) {
    if (rule.test(title, normalized)) {
      return rule;
    }
  }
  return null;
}

async function main() {
  const db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database
  });

  const categories = await db.all(
    `
      SELECT industry_key, industry_label
      FROM job_industry_categories;
    `
  );
  const categoryLabelByKey = new Map(categories.map((row) => [row.industry_key, row.industry_label]));

  const rows = await db.all(
    `
      SELECT id, job_title, industry_key, industry_label
      FROM job_position_industry
      ORDER BY id ASC;
    `
  );

  const updates = [];
  const ruleCounts = new Map();
  for (const row of rows) {
    const rule = pickRule(row.job_title);
    if (!rule) continue;
    if (row.industry_key === rule.targetKey) continue;

    const nextLabel = categoryLabelByKey.get(rule.targetKey);
    if (!nextLabel) continue;

    updates.push({
      id: row.id,
      previousKey: row.industry_key,
      previousLabel: row.industry_label,
      nextKey: rule.targetKey,
      nextLabel,
      ruleId: rule.id
    });
    ruleCounts.set(rule.id, (ruleCounts.get(rule.id) || 0) + 1);
  }

  const byFromTo = new Map();
  for (const item of updates) {
    const key = `${item.previousKey} -> ${item.nextKey}`;
    byFromTo.set(key, (byFromTo.get(key) || 0) + 1);
  }

  console.log(`DB: ${DB_PATH}`);
  console.log(`Total titles scanned: ${rows.length}`);
  console.log(`Planned updates: ${updates.length}`);
  console.log("Rule hit counts:");
  for (const [ruleId, count] of Array.from(ruleCounts.entries()).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${ruleId}: ${count}`);
  }
  console.log("Top source->target movements:");
  for (const [movement, count] of Array.from(byFromTo.entries()).sort((a, b) => b[1] - a[1]).slice(0, 25)) {
    console.log(`  ${movement}: ${count}`);
  }

  if (!APPLY) {
    console.log("Dry run only. Re-run with --apply to execute updates.");
    await db.close();
    return;
  }

  if (updates.length === 0) {
    console.log("No updates required.");
    await db.close();
    return;
  }

  await db.exec("BEGIN TRANSACTION;");
  try {
    for (const item of updates) {
      await db.run(
        `
          UPDATE job_position_industry
          SET
            industry_key = ?,
            industry_label = ?,
            matched_rules = ?,
            confidence_score = 10.0,
            rule_version = 'manual_override_v3',
            updated_at = datetime('now')
          WHERE id = ?;
        `,
        [
          item.nextKey,
          item.nextLabel,
          `manual_override:${item.ruleId}`,
          item.id
        ]
      );
    }
    await db.exec("COMMIT;");
    console.log(`Applied updates: ${updates.length}`);
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  } finally {
    await db.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
