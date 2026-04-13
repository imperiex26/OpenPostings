const path = require("path");
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");

const DB_PATH = process.env.DB_PATH || path.resolve(__dirname, "..", "jobs.db");

const INDIAN_STATES_AND_UTS = [
  { code: "AP", name: "Andhra Pradesh" },
  { code: "AR", name: "Arunachal Pradesh" },
  { code: "AS", name: "Assam" },
  { code: "BR", name: "Bihar" },
  { code: "CG", name: "Chhattisgarh" },
  { code: "GA", name: "Goa" },
  { code: "GJ", name: "Gujarat" },
  { code: "HR", name: "Haryana" },
  { code: "HP", name: "Himachal Pradesh" },
  { code: "JH", name: "Jharkhand" },
  { code: "KA", name: "Karnataka" },
  { code: "KL", name: "Kerala" },
  { code: "MP", name: "Madhya Pradesh" },
  { code: "MH", name: "Maharashtra" },
  { code: "MN", name: "Manipur" },
  { code: "ML", name: "Meghalaya" },
  { code: "MZ", name: "Mizoram" },
  { code: "NL", name: "Nagaland" },
  { code: "OD", name: "Odisha" },
  { code: "PB", name: "Punjab" },
  { code: "RJ", name: "Rajasthan" },
  { code: "SK", name: "Sikkim" },
  { code: "TN", name: "Tamil Nadu" },
  { code: "TS", name: "Telangana" },
  { code: "TR", name: "Tripura" },
  { code: "UP", name: "Uttar Pradesh" },
  { code: "UK", name: "Uttarakhand" },
  { code: "WB", name: "West Bengal" },
  { code: "AN", name: "Andaman and Nicobar Islands" },
  { code: "CH", name: "Chandigarh" },
  { code: "DN", name: "Dadra and Nagar Haveli and Daman and Diu" },
  { code: "DL", name: "Delhi" },
  { code: "JK", name: "Jammu and Kashmir" },
  { code: "LA", name: "Ladakh" },
  { code: "LD", name: "Lakshadweep" },
  { code: "PY", name: "Puducherry" }
];

const INDIAN_CITIES = [
  { state: "AP", cities: ["Visakhapatnam", "Vijayawada", "Guntur", "Nellore", "Kurnool", "Rajahmundry", "Tirupati", "Kakinada", "Anantapur", "Kadapa"] },
  { state: "AR", cities: ["Itanagar", "Naharlagun", "Tawang"] },
  { state: "AS", cities: ["Guwahati", "Silchar", "Dibrugarh", "Jorhat", "Nagaon", "Tinsukia", "Tezpur"] },
  { state: "BR", cities: ["Patna", "Gaya", "Bhagalpur", "Muzaffarpur", "Darbhanga", "Purnia", "Bihar Sharif"] },
  { state: "CG", cities: ["Raipur", "Bhilai", "Bilaspur", "Korba", "Durg", "Rajnandgaon"] },
  { state: "GA", cities: ["Panaji", "Margao", "Vasco da Gama", "Mapusa", "Ponda"] },
  { state: "GJ", cities: ["Ahmedabad", "Surat", "Vadodara", "Rajkot", "Bhavnagar", "Jamnagar", "Gandhinagar", "Junagadh", "Anand", "Navsari", "Morbi", "Mehsana"] },
  { state: "HR", cities: ["Gurugram", "Faridabad", "Panipat", "Ambala", "Karnal", "Hisar", "Rohtak", "Sonipat", "Panchkula"] },
  { state: "HP", cities: ["Shimla", "Dharamshala", "Manali", "Mandi", "Solan", "Kullu"] },
  { state: "JH", cities: ["Ranchi", "Jamshedpur", "Dhanbad", "Bokaro", "Hazaribagh", "Deoghar"] },
  { state: "KA", cities: ["Bengaluru", "Mysuru", "Mangaluru", "Hubli", "Dharwad", "Belgaum", "Shimoga", "Tumkur", "Udupi", "Davangere", "Gulbarga"] },
  { state: "KL", cities: ["Thiruvananthapuram", "Kochi", "Kozhikode", "Thrissur", "Kollam", "Kannur", "Alappuzha", "Palakkad", "Malappuram"] },
  { state: "MP", cities: ["Bhopal", "Indore", "Jabalpur", "Gwalior", "Ujjain", "Sagar", "Dewas", "Satna", "Ratlam", "Rewa"] },
  { state: "MH", cities: ["Mumbai", "Pune", "Nagpur", "Thane", "Nashik", "Aurangabad", "Solapur", "Kolhapur", "Navi Mumbai", "Vasai-Virar", "Amravati", "Sangli", "Kalyan-Dombivli"] },
  { state: "MN", cities: ["Imphal", "Thoubal", "Bishnupur"] },
  { state: "ML", cities: ["Shillong", "Tura", "Jowai"] },
  { state: "MZ", cities: ["Aizawl", "Lunglei", "Champhai"] },
  { state: "NL", cities: ["Kohima", "Dimapur", "Mokokchung"] },
  { state: "OD", cities: ["Bhubaneswar", "Cuttack", "Rourkela", "Berhampur", "Sambalpur", "Puri", "Balasore"] },
  { state: "PB", cities: ["Ludhiana", "Amritsar", "Jalandhar", "Patiala", "Bathinda", "Mohali", "Pathankot", "Hoshiarpur"] },
  { state: "RJ", cities: ["Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer", "Bikaner", "Alwar", "Bharatpur", "Sikar", "Bhilwara"] },
  { state: "SK", cities: ["Gangtok", "Namchi", "Gyalshing"] },
  { state: "TN", cities: ["Chennai", "Coimbatore", "Madurai", "Tiruchirappalli", "Salem", "Tirunelveli", "Erode", "Vellore", "Thoothukudi", "Tiruppur", "Dindigul"] },
  { state: "TS", cities: ["Hyderabad", "Warangal", "Nizamabad", "Khammam", "Karimnagar", "Ramagundam", "Mahbubnagar", "Secunderabad"] },
  { state: "TR", cities: ["Agartala", "Dharmanagar", "Udaipur"] },
  { state: "UP", cities: ["Lucknow", "Kanpur", "Agra", "Varanasi", "Prayagraj", "Meerut", "Ghaziabad", "Noida", "Greater Noida", "Aligarh", "Bareilly", "Moradabad", "Gorakhpur", "Saharanpur", "Jhansi", "Mathura", "Firozabad"] },
  { state: "UK", cities: ["Dehradun", "Haridwar", "Haldwani", "Rishikesh", "Roorkee", "Nainital", "Kashipur", "Rudrapur"] },
  { state: "WB", cities: ["Kolkata", "Howrah", "Durgapur", "Asansol", "Siliguri", "Bardhaman", "Malda", "Baharampur", "Kharagpur"] },
  { state: "AN", cities: ["Port Blair"] },
  { state: "CH", cities: ["Chandigarh"] },
  { state: "DN", cities: ["Silvassa", "Daman", "Diu"] },
  { state: "DL", cities: ["New Delhi", "Delhi"] },
  { state: "JK", cities: ["Srinagar", "Jammu", "Anantnag", "Baramulla", "Sopore"] },
  { state: "LA", cities: ["Leh", "Kargil"] },
  { state: "LD", cities: ["Kavaratti"] },
  { state: "PY", cities: ["Puducherry", "Karaikal", "Mahe", "Yanam"] }
];

async function main() {
  const db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database
  });

  console.log(`DB: ${DB_PATH}`);

  // Drop and recreate the table with updated CHECK constraint (city/state instead of city/county)
  const beforeCount = await db.get("SELECT COUNT(*) as c FROM state_location_index");
  console.log(`Existing rows: ${beforeCount.c}`);

  await db.exec("DROP TABLE IF EXISTS state_location_index;");
  await db.exec(`
    CREATE TABLE state_location_index (
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
    );
  `);
  console.log("Recreated state_location_index table with India schema.");

  // Insert Indian states as 'state' type entries
  let insertedStates = 0;
  let insertedCities = 0;

  const INSERT_SQL = `
    INSERT INTO state_location_index (
      location_type, state_usps, state_geoid, location_geoid,
      ansicode, location_name, search_location_name,
      normalized_location_name, normalized_search_location_name,
      lsad_code, funcstat, aland, awater, aland_sqmi, awater_sqmi,
      intptlat, intptlong, source_file, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
  `;

  await db.exec("BEGIN TRANSACTION;");
  try {
    for (const state of INDIAN_STATES_AND_UTS) {
      await db.run(INSERT_SQL, [
        "state", state.code, state.code, state.code,
        null, state.name, state.name,
        state.name.toLowerCase(), state.name.toLowerCase(),
        null, "A", null, null, null, null, null, null,
        "seed-india-locations.js"
      ]);
      insertedStates++;
    }

    for (const stateEntry of INDIAN_CITIES) {
      for (const city of stateEntry.cities) {
        await db.run(INSERT_SQL, [
          "city", stateEntry.state, stateEntry.state,
          `${stateEntry.state}_${city.toLowerCase().replace(/\s+/g, "_")}`,
          null, `${city} city`, city,
          `${city.toLowerCase()} city`, city.toLowerCase(),
          null, "A", null, null, null, null, null, null,
          "seed-india-locations.js"
        ]);
        insertedCities++;
      }
    }

    await db.exec("COMMIT;");

    console.log(`Inserted ${insertedStates} states/UTs`);
    console.log(`Inserted ${insertedCities} cities`);

    const afterCount = await db.get("SELECT COUNT(*) as c FROM state_location_index");
    console.log(`Total rows: ${afterCount.c}`);

    const stateCheck = await db.all("SELECT DISTINCT state_usps FROM state_location_index ORDER BY state_usps");
    console.log(`Distinct states: ${stateCheck.length} - ${stateCheck.map(s => s.state_usps).join(", ")}`);
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
