# OpenPostings

<p align="center">
  <img src="logo.png" alt="OpenPostings" width="400" />
</p>

<p align="center">
  Open-source job aggregator that pulls fresh postings from <strong>30,000+ companies across India</strong> — all in one local app.
</p>

---

OpenPostings scrapes 10 major Applicant Tracking Systems every 30 minutes, stores jobs locally in SQLite, and lets you search, filter, track applications, and even auto-apply with an MCP-powered AI agent.

## Youtube Video
[![OpenPostings Discussion](https://img.youtube.com/vi/5sVIhhwx3Yk/0.jpg)](https://www.youtube.com/watch?v=5sVIhhwx3Yk)

## Highlights

- **30,000+ companies** across India, pre-loaded and ready on first run
- **10 ATS integrations** — Workday, Greenhouse, Lever, Ashby, iCIMS, Recruitee, UKG/UltiPro, OracleCloud, Workable, BambooHR
- **Fresh jobs only** — pulls postings from the last 24 hours, auto-prunes stale ones
- **28 industry categories** with smart job-title classification
- **36 Indian states/UTs and 200+ cities** for location-based filtering
- **Zero cloud dependency** — everything runs on your machine, your data stays local
- **MCP agent support** — let Claude, Codex, Gemini, or any LLM find and apply to jobs for you

## Architecture

```
+------------------+       +------------------+       +----------------+
|                  |       |                  |       |                |
|  React Native    | <---> |  Express API     | <---> |  SQLite        |
|  (Web/Android/   |  REST |  (localhost:8787)|       |  (jobs.db)     |
|   Windows)       |       |                  |       |                |
+------------------+       +--------+---------+       +-------+--------+
                                    |                         |
                           +--------+---------+     +---------+--------+
                           |                  |     |                  |
                           |  ATS Scrapers    |     |  CSV Seed Data   |
                           |  (10 providers)  |     |  (server/data/)  |
                           |  every 30 min    |     |  companies, etc. |
                           +------------------+     +------------------+
                                    
                           +------------------+
                           |                  |
                           |  MCP Apply Agent |
                           |  (stdio server)  |
                           +------------------+
```

**On first `npm run server`**, the app creates `jobs.db` from CSV seed files shipped in the repo (`server/data/`). No external database download needed. The sync cycle then begins pulling live postings from all 30,000+ companies.

## Screenshots

![Web UI Screenshot](README-Images/webui.png)

<br>
<img src="README-Images/apply_or_ignore.png" alt="Apply or Ignore" width="25%" />
<br>
<img src="README-Images/applications.png" alt="Applications" width="70%" />

## Supported ATS

| ATS | Companies |
|-----|-----------|
| Workday | ~12,000 |
| Greenhouse | ~8,000 |
| Lever | ~4,300 |
| Ashby | ~3,100 |
| UKG / UltiPro | ~900 |
| iCIMS | ~750 |
| Recruitee | ~600 |
| Workable | 24 |
| OracleCloud | 16 |
| BambooHR | 3 |

## Features

- Pulls jobs from **10 ATS providers** into one local database
- Filters postings by **search text, ATS, industry, state, and remote mode**
- Tracks **applied/ignored** posting state and full application lifecycle
- Stores applicant profile and MCP agent settings in SQLite
- MCP tools for **candidate selection, cover-letter drafting, and result recording**
- Runs on **Web, Android, and Windows** via React Native

## Requirements

- Node.js 18+ and npm
  - https://docs.npmjs.com/downloading-and-installing-node-js-and-npm
- For Windows target: React Native Windows prerequisites
  - https://microsoft.github.io/react-native-windows/
- For Android target: Android Studio/emulator or device
  - https://developer.android.com/studio

## Installation

```bash
cd OpenPostings
npm install
```

## Quick Start (Web)

Terminal 1 — start the API server (creates `jobs.db` on first run and begins syncing jobs):

```bash
npm run server
```

Terminal 2 — start the frontend:

```bash
npm run web
```

- Web UI: `http://localhost:8081`
- API: `http://localhost:8787`
- Android emulator API: `http://10.0.2.2:8787`

## Windows / Android

```bash
npm run windows
npm run android
```

## How the Database Works

The repo ships CSV seed files in `server/data/` containing company lists, industry categories, and location data. **No binary database file is committed to git.**

On first `npm run server`:
1. `jobs.db` is created automatically
2. Seed tables are populated from the CSV files (companies, industries, locations)
3. The sync cycle starts pulling live job postings from all companies
4. Postings older than 24 hours are automatically pruned

The database is local-only and can grow freely on your machine.

## REST API

Core:
- `GET /health`
- `GET /sync/status`
- `POST /sync/ats` (`?wait=1` optional)

Postings:
- `GET /postings`
- `GET /postings/filter-options`
- `POST /postings/ignore`

Applications:
- `GET /applications`
- `POST /applications`
- `PATCH /applications/:id`
- `DELETE /applications/:id`

Settings:
- `GET /settings/personal-information`
- `PUT /settings/personal-information`
- `GET /settings/mcp`
- `PUT /settings/mcp`

MCP helper endpoints:
- `GET /mcp/candidates`
- `POST /mcp/cover-letter-draft`
- `POST /mcp/applications/complete`

## MCP Apply Agent

Let Claude, Codex, Gemini, or any LLM with MCP support:
- Read your applicant profile — `get_applicant_context`
- Find relevant fresh jobs — `find_posting_candidates`
- Draft tailored cover letters — `draft_cover_letter`
- Apply to jobs (with browser access)
- Track application results — `record_application_result`

Start the MCP server:

```bash
npm run mcp:apply-agent
```

MCP config example (for Codex / Claude Code):
```json
[mcp_servers.openpostings-apply]
command = "node"
args = ["<path-to>/OpenPostings/server/mcp-apply-server.js"]
```

## Security Notes

This is designed for local/self-hosted usage.

- MCP credentials and settings are stored in local SQLite.
- If you need stricter controls, add OS-level secret storage, DB encryption-at-rest, and tighter filesystem permissions.
