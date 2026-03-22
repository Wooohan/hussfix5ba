# FreightIntel  -- Developer Technical Guide

## Table of Contents

1. [Repositories](#repositories)
2. [Tech Stack](#tech-stack)
3. [Project Structure](#project-structure)
4. [Local Development Setup](#local-development-setup)
5. [Environment Variables](#environment-variables)
6. [Database Schema](#database-schema)
7. [Backend API Reference](#backend-api-reference)
8. [Frontend Architecture](#frontend-architecture)
9. [Service Layer & Data Flow](#service-layer--data-flow)
10. [Authentication](#authentication)
11. [Scraping Engine](#scraping-engine)
12. [Background Task System](#background-task-system)
13. [Deployment](#deployment)
14. [Key Design Decisions](#key-design-decisions)

---

## Repositories

| Repo | Purpose | URL |
|------|---------|-----|
| `hussfix591` | Frontend (React SPA) | https://github.com/Wooohan/hussfix591 |
| `hussfix5ba` | Backend (FastAPI API + scraper) | https://github.com/Wooohan/hussfix5ba |

---

## Tech Stack

### Frontend
| Concern | Library | Version |
|---------|---------|---------|
| Framework | React | 19.2 |
| Language | TypeScript | 5.8 |
| Build tool | Vite | 6.2 |
| Styling | Tailwind CSS | 4.2 |
| Charts | Recharts | 3.5 |
| Icons | lucide-react | 0.554 |
| HTTP | Native `fetch` + axios (dep but mostly fetch) | -- |
| Package manager | pnpm | -- |

### Backend
| Concern | Library | Version |
|---------|---------|---------|
| Framework | FastAPI (with Starlette) | 0.135+ |
| Language | Python | 3.12+ |
| HTTP client | httpx (async) | 0.28 |
| HTML parsing | BeautifulSoup4 + lxml | 4.14 / 6.0 |
| Database driver | asyncpg | 0.30 |
| Auth | PyJWT + bcrypt | 2.9 / 5.0 |
| Env config | python-dotenv | 1.2 |
| Package manager | Poetry | -- |
| Runtime | Uvicorn (ASGI) | -- |

### Infrastructure
| Concern | Technology |
|---------|-----------|
| Database | PostgreSQL (any version with `gen_random_uuid()`) |
| Backend hosting | Railway (Docker) |
| Frontend hosting | Vercel / Netlify (static build) |
| Containerization | Dockerfile (python:3.12-slim) |

---

## Project Structure

### Frontend (`hussfix591`)
```
.
├── App.tsx                  # Root component, routing, state management
├── index.tsx                # React DOM entry point
├── index.html               # HTML shell
├── index.css                # Global styles (Tailwind base)
├── types.ts                 # All TypeScript interfaces
├── metadata.json            # App metadata
├── vite.config.ts           # Vite config (port 3000, path alias @)
├── package.json             # Dependencies (pnpm)
├── tsconfig.json            # TypeScript config
├── postcss.config.js        # PostCSS + Tailwind
├── supabase_schema.sql      # Reference DB schema (not used at runtime)
├── env.example              # Example env vars
├── components/
│   ├── Sidebar.tsx           # Navigation sidebar (role-based filtering)
│   └── ErrorBoundary.tsx     # React error boundary wrapper
├── pages/
│   ├── Landing.tsx           # Marketing landing + login/register modals
│   ├── Dashboard.tsx         # KPI cards + authority status chart
│   ├── Scraper.tsx           # Live scraper UI (config, logs, results table)
│   ├── CarrierSearch.tsx     # Carrier database with 20+ filters + detail modal
│   ├── InsuranceScraper.tsx  # Insurance batch enrichment + quick lookup
│   ├── FMCSARegister.tsx     # FMCSA daily register viewer
│   ├── AdminPanel.tsx        # User CRUD, IP blocking, analytics
│   └── Subscription.tsx      # Plan comparison page
└── services/
    ├── backendApiService.ts  # Primary API client (JWT, all CRUD ops)
    ├── backendService.ts     # Scraper/task API client (start/stop/poll)
    ├── supabaseClient.ts     # Compatibility layer (routes to backendApiService)
    ├── userService.ts        # User/IP facade (routes to backendApiService)
    ├── fmcsaRegisterService.ts # FMCSA register API calls
    ├── mockService.ts        # Insurance proxy fetch + CSV download utility
    └── hashUtils.ts          # Client-side hashing utilities
```

### Backend (`hussfix5ba`)
```
.
├── README.md                # Setup & deployment guide
└── backend/
    ├── Dockerfile            # python:3.12-slim, Poetry install, Uvicorn CMD
    ├── Procfile              # Railway process definition
    ├── railway.json          # Railway deploy config (Dockerfile builder)
    ├── pyproject.toml        # Python dependencies (Poetry)
    ├── poetry.lock           # Locked dependency versions
    ├── schema.sql            # Standalone DB schema for manual setup
    ├── tests/                # Test directory
    └── app/
        ├── __init__.py
        ├── main.py           # FastAPI app, all route definitions, middleware
        ├── scraper.py        # FMCSA scraping functions (carrier, safety, insurance)
        ├── task_manager.py   # Background task orchestrator (singleton)
        ├── database.py       # asyncpg connection pool, all SQL queries
        ├── auth.py           # JWT creation/verification, auth middleware helper
        └── fmcsa_register.py # FMCSA Daily Register scraper
```

---

## Local Development Setup

### Prerequisites
- Node.js 18+ and pnpm
- Python 3.12+, Poetry
- PostgreSQL (local or remote)

### 1. Database
```bash
# Create database
createdb freightintel

# Run schema (from backend repo)
psql -U postgres -d freightintel -f backend/schema.sql
```
The backend also auto-creates all tables, indexes, and triggers on startup via the embedded `_SCHEMA_SQL` in `database.py`, so manual schema setup is optional.

### 2. Backend
```bash
cd hussfix5ba/backend
pip install poetry
poetry install

export DATABASE_URL="postgresql://postgres:password@localhost:5432/freightintel"
export JWT_SECRET="your-secret-key-here"   # optional; random generated if unset

poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Backend runs at `http://localhost:8000`. Swagger docs at `/docs`.

### 3. Frontend
```bash
cd hussfix591
pnpm install

# Create .env file
echo "VITE_BACKEND_URL=http://localhost:8000" > .env

pnpm dev
```
Frontend runs at `http://localhost:3000`.

### Default Admin Account
The schema seeds a default admin:
- **Email**: `wooohan3@gmail.com`
- **Role**: admin
- **Plan**: Enterprise (100k daily limit)
- **Note**: No password hash is set in the seed; you need to register via the UI or manually insert a bcrypt hash.

---

## Environment Variables

### Frontend (`.env`)
| Variable | Description | Example |
|----------|-------------|---------|
| `VITE_BACKEND_URL` | Backend API base URL | `http://localhost:8000` or `https://your-app.up.railway.app` |

### Backend
| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `DATABASE_URL` | Yes | PostgreSQL connection string | _(none -- warns if missing)_ |
| `JWT_SECRET` | Recommended | Secret key for signing JWT tokens | Random on startup (tokens don't survive restarts) |
| `JWT_EXPIRY_SECONDS` | No | Token lifetime in seconds | `86400` (24 hours) |
| `CORS_ORIGINS` | No | Comma-separated allowed origins | `*` (all origins) |
| `PORT` | No | Server port (Railway sets this) | `8000` |

---

## Database Schema

Four tables, all with Row Level Security (RLS) enabled in Supabase mode:

### `carriers`
Primary data table. ~30 columns.

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID (PK) | Auto-generated |
| `mc_number` | TEXT (UNIQUE) | MC/MX number -- upsert key |
| `dot_number` | TEXT | USDOT number |
| `legal_name` | TEXT | Required |
| `dba_name` | TEXT | Nullable |
| `entity_type` | TEXT | CARRIER, BROKER, etc. |
| `status` | TEXT | Authority status string |
| `email` | TEXT | Decoded from FMCSA |
| `phone` | TEXT | |
| `power_units` | TEXT | Fleet size |
| `drivers` | TEXT | Driver count |
| `operation_classification` | TEXT[] | Array: Auth. For Hire, Exempt, etc. |
| `carrier_operation` | TEXT[] | Array: Interstate, Intrastate, etc. |
| `cargo_carried` | TEXT[] | Array: General Freight, Household Goods, etc. |
| `safety_rating` | TEXT | SATISFACTORY, UNSATISFACTORY, etc. |
| `basic_scores` | JSONB | Array of `{category, measure}` |
| `oos_rates` | JSONB | Array of `{type, rate, nationalAvg}` |
| `insurance_policies` | JSONB | Array of policy objects |
| `inspections` | JSONB | Array of inspection records |
| `crashes` | JSONB | Array of crash records |
| `created_at` / `updated_at` | TIMESTAMPTZ | Auto-managed by triggers |

**Indexes**: `mc_number`, `dot_number`, `created_at DESC`, `status`

### `users`
| Column | Type | Notes |
|--------|------|-------|
| `user_id` | TEXT (UNIQUE) | App-level unique ID |
| `email` | TEXT (UNIQUE) | |
| `password_hash` | TEXT | bcrypt hash |
| `role` | TEXT | `'user'` or `'admin'` (CHECK constraint) |
| `plan` | TEXT | `'Free'`, `'Starter'`, `'Pro'`, `'Enterprise'` |
| `daily_limit` | INTEGER | Max records/day |
| `records_extracted_today` | INTEGER | Current day usage |
| `is_blocked` | BOOLEAN | Account block flag |
| `is_online` | BOOLEAN | Online status |

### `fmcsa_register`
| Column | Type | Notes |
|--------|------|-------|
| `number` | TEXT | Docket number (e.g., MC-123456) |
| `title` | TEXT | Entry description |
| `decided` | TEXT | Date decided |
| `category` | TEXT | NAME CHANGE, REVOCATION, etc. |
| `date_fetched` | TEXT | Scrape date |
| **Unique constraint**: `(number, date_fetched)` |

### `blocked_ips`
| Column | Type | Notes |
|--------|------|-------|
| `ip_address` | TEXT (UNIQUE) | Blocked IP |
| `reason` | TEXT | |
| `blocked_at` | TIMESTAMPTZ | |

All tables have `updated_at` triggers that auto-set `NOW()` on update.

---

## Backend API Reference

### Public Endpoints (no JWT required)
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/healthz` | Health check with DB ping |
| `GET` | `/api/get-ip` | Returns client IP |
| `POST` | `/api/auth/login` | Login (email + password) -> JWT token |
| `POST` | `/api/auth/register` | Register new user -> JWT token |
| `POST` | `/api/users/verify-password` | Verify password (public) |
| `GET` | `/api/blocked-ips/check/{ip}` | Check if IP is blocked |

### Protected Endpoints (JWT Bearer token required)

#### Scraping
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/proxy?url=...` | CORS proxy for FMCSA/insurance sites |
| `GET` | `/api/scrape/carrier/{mc}` | Scrape single carrier by MC# |
| `GET` | `/api/scrape/safety/{dot}` | Fetch safety data by DOT# |
| `GET` | `/api/scrape/insurance/{dot}` | Fetch insurance data by DOT# |

#### Background Tasks
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/tasks/scraper/start` | Start carrier scraping task |
| `POST` | `/api/tasks/scraper/stop` | Stop a running scraper task |
| `GET` | `/api/tasks/scraper/status?task_id=X` | Poll task status + recent data |
| `GET` | `/api/tasks/scraper/data?task_id=X` | Get full scraped dataset |
| `POST` | `/api/tasks/insurance/start` | Start insurance enrichment task |
| `POST` | `/api/tasks/insurance/stop` | Stop insurance task |
| `GET` | `/api/tasks/insurance/status?task_id=X` | Insurance task status |
| `GET` | `/api/tasks/active?task_type=scraper` | Get currently running task |
| `GET` | `/api/tasks` | List all tasks |

#### Carriers CRUD
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/carriers?...filters` | Query carriers (20+ filter params) |
| `POST` | `/api/carriers` | Upsert single carrier |
| `POST` | `/api/carriers/batch` | Upsert batch of carriers |
| `DELETE` | `/api/carriers/{mc}` | Delete carrier by MC# |
| `GET` | `/api/carriers/count` | Total carrier count |
| `PUT` | `/api/carriers/{dot}/insurance` | Update insurance for a carrier |
| `PUT` | `/api/carriers/{dot}/safety` | Update safety data for a carrier |
| `GET` | `/api/carriers/range?start=X&end=Y` | Get carriers in MC range |

#### Users CRUD
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/users` | List all users |
| `GET` | `/api/users/by-email/{email}` | Fetch user by email |
| `POST` | `/api/users` | Create user |
| `PUT` | `/api/users/{user_id}` | Update user |
| `DELETE` | `/api/users/{user_id}` | Delete user |

#### Blocked IPs
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/blocked-ips` | List blocked IPs |
| `POST` | `/api/blocked-ips` | Block an IP |
| `DELETE` | `/api/blocked-ips/{ip}` | Unblock an IP |

#### FMCSA Register
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/fmcsa-register` | Scrape FMCSA register for a date |
| `GET` | `/api/fmcsa-register/entries?extracted_date=X` | Fetch stored entries |
| `GET` | `/api/fmcsa-register/dates` | List all scraped dates |
| `GET` | `/api/fmcsa-register/categories` | List distinct categories |
| `DELETE` | `/api/fmcsa-register/before/{date}` | Delete old entries |

---

## Frontend Architecture

### Routing
The app uses **manual view state** (`useState<ViewState>`) rather than a client-side router. The `ViewState` type determines which page component renders:

```typescript
type ViewState = 'dashboard' | 'scraper' | 'carrier-search' | 'insurance-scraper'
              | 'subscription' | 'settings' | 'admin' | 'fmcsa-register';
```

View state is persisted to `localStorage` (`hussfix_view`) and restored on reload.

### State Management
- **No external state library** -- all state lives in `App.tsx` via `useState` hooks.
- User object stored in `localStorage` (`hussfix_user`) for session persistence.
- JWT token stored in `localStorage` (`hussfix_token`) via `backendApiService.ts`.
- Carrier data flows down from `App.tsx` -> page components as props.

### Role-Based Access
- The `Sidebar` component filters navigation items by `user.role`.
- `App.tsx` `handleViewChange` blocks non-admin users from admin-only views: `scraper`, `insurance-scraper`, `settings`, `admin`.
- Regular users see: Dashboard, Carrier Database, FMCSA Register, Subscription.

### Component Hierarchy
```
App.tsx
├── Landing.tsx (if !user)
├── Sidebar.tsx
└── <current page>
    ├── Dashboard.tsx
    ├── Scraper.tsx
    ├── CarrierSearch.tsx
    ├── InsuranceScraper.tsx
    ├── FMCSARegister.tsx
    ├── Subscription.tsx
    ├── SettingsPage (inline in App.tsx)
    └── AdminPanel.tsx
```

---

## Service Layer & Data Flow

The frontend has a **3-tier service architecture** for backward compatibility:

```
Page Components
      │
      ▼
supabaseClient.ts / userService.ts   ← "Supabase" facade (legacy names)
      │
      ▼
backendApiService.ts                  ← Actual HTTP calls to FastAPI backend
      │
      ▼
FastAPI Backend (main.py)
      │
      ▼
PostgreSQL (database.py via asyncpg)
```

- `supabaseClient.ts` and `userService.ts` maintain the old Supabase function names (`saveCarrierToSupabase`, `fetchUsersFromSupabase`, etc.) but internally delegate to `backendApiService.ts`.
- `backendService.ts` is a separate client specifically for scraper/task endpoints.
- `mockService.ts` handles insurance proxy fetching and CSV download generation.

### Data Format Conversion
The backend uses `snake_case` (Python convention). The frontend uses `camelCase` (JS convention). The `supabaseClient.ts` layer handles the mapping in both directions:
- Backend `mc_number` <-> Frontend `mcNumber`
- Backend `legal_name` <-> Frontend `legalName`
- etc.

---

## Authentication

### Flow
1. User submits email/password on `Landing.tsx`.
2. Frontend calls `POST /api/auth/login` (or `/register`).
3. Backend verifies credentials (bcrypt), returns a JWT token + user object.
4. Frontend stores token in `localStorage` under `hussfix_token`.
5. All subsequent API calls include `Authorization: Bearer <token>` header.
6. `AuthMiddleware` in `main.py` intercepts all requests, verifies JWT, and attaches decoded payload to `request.state.user`.
7. Public endpoints (login, register, health, IP check) bypass auth.

### JWT Structure
```json
{
  "sub": "user-id",
  "email": "user@example.com",
  "role": "admin",
  "iat": 1234567890,
  "exp": 1234654290
}
```

### Password Hashing
- Backend uses `bcrypt` (cost factor default).
- Hashing happens server-side in `POST /api/auth/register` and `POST /api/users`.
- Verification via `bcrypt.checkpw()` in `POST /api/auth/login`.

---

## Scraping Engine

Located in `backend/app/scraper.py`. All scraping is **async** using `httpx.AsyncClient`.

### Shared HTTP Clients
Two singleton `httpx.AsyncClient` instances are maintained for connection pooling:
- `_fmcsa_client`: For FMCSA SAFER and SMS pages (max 20 connections)
- `_insurance_client`: For searchcarriers.com API (max 10 connections)

Both are closed on app shutdown via `close_clients()`.

### Scraping Functions

| Function | Source URL | Data Extracted |
|----------|-----------|----------------|
| `scrape_carrier(mc)` | `safer.fmcsa.dot.gov/query.asp` | All carrier fields from SAFER snapshot |
| `find_dot_email(dot)` | `ai.fmcsa.dot.gov/SMS/Carrier/{dot}/CarrierRegistration.aspx` | Carrier email (Cloudflare-decoded) |
| `fetch_safety_data(dot)` | `ai.fmcsa.dot.gov/SMS/Carrier/{dot}/CompleteProfile.aspx` | Safety rating, BASIC scores, OOS rates |
| `fetch_inspection_and_crash_data(dot)` | Same as safety | Inspection history, crash records |
| `fetch_insurance_data(dot)` | `searchcarriers.com/company/{dot}/insurances` | Insurance policies (JSON API) |

### Per-Carrier Scrape Flow
When `scrape_carrier(mc)` is called:
1. Fetch SAFER snapshot HTML for the MC number.
2. Parse carrier fields from HTML table.
3. **In parallel** (`asyncio.gather`): fetch email, safety data, and inspection/crash data using the DOT number.
4. Return assembled carrier dict.

### Retry Logic
`fetch_fmcsa()` retries up to 2 times with incremental backoff (300ms * attempt). 4xx responses abort immediately.

### Cloudflare Email Decoding
FMCSA pages obfuscate emails with Cloudflare's `data-cfemail` attribute. The `cf_decode_email()` function XOR-decodes the hex string to recover the original email.

---

## Background Task System

Located in `backend/app/task_manager.py`. Uses a singleton `TaskManager` class.

### How It Works
1. Frontend calls `POST /api/tasks/scraper/start` with config.
2. `TaskManager.start_scraper_task()` creates an `asyncio.Task` that runs `_run_scraper()`.
3. The task iterates through MC numbers, scraping each one and updating its internal state dict.
4. Frontend polls `GET /api/tasks/scraper/status?task_id=X` every 1.5s for progress, logs, and recent data.
5. On stop/completion, final batch is saved to DB and task status is updated.

### Task Lifecycle States
```
running -> stopping -> stopped
running -> completed
```

### Memory Management
- Scraped data is buffered and batch-saved to DB every 50 records.
- Completed task data (`scrapedData`) is cleared from memory after the task finishes.
- Only the last 20 completed tasks are kept in memory (`_MAX_COMPLETED_TASKS`).
- Logs are capped at 500 entries per task.
- Status endpoint returns only the last 100 log lines and last 20 recent records.

### Task Reconnection
If a user refreshes the page, the frontend calls `GET /api/tasks/active?task_type=scraper` on mount. If a running task exists, it reconnects to it and resumes polling.

---

## Deployment

### Backend (Railway)
1. Push `hussfix5ba` to GitHub.
2. Create a Railway project, connect the repo, point to the `backend/` directory.
3. Add a PostgreSQL addon.
4. Set env vars: `DATABASE_URL` (from Railway Postgres), `JWT_SECRET`, `PORT=8000`.
5. Railway auto-detects `Dockerfile` via `railway.json` and deploys.
6. The `Procfile` defines: `web: .venv/bin/uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}`

### Frontend (Vercel/Netlify)
1. Push `hussfix591` to GitHub.
2. Connect to Vercel/Netlify.
3. Set env var: `VITE_BACKEND_URL=https://your-backend.up.railway.app`
4. Build command: `pnpm build` (or `vite build`)
5. Output directory: `dist/`

### Docker (Self-Hosted)
```bash
cd hussfix5ba/backend
docker build -t freightintel-backend .
docker run -p 8000:8000 \
  -e DATABASE_URL="postgresql://user:pass@host:5432/dbname" \
  -e JWT_SECRET="your-secret" \
  freightintel-backend
```

---

## Key Design Decisions

1. **No client-side router**: View state is managed with `useState` + `localStorage`. Simple and works, but doesn't support deep linking or browser back/forward.

2. **Supabase compatibility layer**: The codebase was migrated from Supabase to a custom backend. The `supabaseClient.ts` and `userService.ts` files preserve the old function names to minimize refactoring in page components.

3. **Server-side scraping**: All scraping runs on the backend to avoid CORS issues, maintain persistent connections, and allow tasks to survive browser closures.

4. **In-memory task state**: Task progress and scraped data live in the `TaskManager` singleton's memory. This means task state is lost on server restart. Scraped data is periodically flushed to PostgreSQL to prevent data loss.

5. **Async-first backend**: Every I/O operation (DB queries, HTTP requests) is async. The scraper uses `asyncio.gather` to parallelize email + safety + inspection fetches per carrier.

6. **JWT auth middleware**: A Starlette `BaseHTTPMiddleware` intercepts all requests. Public paths are whitelisted. The middleware is added before CORS so that CORS headers are always present (even on 401 responses).

7. **CORS proxy**: The backend includes a `/api/proxy` endpoint that proxies requests to FMCSA and searchcarriers.com domains. Domain whitelist: `safer.fmcsa.dot.gov`, `ai.fmcsa.dot.gov`, `searchcarriers.com`, `li-public.fmcsa.dot.gov`.

8. **Auto-schema migration**: `database.py` contains the full schema as an embedded SQL string. On startup, it executes the schema against the DB (all `IF NOT EXISTS` / `ON CONFLICT`), so the DB is always in the correct state without manual migration steps.
