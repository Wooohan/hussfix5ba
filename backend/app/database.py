import os
import json
import asyncpg
from typing import Optional

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    import warnings
    warnings.warn("DATABASE_URL is not set. Database connections will fail.")

_pool: Optional[asyncpg.Pool] = None


_SCHEMA_SQL = """
-- ── Tables ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS carriers (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    mc_number TEXT NOT NULL UNIQUE,
    dot_number TEXT NOT NULL,
    legal_name TEXT NOT NULL,
    dba_name TEXT,
    entity_type TEXT,
    status TEXT,
    email TEXT,
    phone TEXT,
    power_units TEXT,
    drivers TEXT,
    non_cmv_units TEXT,
    physical_address TEXT,
    mailing_address TEXT,
    date_scraped TEXT,
    mcs150_date TEXT,
    mcs150_mileage TEXT,
    operation_classification TEXT[],
    carrier_operation TEXT[],
    cargo_carried TEXT[],
    out_of_service_date TEXT,
    state_carrier_id TEXT,
    duns_number TEXT,
    safety_rating TEXT,
    safety_rating_date TEXT,
    basic_scores JSONB,
    oos_rates JSONB,
    insurance_policies JSONB,
    crashes JSONB,
    inspections JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fmcsa_register (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    number TEXT NOT NULL,
    title TEXT NOT NULL,
    decided TEXT,
    category TEXT,
    date_fetched TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(number, date_fetched)
);

CREATE TABLE IF NOT EXISTS users (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    plan TEXT NOT NULL DEFAULT 'Free' CHECK (plan IN ('Free', 'Starter', 'Pro', 'Enterprise')),
    daily_limit INTEGER NOT NULL DEFAULT 50,
    records_extracted_today INTEGER NOT NULL DEFAULT 0,
    last_active TEXT DEFAULT 'Never',
    ip_address TEXT,
    is_online BOOLEAN DEFAULT false,
    is_blocked BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS blocked_ips (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ip_address TEXT NOT NULL UNIQUE,
    reason TEXT,
    blocked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    blocked_by TEXT
);

-- ── Indexes ─────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_carriers_mc_number ON carriers(mc_number);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number ON carriers(dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_created_at ON carriers(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_carriers_status ON carriers(status);

CREATE INDEX IF NOT EXISTS idx_fmcsa_register_number ON fmcsa_register(number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_date_fetched ON fmcsa_register(date_fetched DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_category ON fmcsa_register(category);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address);

-- ── Timestamp triggers ──────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_carriers_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_carriers_updated_at ON carriers;
CREATE TRIGGER update_carriers_updated_at BEFORE UPDATE ON carriers
    FOR EACH ROW EXECUTE FUNCTION update_carriers_updated_at();

CREATE OR REPLACE FUNCTION update_fmcsa_register_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_fmcsa_register_updated_at ON fmcsa_register;
CREATE TRIGGER update_fmcsa_register_updated_at BEFORE UPDATE ON fmcsa_register
    FOR EACH ROW EXECUTE FUNCTION update_fmcsa_register_updated_at();

CREATE OR REPLACE FUNCTION update_users_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_users_updated_at();

CREATE TABLE IF NOT EXISTS new_ventures (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    dot_number TEXT NOT NULL,
    mc_number TEXT,
    legal_name TEXT,
    dba_name TEXT,
    entity_type TEXT,
    status TEXT,
    email TEXT,
    phone TEXT,
    physical_address TEXT,
    mailing_address TEXT,
    power_units TEXT,
    drivers TEXT,
    cargo_carried TEXT,
    hazmat_indicator TEXT,
    operation_classification TEXT,
    carrier_operation TEXT,
    safety_rating TEXT,
    safety_rating_type_code TEXT,
    safety_rating_effective_date TEXT,
    safety_rating_latest_review_type TEXT,
    safety_rating_latest_review_date TEXT,
    mcsip_step_number TEXT,
    interstate_within_100 TEXT,
    interstate_beyond_100 TEXT,
    interstate_total TEXT,
    intrastate_within_100 TEXT,
    intrastate_beyond_100 TEXT,
    intrastate_total TEXT,
    avg_trip_leased_drivers TEXT,
    grand_total_drivers TEXT,
    total_cdl TEXT,
    total_non_cdl TEXT,
    total_trucks TEXT,
    total_power_units TEXT,
    fleet_size_code TEXT,
    owned_trucks TEXT,
    term_leased_trucks TEXT,
    trip_leased_trucks TEXT,
    owned_tractors TEXT,
    term_leased_tractors TEXT,
    trip_leased_tractors TEXT,
    owned_trailers TEXT,
    term_leased_trailers TEXT,
    trip_leased_trailers TEXT,
    recordable_accident_rate TEXT,
    preventable_accident_rate TEXT,
    mcs150_mileage TEXT,
    mcs150_mileage_year TEXT,
    mcs150_date TEXT,
    state_carrier_id TEXT,
    duns_number TEXT,
    out_of_service_date TEXT,
    date_added TEXT NOT NULL,
    date_fetched TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(dot_number, date_added)
);

CREATE INDEX IF NOT EXISTS idx_new_ventures_dot ON new_ventures(dot_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_mc ON new_ventures(mc_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_date_added ON new_ventures(date_added DESC);
CREATE INDEX IF NOT EXISTS idx_new_ventures_legal_name ON new_ventures(legal_name);
CREATE INDEX IF NOT EXISTS idx_new_ventures_status ON new_ventures(status);
CREATE INDEX IF NOT EXISTS idx_new_ventures_created_at ON new_ventures(created_at DESC);

CREATE OR REPLACE FUNCTION update_new_ventures_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_new_ventures_updated_at ON new_ventures;
CREATE TRIGGER update_new_ventures_updated_at BEFORE UPDATE ON new_ventures
    FOR EACH ROW EXECUTE FUNCTION update_new_ventures_updated_at();

-- ── Default admin user ──────────────────────────────────────────────────────
INSERT INTO users (user_id, name, email, role, plan, daily_limit, records_extracted_today, ip_address, is_online, is_blocked)
VALUES ('1', 'Admin User', 'wooohan3@gmail.com', 'admin', 'Enterprise', 100000, 0, '192.168.1.1', false, false)
ON CONFLICT (email) DO NOTHING;
"""


async def connect_db() -> None:
    """Connect to PostgreSQL, create tables if needed, and create a connection pool."""
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    try:
        async with _pool.acquire() as conn:
            await conn.execute(_SCHEMA_SQL)
        print("[DB] Connected to PostgreSQL, schema initialized, pool created")
    except Exception as e:
        print(f"[DB] Connected to PostgreSQL, pool created (schema init skipped: {e})")


async def close_db() -> None:
    """Close the PostgreSQL connection pool."""
    global _pool
    if _pool:
        await _pool.close()
    _pool = None
    print("[DB] PostgreSQL connection pool closed")


def get_pool() -> asyncpg.Pool:
    """Return the connection pool."""
    if _pool is None:
        raise RuntimeError("Database not connected. Call connect_db() first.")
    return _pool


async def upsert_carrier(record: dict) -> bool:
    """Upsert a carrier record by mc_number."""
    pool = get_pool()
    mc = record.get("mc_number")
    if not mc:
        return False
    try:
        await pool.execute(
            """
            INSERT INTO carriers (
                mc_number, dot_number, legal_name, dba_name, entity_type,
                status, email, phone, power_units, drivers, non_cmv_units,
                physical_address, mailing_address, date_scraped,
                mcs150_date, mcs150_mileage, operation_classification,
                carrier_operation, cargo_carried, out_of_service_date,
                state_carrier_id, duns_number, safety_rating, safety_rating_date,
                basic_scores, oos_rates, insurance_policies, inspections, crashes
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10, $11,
                $12, $13, $14,
                $15, $16, $17,
                $18, $19, $20,
                $21, $22, $23, $24,
                $25, $26, $27, $28, $29
            )
            ON CONFLICT (mc_number) DO UPDATE SET
                dot_number = EXCLUDED.dot_number,
                legal_name = EXCLUDED.legal_name,
                dba_name = EXCLUDED.dba_name,
                entity_type = EXCLUDED.entity_type,
                status = EXCLUDED.status,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                power_units = EXCLUDED.power_units,
                drivers = EXCLUDED.drivers,
                non_cmv_units = EXCLUDED.non_cmv_units,
                physical_address = EXCLUDED.physical_address,
                mailing_address = EXCLUDED.mailing_address,
                date_scraped = EXCLUDED.date_scraped,
                mcs150_date = EXCLUDED.mcs150_date,
                mcs150_mileage = EXCLUDED.mcs150_mileage,
                operation_classification = EXCLUDED.operation_classification,
                carrier_operation = EXCLUDED.carrier_operation,
                cargo_carried = EXCLUDED.cargo_carried,
                out_of_service_date = EXCLUDED.out_of_service_date,
                state_carrier_id = EXCLUDED.state_carrier_id,
                duns_number = EXCLUDED.duns_number,
                safety_rating = EXCLUDED.safety_rating,
                safety_rating_date = EXCLUDED.safety_rating_date,
                basic_scores = EXCLUDED.basic_scores,
                oos_rates = EXCLUDED.oos_rates,
                insurance_policies = EXCLUDED.insurance_policies,
                inspections = EXCLUDED.inspections,
                crashes = EXCLUDED.crashes,
                updated_at = NOW()
            """,
            record.get("mc_number"),
            record.get("dot_number"),
            record.get("legal_name"),
            record.get("dba_name"),
            record.get("entity_type"),
            record.get("status"),
            record.get("email"),
            record.get("phone"),
            record.get("power_units"),
            record.get("drivers"),
            record.get("non_cmv_units"),
            record.get("physical_address"),
            record.get("mailing_address"),
            record.get("date_scraped"),
            record.get("mcs150_date"),
            record.get("mcs150_mileage"),
            record.get("operation_classification", []),
            record.get("carrier_operation", []),
            record.get("cargo_carried", []),
            record.get("out_of_service_date"),
            record.get("state_carrier_id"),
            record.get("duns_number"),
            record.get("safety_rating"),
            record.get("safety_rating_date"),
            _to_jsonb(record.get("basic_scores")),
            _to_jsonb(record.get("oos_rates")),
            _to_jsonb(record.get("insurance_policies")),
            _to_jsonb(record.get("inspections")),
            _to_jsonb(record.get("crashes")),
        )
        return True
    except Exception as e:
        print(f"[DB] Error upserting carrier {mc}: {e}")
        return False


async def update_carrier_insurance(dot_number: str, policies: list) -> bool:
    """Update insurance_policies for a carrier by dot_number."""
    pool = get_pool()
    try:
        result = await pool.execute(
            """
            UPDATE carriers
            SET insurance_policies = $1, updated_at = NOW()
            WHERE dot_number = $2
            """,
            _to_jsonb(policies),
            dot_number,
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating insurance for DOT {dot_number}: {e}")
        return False


async def save_fmcsa_register_entries(entries: list[dict], extracted_date: str) -> dict:
    """Save FMCSA register entries in bulk using a single transaction.

    Uses batch INSERT with ON CONFLICT instead of individual queries,
    which is significantly faster for large entry sets.
    """
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    saved = 0
    skipped = 0
    batch_size = 500
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(entries), batch_size):
                    batch = entries[i:i + batch_size]
                    args = [
                        (
                            entry["number"],
                            entry.get("title", ""),
                            entry.get("decided", "N/A"),
                            entry.get("category", ""),
                            extracted_date,
                        )
                        for entry in batch
                    ]
                    await conn.executemany(
                        """
                        INSERT INTO fmcsa_register (number, title, decided, category, date_fetched)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (number, date_fetched) DO UPDATE SET
                            title = EXCLUDED.title,
                            decided = EXCLUDED.decided,
                            category = EXCLUDED.category,
                            updated_at = NOW()
                        """,
                        args,
                    )
                    saved += len(batch)
    except Exception as e:
        print(f"[DB] Error batch-saving FMCSA entries: {e}")
        skipped = len(entries) - saved

    return {"success": True, "saved": saved, "skipped": skipped}


async def fetch_fmcsa_register_by_date(
    extracted_date: str,
    category: Optional[str] = None,
    search_term: Optional[str] = None,
) -> list[dict]:
    """Fetch FMCSA register entries by date_fetched with optional filters."""
    pool = get_pool()

    conditions = ["date_fetched = $1"]
    params: list = [extracted_date]
    idx = 2

    if category:
        conditions.append(f"category = ${idx}")
        params.append(category)
        idx += 1

    if search_term:
        conditions.append(f"(title ILIKE ${idx} OR number ILIKE ${idx})")
        params.append(f"%{search_term}%")
        idx += 1

    where = " AND ".join(conditions)
    query = f"""
        SELECT number, title, decided, category, date_fetched
        FROM fmcsa_register
        WHERE {where}
        ORDER BY number
        LIMIT 10000
    """

    rows = await pool.fetch(query, *params)
    return [dict(row) for row in rows]


async def get_fmcsa_extracted_dates() -> list[str]:
    """Return distinct date_fetched values, sorted descending."""
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT date_fetched FROM fmcsa_register ORDER BY date_fetched DESC"
    )
    return [row["date_fetched"] for row in rows]


def _parse_jsonb(value) -> Optional[object]:
    """Parse a JSONB string back to a Python object."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _carrier_row_to_dict(row) -> dict:
    """Convert an asyncpg Record for a carrier to a plain dict with parsed JSONB."""
    d = dict(row)
    for key in ("basic_scores", "oos_rates", "insurance_policies", "inspections", "crashes"):
        if key in d:
            d[key] = _parse_jsonb(d[key])
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


async def fetch_carriers(filters: dict) -> list[dict]:
    """Fetch carriers with optional filters. Mirrors the frontend's Supabase query logic."""
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("mc_number"):
        conditions.append(f"mc_number ILIKE ${idx}")
        params.append(f"%{filters['mc_number']}%")
        idx += 1

    if filters.get("dot_number"):
        conditions.append(f"dot_number ILIKE ${idx}")
        params.append(f"%{filters['dot_number']}%")
        idx += 1

    if filters.get("legal_name"):
        conditions.append(f"legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    active = filters.get("active")
    if active == "true":
        conditions.append(f"status ILIKE ${idx}")
        params.append("%AUTHORIZED%")
        idx += 1
        conditions.append(f"status NOT ILIKE ${idx}")
        params.append("%NOT%")
        idx += 1
    elif active == "false":
        conditions.append(f"(status ILIKE ${idx} OR status NOT ILIKE ${idx + 1})")
        params.append("%NOT AUTHORIZED%")
        params.append("%AUTHORIZED%")
        idx += 2

    if filters.get("years_in_business_min"):
        conditions.append(f"mcs150_date IS NOT NULL AND mcs150_date != '' AND mcs150_date != 'N/A' AND (NOW() - mcs150_date::date) >= make_interval(years => ${idx})")
        params.append(int(filters["years_in_business_min"]))
        idx += 1
    if filters.get("years_in_business_max"):
        conditions.append(f"mcs150_date IS NOT NULL AND mcs150_date != '' AND mcs150_date != 'N/A' AND (NOW() - mcs150_date::date) <= make_interval(years => ${idx})")
        params.append(int(filters["years_in_business_max"]))
        idx += 1

    if filters.get("state"):
        states = filters["state"].split("|")
        or_clauses = []
        for s in states:
            or_clauses.append(f"physical_address ILIKE ${idx}")
            params.append(f"%, {s}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email IS NOT NULL AND email != ''")
    elif has_email == "false":
        conditions.append("(email IS NULL OR email = '')")

    has_boc3 = filters.get("has_boc3")
    if has_boc3 == "true":
        conditions.append("carrier_operation @> ARRAY['BOC-3']")
    elif has_boc3 == "false":
        conditions.append("NOT (carrier_operation @> ARRAY['BOC-3'])")

    has_company_rep = filters.get("has_company_rep")
    if has_company_rep == "true":
        conditions.append("dba_name IS NOT NULL AND dba_name != ''")
    elif has_company_rep == "false":
        conditions.append("(dba_name IS NULL OR dba_name = '')")

    if filters.get("classification"):
        classifications = filters["classification"]
        if isinstance(classifications, str):
            classifications = classifications.split(",")
        conditions.append(f"operation_classification && ${idx}::text[]")
        params.append(classifications)
        idx += 1

    if filters.get("carrier_operation"):
        ops = filters["carrier_operation"]
        if isinstance(ops, str):
            ops = ops.split(",")
        conditions.append(f"carrier_operation && ${idx}::text[]")
        params.append(ops)
        idx += 1

    if filters.get("cargo"):
        cargo = filters["cargo"]
        if isinstance(cargo, str):
            cargo = cargo.split(",")
        conditions.append(f"cargo_carried && ${idx}::text[]")
        params.append(cargo)
        idx += 1

    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("cargo_carried @> ARRAY['Hazardous Materials']")
    elif hazmat == "false":
        conditions.append("NOT (cargo_carried @> ARRAY['Hazardous Materials'])")

    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(power_units, '')::int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(power_units, '')::int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1
    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(drivers, '')::int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(drivers, '')::int <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    if filters.get("insurance_required"):
        ins_types = filters["insurance_required"]
        if isinstance(ins_types, str):
            ins_types = ins_types.split(",")
        or_clauses = []
        for itype in ins_types:
            or_clauses.append(f"insurance_policies @> ${idx}::jsonb")
            params.append(json.dumps([{"type": itype}]))
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    bipd_on_file = filters.get("bipd_on_file")
    if bipd_on_file == "1":
        conditions.append(f"insurance_policies @> ${idx}::jsonb")
        params.append(json.dumps([{"type": "BI&PD"}]))
        idx += 1
    cargo_on_file = filters.get("cargo_on_file")
    if cargo_on_file == "1":
        conditions.append(f"insurance_policies @> ${idx}::jsonb")
        params.append(json.dumps([{"type": "CARGO"}]))
        idx += 1
    bond_on_file = filters.get("bond_on_file")
    if bond_on_file == "1":
        conditions.append(f"insurance_policies @> ${idx}::jsonb")
        params.append(json.dumps([{"type": "BOND"}]))
        idx += 1

    if filters.get("oos_min"):
        conditions.append(
            f"(SELECT COALESCE(SUM((elem->>'oosViolations')::int), 0) "
            f"FROM jsonb_array_elements(COALESCE(inspections, '[]'::jsonb)) elem) >= ${idx}"
        )
        params.append(int(filters["oos_min"]))
        idx += 1
    if filters.get("oos_max"):
        conditions.append(
            f"(SELECT COALESCE(SUM((elem->>'oosViolations')::int), 0) "
            f"FROM jsonb_array_elements(COALESCE(inspections, '[]'::jsonb)) elem) <= ${idx}"
        )
        params.append(int(filters["oos_max"]))
        idx += 1

    if filters.get("crashes_min"):
        conditions.append(f"jsonb_array_length(COALESCE(crashes, '[]'::jsonb)) >= ${idx}")
        params.append(int(filters["crashes_min"]))
        idx += 1
    if filters.get("crashes_max"):
        conditions.append(f"jsonb_array_length(COALESCE(crashes, '[]'::jsonb)) <= ${idx}")
        params.append(int(filters["crashes_max"]))
        idx += 1

    if filters.get("injuries_min"):
        conditions.append(
            f"(SELECT COALESCE(SUM(CASE WHEN elem->>'injuries' ~ '^[0-9]+$' "
            f"THEN (elem->>'injuries')::int ELSE 0 END), 0) "
            f"FROM jsonb_array_elements(COALESCE(crashes, '[]'::jsonb)) elem) >= ${idx}"
        )
        params.append(int(filters["injuries_min"]))
        idx += 1
    if filters.get("injuries_max"):
        conditions.append(
            f"(SELECT COALESCE(SUM(CASE WHEN elem->>'injuries' ~ '^[0-9]+$' "
            f"THEN (elem->>'injuries')::int ELSE 0 END), 0) "
            f"FROM jsonb_array_elements(COALESCE(crashes, '[]'::jsonb)) elem) <= ${idx}"
        )
        params.append(int(filters["injuries_max"]))
        idx += 1

    if filters.get("fatalities_min"):
        conditions.append(
            f"(SELECT COALESCE(COUNT(*), 0) "
            f"FROM jsonb_array_elements(COALESCE(crashes, '[]'::jsonb)) elem "
            f"WHERE elem->>'fatal' IS NOT NULL AND elem->>'fatal' NOT IN ('No', '0', '', 'N/A')) >= ${idx}"
        )
        params.append(int(filters["fatalities_min"]))
        idx += 1
    if filters.get("fatalities_max"):
        conditions.append(
            f"(SELECT COALESCE(COUNT(*), 0) "
            f"FROM jsonb_array_elements(COALESCE(crashes, '[]'::jsonb)) elem "
            f"WHERE elem->>'fatal' IS NOT NULL AND elem->>'fatal' NOT IN ('No', '0', '', 'N/A')) <= ${idx}"
        )
        params.append(int(filters["fatalities_max"]))
        idx += 1

    if filters.get("inspections_min"):
        conditions.append(f"jsonb_array_length(COALESCE(inspections, '[]'::jsonb)) >= ${idx}")
        params.append(int(filters["inspections_min"]))
        idx += 1
    if filters.get("inspections_max"):
        conditions.append(f"jsonb_array_length(COALESCE(inspections, '[]'::jsonb)) <= ${idx}")
        params.append(int(filters["inspections_max"]))
        idx += 1

    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    limit_val = 200
    if is_filtered:
        limit_val = int(filters.get("limit", 10000))
    else:
        limit_val = int(filters.get("limit", 200))

    query = f"""
        SELECT * FROM carriers
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT {limit_val}
    """

    try:
        rows = await pool.fetch(query, *params)
        return [_carrier_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return []


async def delete_carrier(mc_number: str) -> bool:
    """Delete a carrier by mc_number."""
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM carriers WHERE mc_number = $1", mc_number
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting carrier {mc_number}: {e}")
        return False


async def get_carrier_count() -> int:
    """Return total number of carriers."""
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM carriers")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting carrier count: {e}")
        return 0


async def update_carrier_safety(dot_number: str, safety_data: dict) -> bool:
    """Update safety fields for a carrier by dot_number."""
    pool = get_pool()
    try:
        result = await pool.execute(
            """
            UPDATE carriers
            SET safety_rating = $1,
                safety_rating_date = $2,
                basic_scores = $3,
                oos_rates = $4,
                updated_at = NOW()
            WHERE dot_number = $5
            """,
            safety_data.get("rating"),
            safety_data.get("ratingDate"),
            _to_jsonb(safety_data.get("basicScores")),
            _to_jsonb(safety_data.get("oosRates")),
            dot_number,
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating safety for DOT {dot_number}: {e}")
        return False


async def get_carriers_by_mc_range(start: str, end: str) -> list[dict]:
    """Fetch carriers within a specific MC Number range (numeric comparison)."""
    pool = get_pool()
    try:
        rows = await pool.fetch(
            """
            SELECT * FROM carriers
            WHERE mc_number ~ '^[0-9]+$'
              AND mc_number::bigint >= $1::bigint
              AND mc_number::bigint <= $2::bigint
            ORDER BY mc_number::bigint ASC
            """,
            start,
            end,
        )
        return [_carrier_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching MC range: {e}")
        return []


def _user_row_to_dict(row) -> dict:
    """Convert an asyncpg Record for a user to a plain dict."""
    d = dict(row)
    for key in ("created_at", "updated_at", "blocked_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


async def fetch_users() -> list[dict]:
    """Fetch all users ordered by created_at descending. Excludes password_hash."""
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT id, user_id, name, email, role, plan, daily_limit, "
            "records_extracted_today, last_active, ip_address, is_online, "
            "is_blocked, created_at, updated_at FROM users ORDER BY created_at DESC"
        )
        return [_user_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching users: {e}")
        return []


async def fetch_user_by_email(email: str) -> Optional[dict]:
    """Fetch a single user by email. Excludes password_hash."""
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT id, user_id, name, email, role, plan, daily_limit, "
            "records_extracted_today, last_active, ip_address, is_online, "
            "is_blocked, created_at, updated_at FROM users WHERE email = $1",
            email.lower(),
        )
        if row:
            return _user_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error fetching user by email: {e}")
        return None


async def create_user(user_data: dict) -> Optional[dict]:
    """Insert a new user and return the created record."""
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            """
            INSERT INTO users (user_id, name, email, password_hash, role, plan,
                               daily_limit, records_extracted_today, last_active,
                               ip_address, is_online, is_blocked)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING *
            """,
            user_data.get("user_id"),
            user_data.get("name"),
            user_data.get("email", "").lower(),
            user_data.get("password_hash"),
            user_data.get("role", "user"),
            user_data.get("plan", "Free"),
            user_data.get("daily_limit", 50),
            user_data.get("records_extracted_today", 0),
            user_data.get("last_active", "Never"),
            user_data.get("ip_address"),
            user_data.get("is_online", False),
            user_data.get("is_blocked", False),
        )
        if row:
            return _user_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error creating user: {e}")
        return None


async def update_user(user_id: str, user_data: dict) -> bool:
    """Update an existing user by user_id."""
    pool = get_pool()
    try:
        result = await pool.execute(
            """
            UPDATE users SET
                name = $1, role = $2, plan = $3, daily_limit = $4,
                records_extracted_today = $5, last_active = $6,
                ip_address = $7, is_online = $8, is_blocked = $9
            WHERE user_id = $10
            """,
            user_data.get("name"),
            user_data.get("role"),
            user_data.get("plan"),
            user_data.get("daily_limit"),
            user_data.get("records_extracted_today"),
            user_data.get("last_active"),
            user_data.get("ip_address"),
            user_data.get("is_online", False),
            user_data.get("is_blocked", False),
            user_id,
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating user {user_id}: {e}")
        return False


async def delete_user(user_id: str) -> bool:
    """Delete a user by user_id."""
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM users WHERE user_id = $1", user_id
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting user {user_id}: {e}")
        return False


async def get_user_password_hash(email: str) -> Optional[str]:
    """Return the password_hash for a user by email."""
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT password_hash FROM users WHERE email = $1", email.lower()
        )
        if row and row["password_hash"]:
            return row["password_hash"]
        return None
    except Exception as e:
        print(f"[DB] Error fetching password hash: {e}")
        return None


async def fetch_blocked_ips() -> list[dict]:
    """Fetch all blocked IPs ordered by blocked_at descending."""
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT * FROM blocked_ips ORDER BY blocked_at DESC"
        )
        return [_user_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching blocked IPs: {e}")
        return []


async def block_ip(ip_address: str, reason: str) -> bool:
    """Block an IP address."""
    pool = get_pool()
    try:
        await pool.execute(
            """
            INSERT INTO blocked_ips (ip_address, reason)
            VALUES ($1, $2)
            ON CONFLICT (ip_address) DO NOTHING
            """,
            ip_address,
            reason or "No reason provided",
        )
        return True
    except Exception as e:
        print(f"[DB] Error blocking IP {ip_address}: {e}")
        return False


async def unblock_ip(ip_address: str) -> bool:
    """Unblock an IP address."""
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM blocked_ips WHERE ip_address = $1", ip_address
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error unblocking IP {ip_address}: {e}")
        return False


async def is_ip_blocked(ip_address: str) -> bool:
    """Check if an IP address is blocked."""
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT ip_address FROM blocked_ips WHERE ip_address = $1",
            ip_address,
        )
        return row is not None
    except Exception as e:
        print(f"[DB] Error checking IP block status: {e}")
        return False


async def get_fmcsa_categories() -> list[str]:
    """Return all unique categories from fmcsa_register."""
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT category FROM fmcsa_register WHERE category IS NOT NULL ORDER BY category"
        )
        return [row["category"] for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching FMCSA categories: {e}")
        return []


async def delete_fmcsa_entries_before_date(date: str) -> int:
    """Delete FMCSA register entries before a date. Returns count deleted."""
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM fmcsa_register WHERE date_fetched < $1", date
        )
        parts = result.split(" ")
        return int(parts[-1]) if len(parts) > 1 else 0
    except Exception as e:
        print(f"[DB] Error deleting FMCSA entries: {e}")
        return 0


def _new_venture_row_to_dict(row) -> dict:
    """Convert an asyncpg Record for a new_venture to a plain dict."""
    d = dict(row)
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


_NEW_VENTURE_COLUMNS = [
    "dot_number", "mc_number", "legal_name", "dba_name", "entity_type",
    "status", "email", "phone", "physical_address", "mailing_address",
    "power_units", "drivers", "cargo_carried", "hazmat_indicator",
    "operation_classification", "carrier_operation", "safety_rating",
    "safety_rating_type_code", "safety_rating_effective_date",
    "safety_rating_latest_review_type", "safety_rating_latest_review_date",
    "mcsip_step_number", "interstate_within_100", "interstate_beyond_100",
    "interstate_total", "intrastate_within_100", "intrastate_beyond_100",
    "intrastate_total", "avg_trip_leased_drivers", "grand_total_drivers",
    "total_cdl", "total_non_cdl", "total_trucks", "total_power_units",
    "fleet_size_code", "owned_trucks", "term_leased_trucks", "trip_leased_trucks",
    "owned_tractors", "term_leased_tractors", "trip_leased_tractors",
    "owned_trailers", "term_leased_trailers", "trip_leased_trailers",
    "recordable_accident_rate", "preventable_accident_rate",
    "mcs150_mileage", "mcs150_mileage_year", "mcs150_date",
    "state_carrier_id", "duns_number", "out_of_service_date",
    "date_added", "date_fetched",
]


async def save_new_venture_records(records: list[dict], date_fetched: str) -> dict:
    """Bulk upsert new venture records. Returns saved/skipped counts."""
    pool = get_pool()
    if not records:
        return {"success": True, "saved": 0, "skipped": 0}

    saved = 0
    skipped = 0
    batch_size = 200

    col_names = ", ".join(_NEW_VENTURE_COLUMNS)
    placeholders = ", ".join(f"${i+1}" for i in range(len(_NEW_VENTURE_COLUMNS)))
    update_clauses = ", ".join(
        f"{col} = EXCLUDED.{col}"
        for col in _NEW_VENTURE_COLUMNS
        if col not in ("dot_number", "date_added")
    )

    query = f"""
        INSERT INTO new_ventures ({col_names})
        VALUES ({placeholders})
        ON CONFLICT (dot_number, date_added) DO UPDATE SET
            {update_clauses},
            updated_at = NOW()
    """

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    args = []
                    for rec in batch:
                        row_args = tuple(
                            rec.get(col, "") or ""
                            for col in _NEW_VENTURE_COLUMNS
                        )
                        # Override date_fetched
                        row_list = list(row_args)
                        date_fetched_idx = _NEW_VENTURE_COLUMNS.index("date_fetched")
                        row_list[date_fetched_idx] = date_fetched
                        args.append(tuple(row_list))
                    await conn.executemany(query, args)
                    saved += len(batch)
    except Exception as e:
        print(f"[DB] Error saving new venture records: {e}")
        skipped = len(records) - saved

    return {"success": True, "saved": saved, "skipped": skipped}


async def fetch_new_ventures(filters: dict) -> list[dict]:
    """Fetch new ventures with optional filters including date range."""
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    # Date range filter
    if filters.get("date_from"):
        conditions.append(f"date_added >= ${idx}")
        params.append(filters["date_from"])
        idx += 1
    if filters.get("date_to"):
        conditions.append(f"date_added <= ${idx}")
        params.append(filters["date_to"])
        idx += 1

    if filters.get("mc_number"):
        conditions.append(f"mc_number ILIKE ${idx}")
        params.append(f"%{filters['mc_number']}%")
        idx += 1

    if filters.get("dot_number"):
        conditions.append(f"dot_number ILIKE ${idx}")
        params.append(f"%{filters['dot_number']}%")
        idx += 1

    if filters.get("legal_name"):
        conditions.append(f"legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    active = filters.get("active")
    if active == "true":
        conditions.append(f"status ILIKE ${idx}")
        params.append("%AUTHORIZED%")
        idx += 1
        conditions.append(f"status NOT ILIKE ${idx}")
        params.append("%NOT%")
        idx += 1
    elif active == "false":
        conditions.append(f"(status ILIKE ${idx} OR status NOT ILIKE ${idx + 1})")
        params.append("%NOT AUTHORIZED%")
        params.append("%AUTHORIZED%")
        idx += 2

    if filters.get("state"):
        states = filters["state"].split("|")
        or_clauses = []
        for s in states:
            or_clauses.append(f"physical_address ILIKE ${idx}")
            params.append(f"%, {s}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email IS NOT NULL AND email != ''")
    elif has_email == "false":
        conditions.append("(email IS NULL OR email = '')")

    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("(hazmat_indicator ILIKE '%yes%' OR hazmat_indicator = 'Y')")
    elif hazmat == "false":
        conditions.append("(hazmat_indicator IS NULL OR hazmat_indicator = '' OR hazmat_indicator ILIKE '%no%' OR hazmat_indicator = 'N')")

    if filters.get("classification"):
        classifications = filters["classification"]
        if isinstance(classifications, str):
            classifications = classifications.split(",")
        or_clauses = []
        for cls in classifications:
            or_clauses.append(f"operation_classification ILIKE ${idx}")
            params.append(f"%{cls}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    if filters.get("carrier_operation"):
        ops = filters["carrier_operation"]
        if isinstance(ops, str):
            ops = ops.split(",")
        or_clauses = []
        for op in ops:
            or_clauses.append(f"carrier_operation ILIKE ${idx}")
            params.append(f"%{op}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    if filters.get("cargo"):
        cargo = filters["cargo"]
        if isinstance(cargo, str):
            cargo = cargo.split(",")
        or_clauses = []
        for c in cargo:
            or_clauses.append(f"cargo_carried ILIKE ${idx}")
            params.append(f"%{c}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(power_units, '')::int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(power_units, '')::int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1

    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(drivers, '')::int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(drivers, '')::int <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    if filters.get("search"):
        conditions.append(
            f"(legal_name ILIKE ${idx} OR dot_number ILIKE ${idx} "
            f"OR mc_number ILIKE ${idx} OR email ILIKE ${idx})"
        )
        params.append(f"%{filters['search']}%")
        idx += 1

    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    if is_filtered:
        limit_val = int(filters.get("limit", 10000))
    else:
        limit_val = int(filters.get("limit", 200))

    query = f"""
        SELECT * FROM new_ventures
        WHERE {where}
        ORDER BY date_added DESC, created_at DESC
        LIMIT {limit_val}
    """

    try:
        rows = await pool.fetch(query, *params)
        return [_new_venture_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching new ventures: {e}")
        return []


async def get_new_venture_count() -> int:
    """Return total number of new ventures."""
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM new_ventures")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting new venture count: {e}")
        return 0


async def get_new_venture_dates() -> list[str]:
    """Return distinct date_added values, sorted descending."""
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT date_added FROM new_ventures ORDER BY date_added DESC"
        )
        return [row["date_added"] for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching new venture dates: {e}")
        return []


async def delete_new_ventures_by_date(date_added: str) -> int:
    """Delete new venture entries for a specific date. Returns count deleted."""
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM new_ventures WHERE date_added = $1", date_added
        )
        parts = result.split(" ")
        return int(parts[-1]) if len(parts) > 1 else 0
    except Exception as e:
        print(f"[DB] Error deleting new ventures: {e}")
        return 0


def _to_jsonb(value) -> Optional[str]:
    """Convert a Python object to a JSON string for JSONB columns, or None."""
    if value is None:
        return None
    return json.dumps(value)
