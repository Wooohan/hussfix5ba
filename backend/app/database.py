import os
import json
import math
import asyncio
import time as _time
import asyncpg
from typing import Optional

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    import warnings
    warnings.warn("DATABASE_URL is not set. Database connections will fail.")

_pool: Optional[asyncpg.Pool] = None

# ── Dashboard stat cache ────────────────────────────────────────────────────
_dashboard_cache: Optional[dict] = None
_dashboard_cache_ts: float = 0.0
_DASHBOARD_CACHE_TTL = 300  # 5 minutes

_SCHEMA_SQL = """
-- ── Tables ──────────────────────────────────────────────────────────────────
-- carriers table is populated externally (~4.4M records).
-- Schema managed externally; CREATE IF NOT EXISTS for reference only.

CREATE TABLE IF NOT EXISTS carriers (
    dot_number              BIGINT,
    mc_number               BIGINT,
    dockets                 BIGINT,
    legal_name              CHARACTER VARYING,
    dba_name                CHARACTER VARYING,
    status_code             CHARACTER(1),
    carrier_operation       CHARACTER(1),
    classdef                CHARACTER VARYING,
    hm_ind                  BOOLEAN,
    add_date                DATE,
    mcs150_date             DATE,
    mcs150_mileage          BIGINT,
    mcs150_mileage_year     INTEGER,
    total_cars              INTEGER,
    truck_units             INTEGER,
    power_units             INTEGER,
    fleetsize               INTEGER,
    total_drivers           INTEGER,
    total_intrastate_drivers INTEGER,
    total_cdl               INTEGER,
    avg_drivers_leased_per_month NUMERIC,
    phone                   CHARACTER VARYING,
    fax                     CHARACTER VARYING,
    cell_phone              CHARACTER VARYING,
    email_address           CHARACTER VARYING,
    company_officer_1       CHARACTER VARYING,
    company_officer_2       CHARACTER VARYING,
    phy_street              CHARACTER VARYING,
    phy_city                CHARACTER VARYING,
    phy_state               CHARACTER VARYING,
    phy_zip                 CHARACTER VARYING,
    phy_country             CHARACTER VARYING,
    phy_cnty                CHARACTER VARYING,
    phy_nationality_indicator CHARACTER VARYING,
    carrier_mailing_street  CHARACTER VARYING,
    carrier_mailing_city    CHARACTER VARYING,
    carrier_mailing_state   CHARACTER VARYING,
    carrier_mailing_zip     CHARACTER VARYING,
    carrier_mailing_country CHARACTER VARYING,
    carrier_mailing_cnty    CHARACTER VARYING,
    dun_bradstreet_no       CHARACTER VARYING,
    driver_inter_total      CHARACTER VARYING,
    docket1_status_code     CHARACTER VARYING,
    docket2_status_code     CHARACTER VARYING,
    docket3_status_code     CHARACTER VARYING,
    prior_revoke_flag       CHARACTER VARYING,
    prior_revoke_dot_number CHARACTER VARYING,
    mcsipdate               CHARACTER VARYING,
    interstate_beyond_100_miles  BOOLEAN,
    interstate_within_100_miles  BOOLEAN,
    intrastate_beyond_100_miles  BOOLEAN,
    intrastate_within_100_miles  BOOLEAN,
    cargo                   JSONB,
    other_dockets           JSONB,
    equipment               JSONB
);

-- ── Insurance history ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS insurance_history (
    id SERIAL PRIMARY KEY,
    docket_number CHARACTER VARYING(20),
    dot_number TEXT,
    ins_form_code CHARACTER VARYING(10),
    ins_type_desc CHARACTER VARYING(50),
    name_company CHARACTER VARYING(100),
    policy_no CHARACTER VARYING(50),
    trans_date CHARACTER VARYING(15),
    underl_lim_amount CHARACTER VARYING(15),
    max_cov_amount CHARACTER VARYING(15),
    effective_date CHARACTER VARYING(15),
    cancl_effective_date CHARACTER VARYING(15)
);

-- ── Inspections ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inspections (
    unique_id BIGINT,
    report_number TEXT,
    report_state TEXT,
    dot_number BIGINT,
    insp_date TEXT,
    insp_level_id BIGINT,
    county_code_state TEXT,
    time_weight BIGINT,
    driver_oos_total BIGINT,
    vehicle_oos_total BIGINT,
    total_hazmat_sent BIGINT,
    oos_total BIGINT,
    hazmat_oos_total BIGINT,
    hazmat_placard_req BOOLEAN,
    unit_type_desc TEXT,
    unit_make TEXT,
    unit_license TEXT,
    unit_license_state TEXT,
    vin TEXT,
    unit_decal_number TEXT,
    unit_type_desc2 TEXT,
    unit_make2 TEXT,
    unit_license2 TEXT,
    unit_license_state2 TEXT,
    vin2 TEXT,
    unit_decal_number2 TEXT,
    unsafe_insp BOOLEAN,
    fatigued_insp BOOLEAN,
    dr_fitness_insp BOOLEAN,
    subt_alcohol_insp BOOLEAN,
    vh_maint_insp BOOLEAN,
    hm_insp BOOLEAN,
    basic_viol BIGINT,
    unsafe_viol BIGINT,
    fatigued_viol BIGINT,
    dr_fitness_viol BIGINT,
    subt_alcohol_viol BIGINT,
    vh_maint_viol BIGINT,
    hm_viol DOUBLE PRECISION
);

-- ── Crashes ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS crashes (
    report_number TEXT,
    report_seq_no INTEGER,
    dot_number TEXT,
    report_date DATE,
    report_state TEXT,
    fatalities INTEGER,
    injuries INTEGER,
    tow_away BOOLEAN,
    hazmat_released BOOLEAN,
    trafficway_desc TEXT,
    access_control_desc TEXT,
    road_surface_condition_desc TEXT,
    weather_condition_desc TEXT,
    light_condition_desc TEXT,
    vehicle_id_number TEXT,
    vehicle_license_number TEXT,
    vehicle_license_state TEXT,
    citation_issued_desc TEXT,
    seq_num INTEGER,
    not_preventable BOOLEAN
);

-- ── Safety ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS safety (
    dot_number BIGINT,
    insp_total INTEGER,
    driver_insp_total INTEGER,
    driver_oos_insp_total INTEGER,
    vehicle_insp_total INTEGER,
    vehicle_oos_insp_total INTEGER,
    unsafe_driv_insp_w_viol INTEGER,
    unsafe_driv_measure NUMERIC,
    unsafe_driv_ac VARCHAR(255),
    hos_driv_insp_w_viol INTEGER,
    hos_driv_measure NUMERIC,
    hos_driv_ac VARCHAR(255),
    driv_fit_insp_w_viol INTEGER,
    driv_fit_measure NUMERIC,
    driv_fit_ac VARCHAR(255),
    contr_subst_insp_w_viol INTEGER,
    contr_subst_measure NUMERIC,
    contr_subst_ac VARCHAR(255),
    veh_maint_insp_w_viol INTEGER,
    veh_maint_measure NUMERIC,
    veh_maint_ac VARCHAR(255),
    type VARCHAR(50)
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

-- ── New Ventures table (ALL BrokerSnapshot CSV columns) ──────────────────────
CREATE TABLE IF NOT EXISTS new_ventures (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    dot_number TEXT,
    prefix TEXT,
    docket_number TEXT,
    status_code TEXT,
    carship TEXT,
    carrier_operation TEXT,
    name TEXT,
    name_dba TEXT,
    add_date TEXT,
    chgn_date TEXT,
    common_stat TEXT,
    contract_stat TEXT,
    broker_stat TEXT,
    common_app_pend TEXT,
    contract_app_pend TEXT,
    broker_app_pend TEXT,
    common_rev_pend TEXT,
    contract_rev_pend TEXT,
    broker_rev_pend TEXT,
    property_chk TEXT,
    passenger_chk TEXT,
    hhg_chk TEXT,
    private_auth_chk TEXT,
    enterprise_chk TEXT,
    operating_status TEXT,
    operating_status_indicator TEXT,
    phy_str TEXT,
    phy_city TEXT,
    phy_st TEXT,
    phy_zip TEXT,
    phy_country TEXT,
    phy_cnty TEXT,
    mai_str TEXT,
    mai_city TEXT,
    mai_st TEXT,
    mai_zip TEXT,
    mai_country TEXT,
    mai_cnty TEXT,
    phy_undeliv TEXT,
    mai_undeliv TEXT,
    phy_phone TEXT,
    phy_fax TEXT,
    mai_phone TEXT,
    mai_fax TEXT,
    cell_phone TEXT,
    email_address TEXT,
    company_officer_1 TEXT,
    company_officer_2 TEXT,
    genfreight TEXT,
    household TEXT,
    metalsheet TEXT,
    motorveh TEXT,
    drivetow TEXT,
    logpole TEXT,
    bldgmat TEXT,
    mobilehome TEXT,
    machlrg TEXT,
    produce TEXT,
    liqgas TEXT,
    intermodal TEXT,
    passengers TEXT,
    oilfield TEXT,
    livestock TEXT,
    grainfeed TEXT,
    coalcoke TEXT,
    meat TEXT,
    garbage TEXT,
    usmail TEXT,
    chem TEXT,
    drybulk TEXT,
    coldfood TEXT,
    beverages TEXT,
    paperprod TEXT,
    utility TEXT,
    farmsupp TEXT,
    construct TEXT,
    waterwell TEXT,
    cargoothr TEXT,
    cargoothr_desc TEXT,
    hm_ind TEXT,
    bipd_req TEXT,
    cargo_req TEXT,
    bond_req TEXT,
    bipd_file TEXT,
    cargo_file TEXT,
    bond_file TEXT,
    owntruck TEXT,
    owntract TEXT,
    owntrail TEXT,
    owncoach TEXT,
    ownschool_1_8 TEXT,
    ownschool_9_15 TEXT,
    ownschool_16 TEXT,
    ownbus_16 TEXT,
    ownvan_1_8 TEXT,
    ownvan_9_15 TEXT,
    ownlimo_1_8 TEXT,
    ownlimo_9_15 TEXT,
    ownlimo_16 TEXT,
    trmtruck TEXT,
    trmtract TEXT,
    trmtrail TEXT,
    trmcoach TEXT,
    trmschool_1_8 TEXT,
    trmschool_9_15 TEXT,
    trmschool_16 TEXT,
    trmbus_16 TEXT,
    trmvan_1_8 TEXT,
    trmvan_9_15 TEXT,
    trmlimo_1_8 TEXT,
    trmlimo_9_15 TEXT,
    trmlimo_16 TEXT,
    trptruck TEXT,
    trptract TEXT,
    trptrail TEXT,
    trpcoach TEXT,
    trpschool_1_8 TEXT,
    trpschool_9_15 TEXT,
    trpschool_16 TEXT,
    trpbus_16 TEXT,
    trpvan_1_8 TEXT,
    trpvan_9_15 TEXT,
    trplimo_1_8 TEXT,
    trplimo_9_15 TEXT,
    trplimo_16 TEXT,
    total_trucks TEXT,
    total_buses TEXT,
    total_pwr INTEGER,
    fleetsize TEXT,
    inter_within_100 TEXT,
    inter_beyond_100 TEXT,
    total_inter_drivers TEXT,
    intra_within_100 TEXT,
    intra_beyond_100 TEXT,
    total_intra_drivers TEXT,
    total_drivers TEXT,
    avg_tld TEXT,
    total_cdl TEXT,
    review_type TEXT,
    review_id TEXT,
    review_date TEXT,
    recordable_crash_rate TEXT,
    mcs150_mileage TEXT,
    mcs151_mileage TEXT,
    mcs150_mileage_year TEXT,
    mcs150_date TEXT,
    safety_rating TEXT,
    safety_rating_date TEXT,
    arber TEXT,
    smartway TEXT,
    tia TEXT,
    tia_phone TEXT,
    tia_contact_name TEXT,
    tia_tool_free TEXT,
    tia_fax TEXT,
    tia_email TEXT,
    tia_website TEXT,
    phy_ups_store TEXT,
    mai_ups_store TEXT,
    phy_mail_box TEXT,
    mai_mail_box TEXT,
    raw_data JSONB,
    scrape_date TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(dot_number, add_date)
);

CREATE OR REPLACE FUNCTION update_new_ventures_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_new_ventures_updated_at ON new_ventures;
CREATE TRIGGER update_new_ventures_updated_at BEFORE UPDATE ON new_ventures
    FOR EACH ROW EXECUTE FUNCTION update_new_ventures_updated_at();

-- ── Default admin user ──────────────────────────────────────────────────────
INSERT INTO users (user_id, name, email, role, plan, daily_limit, records_extracted_today, ip_address, is_online, is_blocked)
VALUES ('1', 'Admin User', 'wooohan3@gmail.com', 'admin', 'Enterprise', 100000, 0, '192.168.1.1', false, false)
ON CONFLICT (email) DO NOTHING;

-- ── Performance indexes ─────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number ON carriers(dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_mc_number ON carriers(mc_number);
CREATE INDEX IF NOT EXISTS idx_carriers_status_code ON carriers(status_code);
CREATE INDEX IF NOT EXISTS idx_carriers_legal_name ON carriers(legal_name);
CREATE INDEX IF NOT EXISTS idx_crashes_dot_number ON crashes(dot_number);
CREATE INDEX IF NOT EXISTS idx_inspections_dot_number ON inspections(dot_number);
CREATE INDEX IF NOT EXISTS idx_insurance_history_docket ON insurance_history(docket_number);
CREATE INDEX IF NOT EXISTS idx_insurance_history_dot ON insurance_history(dot_number);
CREATE INDEX IF NOT EXISTS idx_safety_dot_number ON safety(dot_number);
"""

async def connect_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=20,
        command_timeout=60,
    )
    try:
        async with _pool.acquire() as conn:
            await conn.execute(_SCHEMA_SQL)
        print("[DB] Connected to PostgreSQL, schema initialized, pool created")
    except Exception as e:
        print(f"[DB] Connected to PostgreSQL, pool created (schema init skipped: {e})")

async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
    _pool = None
    print("[DB] PostgreSQL connection pool closed")

def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database not connected. Call connect_db() first.")
    return _pool

async def upsert_carrier(record: dict) -> bool:
    """Upsert a carrier using Census File column names.

    The carriers table uses dot_number (bigint) as the natural key.
    """
    pool = get_pool()
    dot = record.get("dot_number")
    if not dot:
        return False
    try:
        await pool.execute(
            """
            INSERT INTO carriers (
                dot_number, legal_name, dba_name, phone, email_address,
                power_units, total_drivers, phy_street, phy_city, phy_state, phy_zip, phy_country,
                carrier_mailing_street, carrier_mailing_city, carrier_mailing_state, carrier_mailing_zip,
                mcs150_date, mcs150_mileage, mcs150_mileage_year,
                classdef, carrier_operation, hm_ind,
                dun_bradstreet_no, status_code, mc_number
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16,
                $17, $18, $19,
                $20, $21, $22,
                $23, $24, $25
            )
            ON CONFLICT (dot_number) DO UPDATE SET
                legal_name = EXCLUDED.legal_name,
                dba_name = EXCLUDED.dba_name,
                phone = EXCLUDED.phone,
                email_address = EXCLUDED.email_address,
                power_units = EXCLUDED.power_units,
                total_drivers = EXCLUDED.total_drivers,
                phy_street = EXCLUDED.phy_street,
                phy_city = EXCLUDED.phy_city,
                phy_state = EXCLUDED.phy_state,
                phy_zip = EXCLUDED.phy_zip,
                mcs150_date = EXCLUDED.mcs150_date,
                mcs150_mileage = EXCLUDED.mcs150_mileage,
                classdef = EXCLUDED.classdef,
                carrier_operation = EXCLUDED.carrier_operation,
                hm_ind = EXCLUDED.hm_ind,
                dun_bradstreet_no = EXCLUDED.dun_bradstreet_no,
                status_code = EXCLUDED.status_code,
                mc_number = EXCLUDED.mc_number
            """,
            int(dot),
            record.get("legal_name"),
            record.get("dba_name"),
            record.get("phone"),
            record.get("email_address", record.get("email")),
            record.get("power_units"),
            record.get("total_drivers", record.get("drivers")),
            record.get("phy_street"),
            record.get("phy_city"),
            record.get("phy_state"),
            record.get("phy_zip"),
            record.get("phy_country"),
            record.get("carrier_mailing_street"),
            record.get("carrier_mailing_city"),
            record.get("carrier_mailing_state"),
            record.get("carrier_mailing_zip"),
            record.get("mcs150_date"),
            record.get("mcs150_mileage"),
            record.get("mcs150_mileage_year"),
            record.get("classdef"),
            record.get("carrier_operation"),
            record.get("hm_ind"),
            record.get("dun_bradstreet_no", record.get("duns_number")),
            record.get("status_code"),
            record.get("mc_number"),
        )
        return True
    except Exception as e:
        print(f"[DB] Error upserting carrier DOT {dot}: {e}")
        return False

async def update_carrier_insurance(dot_number: str, policies: list) -> bool:
    """Insurance data is now in the separate insurance_history table.

    This is a no-op for the Census schema but kept for API compatibility.
    """
    _ = policies  # insurance is managed via insurance_history table
    print(f"[DB] update_carrier_insurance called for DOT {dot_number} – "
          "insurance is managed via insurance_history table, skipping.")
    return True

async def save_fmcsa_register_entries(entries: list[dict], extracted_date: str) -> dict:
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
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT date_fetched FROM fmcsa_register ORDER BY date_fetched DESC"
    )
    return [row["date_fetched"] for row in rows]

def _parse_jsonb(value) -> Optional[object]:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value

def _format_insurance_history(raw_filings) -> list[dict]:
    if not raw_filings:
        return []
    results = []
    for row in raw_filings:
        raw_amount = (row.get("max_cov_amount") or "").strip()
        try:
            amount_int = int(raw_amount) * 1000
            coverage = f"${amount_int:,}"
        except (ValueError, TypeError):
            coverage = raw_amount or "N/A"
        cancl = (row.get("cancl_effective_date") or "").strip()
        results.append({
            "type": (row.get("ins_type_desc") or "").strip(),
            "coverageAmount": coverage,
            "policyNumber": (row.get("policy_no") or "").strip(),
            "effectiveDate": (row.get("effective_date") or "").strip(),
            "carrier": (row.get("name_company") or "").strip(),
            "formCode": (row.get("ins_form_code") or "").strip(),
            "transDate": (row.get("trans_date") or "").strip(),
            "underlLimAmount": (row.get("underl_lim_amount") or "").strip(),
            "canclEffectiveDate": cancl,
            "status": "Cancelled" if cancl else "Active",
        })
    return results

_CARGO_LABEL_MAP = {
    "General Freight": "General Freight",
    "Household Goods": "Household Goods",
    "Metal Sheets": "Metal Sheets",
    "Motor Vehicles": "Motor Vehicles",
    "Drive/Tow away": "Drive/Tow away",
    "Logs, Poles, Beams, Lumber": "Logs, Poles, Beams, Lumber",
    "Building Materials": "Building Materials",
    "Mobile Homes": "Mobile Homes",
    "Machinery, Large Objects": "Machinery, Large Objects",
    "Fresh Produce": "Fresh Produce",
    "Liquids/Gases": "Liquids/Gases",
    "Intermodal Containers": "Intermodal Containers",
    "Passengers": "Passengers",
    "Oilfield Equipment": "Oilfield Equipment",
    "Livestock": "Livestock",
    "Grain, Feed, Hay": "Grain, Feed, Hay",
    "Coal/Coke": "Coal/Coke",
    "Meat": "Meat",
    "Garbage/Refuse": "Garbage/Refuse",
    "US Mail": "US Mail",
    "Chemicals": "Chemicals",
    "Commodities Dry Bulk": "Commodities Dry Bulk",
    "Refrigerated Food": "Refrigerated Food",
    "Beverages": "Beverages",
    "Paper Products": "Paper Products",
    "Utilities": "Utilities",
    "Agricultural/Farm Supplies": "Agricultural/Farm Supplies",
    "Construction": "Construction",
    "Water Well": "Water Well",
    "Other": "Other",
}

_CARRIER_OP_MAP = {
    "A": "Interstate",
    "B": "Intrastate Only (HM)",
    "C": "Intrastate Only (Non-HM)",
}

_STATUS_CODE_MAP = {
    "A": "Active",
    "I": "Inactive",
    "P": "Pending",
}

def _build_mc_number(d: dict) -> str:
    """Build a display MC number from the mc_number field.

    mc_number is now a bigint column. Also appends any other_dockets entries.
    """
    mc = d.get("mc_number")
    parts = []
    if mc:
        parts.append(f"MC-{mc}")
    other = d.get("other_dockets")
    if other and isinstance(other, list):
        for od in other:
            if od:
                parts.append(str(od))
    return ", ".join(parts)

def _build_address(street: str, city: str, state: str, zipcode: str, country: str = "") -> str:
    parts = [p for p in [street, city, state, zipcode] if p]
    addr = ", ".join(parts)
    if country and country != "US":
        addr = f"{addr}, {country}" if addr else country
    return addr

def _build_cargo_list(d: dict) -> list[str]:
    """Build a list of human-readable cargo types from cargo JSONB column."""
    cargo = d.get("cargo")
    if not cargo or not isinstance(cargo, dict):
        return []
    result = []
    other_desc = cargo.get("crgo_cargoothr_desc")
    for key, val in cargo.items():
        if key == "crgo_cargoothr_desc":
            continue
        label = _CARGO_LABEL_MAP.get(key, key)
        if val and str(val).strip().upper() == "X":
            result.append(label)
    if other_desc and str(other_desc).strip():
        if "Other" in result:
            result.remove("Other")
        result.append(str(other_desc).strip())
    return result

def _format_mcs150_date(raw) -> str:
    """Format mcs150_date (now a date type) to MM/DD/YYYY."""
    if not raw:
        return ""
    if hasattr(raw, 'strftime'):
        return raw.strftime("%m/%d/%Y")
    return str(raw)

def _carrier_row_to_dict(row) -> dict:
    """Map a Census-schema carriers row to the API response format."""
    d = dict(row)

    mc_number = _build_mc_number(d)
    dot_number = str(d.get("dot_number") or "")

    physical_address = _build_address(
        d.get("phy_street") or "",
        d.get("phy_city") or "",
        d.get("phy_state") or "",
        d.get("phy_zip") or "",
        d.get("phy_country") or "",
    )
    mailing_address = _build_address(
        d.get("carrier_mailing_street") or "",
        d.get("carrier_mailing_city") or "",
        d.get("carrier_mailing_state") or "",
        d.get("carrier_mailing_zip") or "",
        d.get("carrier_mailing_country") or "",
    )

    cargo_carried = _build_cargo_list(d)
    op_code = (d.get("carrier_operation") or "").strip()
    carrier_operation_list = [_CARRIER_OP_MAP.get(op_code, op_code)] if op_code else []

    classdef = d.get("classdef") or ""
    operation_classification = [c.strip() for c in classdef.split(";")] if classdef else []

    status_code = (d.get("status_code") or "").strip()
    status_label = _STATUS_CODE_MAP.get(status_code, status_code)

    docket_status_code = (d.get("docket1_status_code") or "").strip()
    _DOCKET_STATUS_MAP = {"A": "AUTHORIZED", "I": "NOT AUTHORIZED", "P": "PENDING"}
    docket_status = _DOCKET_STATUS_MAP.get(docket_status_code, "NOT AUTHORIZED")

    mcs150_date = _format_mcs150_date(d.get("mcs150_date"))
    mileage = d.get("mcs150_mileage")
    mileage_year = d.get("mcs150_mileage_year")
    mcs150_mileage = f"{mileage} ({mileage_year})" if mileage and mileage_year else str(mileage or "")

    territory = []
    if d.get("interstate_beyond_100_miles"):
        territory.append("Interstate (>100 mi)")
    if d.get("interstate_within_100_miles"):
        territory.append("Interstate (<100 mi)")
    if d.get("intrastate_beyond_100_miles"):
        territory.append("Intrastate (>100 mi)")
    if d.get("intrastate_within_100_miles"):
        territory.append("Intrastate (<100 mi)")

    add_date_raw = d.get("add_date")
    add_date_str = add_date_raw.strftime("%m/%d/%Y") if hasattr(add_date_raw, 'strftime') else str(add_date_raw or "")

    hm_val = d.get("hm_ind")
    hm_str = "Y" if hm_val is True else ("N" if hm_val is False else "")

    equipment = d.get("equipment")
    equipment_dict = equipment if isinstance(equipment, dict) else {}

    result = {
        "id": dot_number,
        "mc_number": mc_number,
        "dot_number": dot_number,
        "legal_name": d.get("legal_name") or "",
        "dba_name": d.get("dba_name") or "",
        "entity_type": "",
        "status": status_label,
        "status_code": status_code,
        "authority_status": docket_status,
        "email": d.get("email_address") or "",
        "phone": d.get("phone") or "",
        "fax": d.get("fax") or "",
        "power_units": d.get("power_units") or "",
        "drivers": d.get("total_drivers") or "",
        "physical_address": physical_address,
        "mailing_address": mailing_address,
        "phy_state": d.get("phy_state") or "",
        "mcs150_date": mcs150_date,
        "mcs150_mileage": mcs150_mileage,
        "operation_classification": operation_classification,
        "carrier_operation": carrier_operation_list,
        "cargo_carried": cargo_carried,
        "hm_ind": hm_str,
        "duns_number": d.get("dun_bradstreet_no") or "",
        "safety_rating": "",
        "safety_rating_date": "",
        "operating_territory": territory,
        "company_officer_1": d.get("company_officer_1") or "",
        "company_officer_2": d.get("company_officer_2") or "",
        "fleetsize": d.get("fleetsize") or "",
        "add_date": add_date_str,
        "truck_units": d.get("truck_units") or "",
        "bus_units": "",
        "equipment": equipment_dict,
        "basic_scores": None,
        "oos_rates": None,
        "insurance_policies": None,
        "inspections": None,
        "crashes": None,
    }

    result["insurance_history_filings"] = []
    return result

async def fetch_carriers(filters: dict) -> dict:
    """Fetch carriers from the Census-schema carriers table.

    Performance optimisations vs. the previous version
    ---------------------------------------------------
    1. All insurance-related filters are collected and merged into a
       **single EXISTS / NOT EXISTS** sub-query (or at most two – one
       positive, one negative).  Previously every insurance filter
       generated its own correlated sub-query which multiplied the
       work the planner had to do.
    2. The gigantic CASE expression for next-renewal-date is replaced
       by a simpler ``_next_renewal_sql`` helper that computes the
       same value with far fewer nested calls to ``TO_DATE`` /
       ``MAKE_DATE``.
    3. ``years_in_business`` filters now compare the raw ``add_date``
       string directly (``add_date <= to_char(…, 'YYYYMMDD')``)
       so PG can use a plain B-tree index on ``add_date`` instead of
       casting every row through ``TO_DATE``.
    4. ``carrier_operation`` and ``state`` use ``= ANY($N)`` instead
       of N separate OR-clauses, letting PG use the B-tree index in a
       single pass.
    """
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    # ------------------------------------------------------------------
    # Carrier-table filters (no insurance_history involvement)
    # ------------------------------------------------------------------

    if filters.get("mc_number"):
        mc_raw = filters["mc_number"].strip().upper()
        mc_num = mc_raw
        for pfx in ("MC", "MX", "FF"):
            if mc_raw.startswith(pfx):
                mc_num = mc_raw[len(pfx):].lstrip("-").strip()
                break
        try:
            mc_int = int(mc_num)
            conditions.append(f"c.mc_number = ${idx}")
            params.append(mc_int)
            idx += 1
        except ValueError:
            conditions.append(f"c.mc_number::text = ${idx}")
            params.append(mc_num)
            idx += 1

    if filters.get("dot_number"):
        dot_val = filters["dot_number"].strip()
        try:
            dot_int = int(dot_val)
            conditions.append(f"c.dot_number = ${idx}")
            params.append(dot_int)
            idx += 1
        except ValueError:
            conditions.append(f"c.dot_number::text = ${idx}")
            params.append(dot_val)
            idx += 1

    if filters.get("legal_name"):
        conditions.append(f"c.legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    # entity_type filter removed (carship column no longer exists)

    active = filters.get("active")
    if active == "true":
        conditions.append("c.docket1_status_code = 'A'")
    elif active == "false":
        conditions.append("(c.docket1_status_code IS NULL OR c.docket1_status_code != 'A')")

    # Years-in-business: add_date is now DATE type — direct comparison
    if filters.get("years_in_business_min"):
        conditions.append(
            f"c.add_date IS NOT NULL "
            f"AND c.add_date <= CURRENT_DATE - make_interval(years => ${idx})"
        )
        params.append(int(filters["years_in_business_min"]))
        idx += 1
    if filters.get("years_in_business_max"):
        conditions.append(
            f"c.add_date IS NOT NULL "
            f"AND c.add_date >= CURRENT_DATE - make_interval(years => ${idx})"
        )
        params.append(int(filters["years_in_business_max"]))
        idx += 1

    # State: use ANY() for index-friendly single-pass lookup
    if filters.get("state"):
        states = [s.strip().upper() for s in filters["state"].split("|")]
        conditions.append(f"c.phy_state = ANY(${idx})")
        params.append(states)
        idx += 1

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("c.email_address IS NOT NULL AND c.email_address != ''")
    elif has_email == "false":
        conditions.append("(c.email_address IS NULL OR c.email_address = '')")

    has_company_rep = filters.get("has_company_rep")
    if has_company_rep == "true":
        conditions.append("c.dba_name IS NOT NULL AND c.dba_name != ''")
    elif has_company_rep == "false":
        conditions.append("(c.dba_name IS NULL OR c.dba_name = '')")

    if filters.get("classification"):
        classifications = filters["classification"]
        if isinstance(classifications, str):
            classifications = classifications.split(",")
        or_clauses = []
        for cls in classifications:
            or_clauses.append(f"c.classdef ILIKE ${idx}")
            params.append(f"%{cls.strip()}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # Carrier operation: use ANY() for single-pass index scan
    if filters.get("carrier_operation"):
        ops = filters["carrier_operation"]
        if isinstance(ops, str):
            ops = ops.split(",")
        reverse_op = {v: k for k, v in _CARRIER_OP_MAP.items()}
        codes = [reverse_op.get(o.strip(), o.strip()) for o in ops]
        conditions.append(f"c.carrier_operation = ANY(${idx})")
        params.append(codes)
        idx += 1

    if filters.get("cargo"):
        cargo_filter = filters["cargo"]
        if isinstance(cargo_filter, str):
            cargo_filter = cargo_filter.split(",")
        or_clauses = []
        for c in cargo_filter:
            c_stripped = c.strip()
            if c_stripped:
                or_clauses.append(f"c.cargo ? ${idx}")
                params.append(c_stripped)
                idx += 1
        if or_clauses:
            conditions.append(f"({' OR '.join(or_clauses)})")

    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("c.hm_ind = TRUE")
    elif hazmat == "false":
        conditions.append("(c.hm_ind IS NULL OR c.hm_ind = FALSE)")

    if filters.get("power_units_min") is not None:
        conditions.append(f"c.power_units >= ${idx}")
        params.append(filters["power_units_min"])
        idx += 1
    if filters.get("power_units_max") is not None:
        conditions.append(f"c.power_units <= ${idx}")
        params.append(filters["power_units_max"])
        idx += 1
    if filters.get("drivers_min") is not None:
        conditions.append(f"c.total_drivers >= ${idx}")
        params.append(filters["drivers_min"])
        idx += 1
    if filters.get("drivers_max") is not None:
        conditions.append(f"c.total_drivers <= ${idx}")
        params.append(filters["drivers_max"])
        idx += 1

    # CTE parts: list of (cte_name, cte_sql)
    _cte_parts: list[tuple[str, str]] = []
    # JOIN clauses for positive filters (safety + insurance)
    _ins_joins: list[str] = []
    _cte_idx = 0

    # ------------------------------------------------------------------
    # Safety filters – inspections & crashes tables
    # ------------------------------------------------------------------
    # These filters use CTE-based pre-filtering against the inspections
    # and crashes tables, joining back to carriers via dot_number.

    # OOS violations (from inspections table: sum of oos_total per carrier)
    _oos_conds: list[str] = []
    if filters.get("oos_min"):
        _oos_conds.append(f"SUM(oos_total) >= ${idx}")
        params.append(int(filters["oos_min"]))
        idx += 1
    if filters.get("oos_max"):
        _oos_conds.append(f"SUM(oos_total) <= ${idx}")
        params.append(int(filters["oos_max"]))
        idx += 1
    if _oos_conds:
        _cte_parts.append((
            "_oos_filter",
            f"SELECT dot_number FROM inspections "
            f"GROUP BY dot_number HAVING {' AND '.join(_oos_conds)}",
        ))
        _ins_joins.append(
            "INNER JOIN _oos_filter ON _oos_filter.dot_number = c.dot_number"
        )

    # Inspections count filter (from inspections table)
    _insp_count_conds: list[str] = []
    if filters.get("inspections_min"):
        _insp_count_conds.append(f"COUNT(*) >= ${idx}")
        params.append(int(filters["inspections_min"]))
        idx += 1
    if filters.get("inspections_max"):
        _insp_count_conds.append(f"COUNT(*) <= ${idx}")
        params.append(int(filters["inspections_max"]))
        idx += 1
    if _insp_count_conds:
        _cte_parts.append((
            "_insp_count_filter",
            f"SELECT dot_number FROM inspections "
            f"GROUP BY dot_number HAVING {' AND '.join(_insp_count_conds)}",
        ))
        _ins_joins.append(
            "INNER JOIN _insp_count_filter ON _insp_count_filter.dot_number = c.dot_number"
        )

    # Crashes count filter (from crashes table)
    _crash_count_conds: list[str] = []
    if filters.get("crashes_min"):
        _crash_count_conds.append(f"COUNT(*) >= ${idx}")
        params.append(int(filters["crashes_min"]))
        idx += 1
    if filters.get("crashes_max"):
        _crash_count_conds.append(f"COUNT(*) <= ${idx}")
        params.append(int(filters["crashes_max"]))
        idx += 1
    if _crash_count_conds:
        _cte_parts.append((
            "_crash_count_filter",
            f"SELECT dot_number FROM crashes "
            f"GROUP BY dot_number HAVING {' AND '.join(_crash_count_conds)}",
        ))
        _ins_joins.append(
            "INNER JOIN _crash_count_filter ON _crash_count_filter.dot_number = c.dot_number::text"
        )

    # Injuries filter (from crashes table: sum of injuries per carrier)
    _injuries_conds: list[str] = []
    if filters.get("injuries_min"):
        _injuries_conds.append(f"SUM(injuries) >= ${idx}")
        params.append(int(filters["injuries_min"]))
        idx += 1
    if filters.get("injuries_max"):
        _injuries_conds.append(f"SUM(injuries) <= ${idx}")
        params.append(int(filters["injuries_max"]))
        idx += 1
    if _injuries_conds:
        _cte_parts.append((
            "_injuries_filter",
            f"SELECT dot_number FROM crashes "
            f"GROUP BY dot_number HAVING {' AND '.join(_injuries_conds)}",
        ))
        _ins_joins.append(
            "INNER JOIN _injuries_filter ON _injuries_filter.dot_number = c.dot_number::text"
        )

    # Fatalities filter (from crashes table: sum of fatalities per carrier)
    _fatalities_conds: list[str] = []
    if filters.get("fatalities_min"):
        _fatalities_conds.append(f"SUM(fatalities) >= ${idx}")
        params.append(int(filters["fatalities_min"]))
        idx += 1
    if filters.get("fatalities_max"):
        _fatalities_conds.append(f"SUM(fatalities) <= ${idx}")
        params.append(int(filters["fatalities_max"]))
        idx += 1
    if _fatalities_conds:
        _cte_parts.append((
            "_fatalities_filter",
            f"SELECT dot_number FROM crashes "
            f"GROUP BY dot_number HAVING {' AND '.join(_fatalities_conds)}",
        ))
        _ins_joins.append(
            "INNER JOIN _fatalities_filter ON _fatalities_filter.dot_number = c.dot_number::text"
        )

    # Towaway filter (from crashes table: count of tow_away=true per carrier)
    _toway_conds: list[str] = []
    if filters.get("toway_min"):
        _toway_conds.append(f"COUNT(*) FILTER (WHERE tow_away = true) >= ${idx}")
        params.append(int(filters["toway_min"]))
        idx += 1
    if filters.get("toway_max"):
        _toway_conds.append(f"COUNT(*) FILTER (WHERE tow_away = true) <= ${idx}")
        params.append(int(filters["toway_max"]))
        idx += 1
    if _toway_conds:
        _cte_parts.append((
            "_toway_filter",
            f"SELECT dot_number FROM crashes "
            f"GROUP BY dot_number HAVING {' AND '.join(_toway_conds)}",
        ))
        _ins_joins.append(
            "INNER JOIN _toway_filter ON _toway_filter.dot_number = c.dot_number::text"
        )

    # ------------------------------------------------------------------
    # Insurance-related filters – independent CTE-based pre-filtering
    # ------------------------------------------------------------------
    # Each independent insurance condition pre-selects qualifying
    # docket_numbers from insurance_history via a CTE, then carriers
    # are filtered with INNER JOINs using the expression index on
    # (docket1prefix || docket1).  This is dramatically faster than
    # correlated EXISTS on 4M+ carriers because:
    #   1. insurance_history is scanned once per filter (not per carrier)
    #   2. The expression index drives the carrier lookup
    #   3. Each filter is independent so different insurance_history rows
    #      can satisfy different filters (fixes the correctness bug where
    #      the old consolidated EXISTS required one row to match ALL
    #      conditions simultaneously).

    _INS_TYPE_PATTERN = {"BI&PD": "BIPD%", "CARGO": "CARGO", "BOND": "SURETY", "TRUST FUND": "TRUST FUND"}
    _IH_DOCKET_EXPR = "'MC' || c.mc_number::text"
    _ACTIVE_POLICY = "(cancl_effective_date IS NULL OR cancl_effective_date = '')"

    def _add_positive_ins_filter(where_body: str):
        """Register an insurance CTE and add an INNER JOIN for it."""
        nonlocal _cte_idx
        name = f"_ifd{_cte_idx}"
        _cte_parts.append((
            name,
            f"SELECT DISTINCT docket_number FROM insurance_history WHERE {where_body}",
        ))
        _ins_joins.append(
            f"INNER JOIN {name} ON {name}.docket_number = {_IH_DOCKET_EXPR}"
        )
        _cte_idx += 1

    # insurance_required – carrier must have at least one active policy
    # of any of the selected types (OR between types)
    if filters.get("insurance_required"):
        ins_types = filters["insurance_required"]
        if isinstance(ins_types, str):
            ins_types = ins_types.split(",")
        or_parts = []
        for itype in ins_types:
            pattern = _INS_TYPE_PATTERN.get(itype, itype)
            or_parts.append(f"(ins_type_desc LIKE ${idx} AND {_ACTIVE_POLICY})")
            params.append(pattern)
            idx += 1
        _add_positive_ins_filter(" OR ".join(or_parts))

    # BIPD / Cargo / Bond / Trust Fund on-file flags – each independent
    for filter_key, pattern_val, use_like in [
        ("bipd_on_file", "BIPD%", True),
        ("cargo_on_file", "CARGO", False),
        ("bond_on_file", "SURETY", False),
        ("trust_fund_on_file", "TRUST FUND", False),
    ]:
        val = filters.get(filter_key)
        if val is None:
            continue
        op = "LIKE" if use_like else "="
        if val == "1":
            _add_positive_ins_filter(
                f"ins_type_desc {op} ${idx} AND {_ACTIVE_POLICY}"
            )
            params.append(pattern_val)
            idx += 1
        elif val == "0":
            conditions.append(
                f"NOT EXISTS (SELECT 1 FROM insurance_history ih "
                f"WHERE ih.docket_number = {_IH_DOCKET_EXPR} "
                f"AND ih.ins_type_desc {op} ${idx} "
                f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
            )
            params.append(pattern_val)
            idx += 1

    # BIPD amount range – group min+max in one CTE so a single row
    # must satisfy both bounds
    _bipd_amount_conds: list[str] = []
    if filters.get("bipd_min"):
        raw_min = int(filters["bipd_min"])
        compare_min = raw_min // 1000 if raw_min >= 10000 else raw_min
        _bipd_amount_conds.append(
            f"NULLIF(REPLACE(max_cov_amount, ',', ''), '')::numeric >= ${idx}"
        )
        params.append(compare_min)
        idx += 1
    if filters.get("bipd_max"):
        raw_max = int(filters["bipd_max"])
        compare_max = raw_max // 1000 if raw_max >= 10000 else raw_max
        _bipd_amount_conds.append(
            f"NULLIF(REPLACE(max_cov_amount, ',', ''), '')::numeric <= ${idx}"
        )
        params.append(compare_max)
        idx += 1
    if _bipd_amount_conds:
        _add_positive_ins_filter(" AND ".join(_bipd_amount_conds))

    # Insurance effective-date range – group from+to so one row must
    # fall within the range
    _eff_date_conds: list[str] = []
    if filters.get("ins_effective_date_from"):
        parts = filters["ins_effective_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        _eff_date_conds.append(
            f"TO_DATE(effective_date, 'MM/DD/YYYY') >= TO_DATE(${idx}, 'MM/DD/YYYY')"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("ins_effective_date_to"):
        parts = filters["ins_effective_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        _eff_date_conds.append(
            f"TO_DATE(effective_date, 'MM/DD/YYYY') <= TO_DATE(${idx}, 'MM/DD/YYYY')"
        )
        params.append(date_to_db_fmt)
        idx += 1
    if _eff_date_conds:
        _add_positive_ins_filter(
            "effective_date IS NOT NULL AND effective_date LIKE '%/%/%' AND "
            + " AND ".join(_eff_date_conds)
        )

    # Insurance cancellation-date range – group from+to
    _cancl_date_conds: list[str] = []
    if filters.get("ins_cancellation_date_from"):
        parts = filters["ins_cancellation_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        _cancl_date_conds.append(
            f"TO_DATE(cancl_effective_date, 'MM/DD/YYYY') >= TO_DATE(${idx}, 'MM/DD/YYYY')"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("ins_cancellation_date_to"):
        parts = filters["ins_cancellation_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        _cancl_date_conds.append(
            f"TO_DATE(cancl_effective_date, 'MM/DD/YYYY') <= TO_DATE(${idx}, 'MM/DD/YYYY')"
        )
        params.append(date_to_db_fmt)
        idx += 1
    if _cancl_date_conds:
        _add_positive_ins_filter(
            "cancl_effective_date IS NOT NULL AND cancl_effective_date != '' "
            "AND cancl_effective_date LIKE '%/%/%' AND "
            + " AND ".join(_cancl_date_conds)
        )

    # Insurance Company filter
    _INSURANCE_COMPANY_PATTERNS: dict[str, list[str]] = {
        "GREAT WEST CASUALTY": ["GREAT WEST%"],
        "UNITED FINANCIAL CASUALTY": ["UNITED FINANCIAL%"],
        "GEICO MARINE": ["GEICO MARINE%"],
        "NORTHLAND INSURANCE": ["NORTHLAND%"],
        "ARTISAN & TRUCKERS": ["ARTISAN%", "TRUCKERS CASUALTY%"],
        "CANAL INSURANCE": ["CANAL INS%"],
        "PROGRESSIVE": ["PROGRESSIVE%"],
        "BERKSHIRE HATHAWAY": ["BERKSHIRE%"],
        "OLD REPUBLIC": ["OLD REPUBLIC%"],
        "SENTRY": ["SENTRY%"],
        "TRAVELERS": ["TRAVELERS%"],
    }
    if filters.get("insurance_company"):
        companies = filters["insurance_company"]
        if isinstance(companies, str):
            companies = companies.split(",")
        or_parts = []
        for company in companies:
            company_upper = company.strip().upper()
            patterns = _INSURANCE_COMPANY_PATTERNS.get(company_upper, [f"{company_upper}%"])
            for pattern in patterns:
                or_parts.append(f"UPPER(name_company) LIKE ${idx}")
                params.append(pattern)
                idx += 1
        _add_positive_ins_filter(
            f"({' OR '.join(or_parts)}) "
            f"AND (cancl_effective_date IS NULL OR cancl_effective_date = '' "
            f"OR TO_DATE(cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE)"
        )

    # Next-renewal-date helper (computes next anniversary of effective_date)
    _next_renewal_sql = (
        "CASE "
        "  WHEN MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "         EXTRACT(MONTH FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int,"
        "         LEAST(EXTRACT(DAY FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "                 EXTRACT(MONTH FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "       >= CURRENT_DATE "
        "  THEN MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "         EXTRACT(MONTH FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int,"
        "         LEAST(EXTRACT(DAY FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "                 EXTRACT(MONTH FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "  ELSE MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,"
        "         EXTRACT(MONTH FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int,"
        "         LEAST(EXTRACT(DAY FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,"
        "                 EXTRACT(MONTH FROM TO_DATE(effective_date, 'MM/DD/YYYY'))::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "END"
    )

    # Active-policy guard for renewal filters (no ih. prefix – used in CTE)
    _ACTIVE_POLICY_GUARD = (
        "effective_date IS NOT NULL AND effective_date LIKE '%/%/%' "
        "AND (cancl_effective_date IS NULL OR cancl_effective_date = '' "
        "OR TO_DATE(cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE)"
    )

    # Renewal Policy Monthly filter
    if filters.get("renewal_policy_months"):
        months = int(filters["renewal_policy_months"])
        _add_positive_ins_filter(
            f"{_ACTIVE_POLICY_GUARD} AND "
            f"({_next_renewal_sql}) BETWEEN CURRENT_DATE AND "
            f"(DATE_TRUNC('MONTH', CURRENT_DATE + MAKE_INTERVAL(months => ${idx})) + INTERVAL '1 MONTH - 1 DAY')::date"
        )
        params.append(months)
        idx += 1

    # Renewal Policy Date range filter – group from+to
    _renewal_date_conds: list[str] = []
    if filters.get("renewal_date_from"):
        parts = filters["renewal_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        _renewal_date_conds.append(
            f"({_next_renewal_sql}) >= TO_DATE(${idx}, 'MM/DD/YYYY')"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("renewal_date_to"):
        parts = filters["renewal_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        _renewal_date_conds.append(
            f"({_next_renewal_sql}) <= TO_DATE(${idx}, 'MM/DD/YYYY')"
        )
        params.append(date_to_db_fmt)
        idx += 1
    if _renewal_date_conds:
        _add_positive_ins_filter(
            f"{_ACTIVE_POLICY_GUARD} AND " + " AND ".join(_renewal_date_conds)
        )

    # ------------------------------------------------------------------
    # Default / WHERE / LIMIT / OFFSET
    # ------------------------------------------------------------------
    is_filtered = len(conditions) > 0 or len(_ins_joins) > 0
    if not is_filtered:
        conditions.append("c.status_code = 'A'")

    where = " AND ".join(conditions) if conditions else "TRUE"

    limit_val = min(int(filters.get("limit", 500)), 5000)
    offset_val = int(filters.get("offset", 0))

    _LIST_COLS = """c.dot_number, c.mc_number, c.dockets, c.legal_name, c.dba_name,
        c.phone, c.email_address, c.fax,
        c.power_units, c.total_drivers,
        c.phy_street, c.phy_city, c.phy_state, c.phy_zip, c.phy_country,
        c.carrier_mailing_street, c.carrier_mailing_city,
        c.carrier_mailing_state, c.carrier_mailing_zip, c.carrier_mailing_country,
        c.mcs150_date, c.mcs150_mileage, c.mcs150_mileage_year,
        c.classdef, c.carrier_operation, c.hm_ind,
        c.dun_bradstreet_no,
        c.status_code, c.docket1_status_code,
        c.company_officer_1, c.company_officer_2,
        c.fleetsize, c.add_date, c.truck_units,
        c.interstate_beyond_100_miles, c.interstate_within_100_miles,
        c.intrastate_beyond_100_miles, c.intrastate_within_100_miles,
        c.cargo, c.other_dockets, c.equipment"""

    # Build CTE prefix and FROM clause with JOINs
    cte_prefix = ""
    if _cte_parts:
        cte_prefix = "WITH " + ", ".join(
            f"{name} AS ({sql})" for name, sql in _cte_parts
        ) + " "

    from_clause = "carriers c"
    if _ins_joins:
        from_clause += " " + " ".join(_ins_joins)

    _order = "c.legal_name ASC"

    query = f"""{cte_prefix}
        SELECT {_LIST_COLS}
        FROM {from_clause}
        WHERE {where}
        ORDER BY {_order}
        LIMIT {limit_val} OFFSET {offset_val}
    """

    # ------------------------------------------------------------------
    # Count strategy – exact COUNT(*) for filtered, pg_class for default
    # ------------------------------------------------------------------
    fast_count_query = """
        SELECT reltuples::bigint AS cnt
        FROM pg_class WHERE relname = 'carriers'
    """

    # CTE-based filters can cause Postgres to pick a nested-loop plan.
    # Disabling nested loops forces a hash join which finishes in < 1 s.
    _needs_nestloop_off = bool(_cte_parts)

    async def _run_query(conn):
        """Run the main + count queries, optionally inside a txn."""
        if _needs_nestloop_off:
            await conn.execute("SET LOCAL enable_nestloop = off")
        return await conn.fetch(query, *params)

    async def _run_count(conn):
        count_q = f"""{cte_prefix}
            SELECT COUNT(*) as cnt
            FROM {from_clause}
            WHERE {where}
        """
        if _needs_nestloop_off:
            await conn.execute("SET LOCAL enable_nestloop = off")
        return await conn.fetchrow(count_q, *params)

    try:
        use_fast_count = not is_filtered
        if use_fast_count:
            if _needs_nestloop_off:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        rows = await _run_query(conn)
                count_row = await pool.fetchrow(fast_count_query)
            else:
                rows, count_row = await asyncio.gather(
                    pool.fetch(query, *params),
                    pool.fetchrow(fast_count_query),
                )
            filtered_count = count_row["cnt"] if count_row else 0
        else:
            if _needs_nestloop_off:
                async with pool.acquire() as c1, pool.acquire() as c2:
                    async with c1.transaction(), c2.transaction():
                        rows, count_row = await asyncio.gather(
                            _run_query(c1),
                            _run_count(c2),
                        )
            else:
                count_query = f"""{cte_prefix}
                    SELECT COUNT(*) as cnt
                    FROM {from_clause}
                    WHERE {where}
                """
                rows, count_row = await asyncio.gather(
                    pool.fetch(query, *params),
                    pool.fetchrow(count_query, *params),
                )
            filtered_count = count_row["cnt"] if count_row else 0

        # ── Parallel batch-fetch: insurance, inspections, crashes ────────
        carrier_dicts = [_carrier_row_to_dict(row) for row in rows]

        # Prepare keys for all three fetches
        docket_keys = []
        dot_numbers_int: list[int] = []
        for row in rows:
            d = dict(row)
            mc = d.get("mc_number")
            docket_keys.append(f"MC{mc}" if mc else "")
            dn = d.get("dot_number")
            if dn is not None:
                try:
                    dot_numbers_int.append(int(dn))
                except (TypeError, ValueError):
                    pass

        unique_docket_keys = list(set(k for k in docket_keys if k))
        unique_dots_int = list(set(dot_numbers_int))

        # Fire all three batch fetches in parallel
        async def _fetch_insurance():
            if not unique_docket_keys:
                return []
            return await pool.fetch(
                """SELECT docket_number, ins_type_desc, max_cov_amount,
                          underl_lim_amount, policy_no, effective_date,
                          ins_form_code, name_company, trans_date,
                          cancl_effective_date
                   FROM insurance_history
                   WHERE docket_number = ANY($1)
                   ORDER BY effective_date DESC""",
                unique_docket_keys,
            )

        async def _fetch_insp_batch():
            if not unique_dots_int:
                return []
            return await pool.fetch(
                """SELECT * FROM inspections
                   WHERE dot_number = ANY($1)
                   ORDER BY insp_date DESC NULLS LAST""",
                unique_dots_int,
            )

        async def _fetch_crash_batch():
            if not unique_dots_int:
                return []
            dot_strs = [str(d) for d in unique_dots_int]
            return await pool.fetch(
                """SELECT * FROM crashes
                   WHERE dot_number = ANY($1)
                   ORDER BY report_date DESC NULLS LAST""",
                dot_strs,
            )

        ih_rows, insp_rows_batch, crash_rows_batch = await asyncio.gather(
            _fetch_insurance(),
            _fetch_insp_batch(),
            _fetch_crash_batch(),
        )

        # Attach insurance history
        if ih_rows:
            ih_map: dict[str, list[dict]] = {}
            for ih_row in ih_rows:
                dk = ih_row["docket_number"]
                ih_map.setdefault(dk, []).append(dict(ih_row))
            for i, carrier in enumerate(carrier_dicts):
                dk = docket_keys[i]
                if dk and dk in ih_map:
                    carrier["insurance_history_filings"] = _format_insurance_history(ih_map[dk])

        # Attach inspections
        insp_map: dict[int, list[dict]] = {}
        for ir in insp_rows_batch:
            d = _inspection_row_to_dict(ir)
            dn = d.get("dot_number")
            if dn is None:
                continue
            try:
                insp_map.setdefault(int(dn), []).append(d)
            except (TypeError, ValueError):
                continue
        for carrier in carrier_dicts:
            cdn = carrier.get("dot_number")
            try:
                cdn_int = int(cdn) if cdn is not None else None
            except (TypeError, ValueError):
                cdn_int = None
            carrier["inspections"] = insp_map.get(cdn_int, []) if cdn_int is not None else []

        # Attach crashes (crashes.dot_number is TEXT)
        crash_map: dict[str, list[dict]] = {}
        for cr in crash_rows_batch:
            d = _crash_row_to_dict(cr)
            dn = str(d.get("dot_number", "")).strip()
            if dn:
                crash_map.setdefault(dn, []).append(d)
        for carrier in carrier_dicts:
            cdn = str(carrier.get("dot_number", "")).strip()
            carrier["crashes"] = crash_map.get(cdn, []) if cdn else []

        return {
            "data": carrier_dicts,
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return {"data": [], "filtered_count": 0}

async def delete_carrier(dot_number: str) -> bool:
    """Delete a carrier by DOT number."""
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM carriers WHERE dot_number = $1",
            int(dot_number.strip()),
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting carrier DOT {dot_number}: {e}")
        return False

async def get_carrier_count() -> int:
    """Fast estimated count using pg_class reltuples (instant on large tables)."""
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'"
    )
    return row["cnt"] if row else 0

async def get_carrier_dashboard_stats() -> dict:
    """Dashboard statistics for Census data (cached for 5 minutes)."""
    global _dashboard_cache, _dashboard_cache_ts
    now = _time.time()
    if _dashboard_cache and (now - _dashboard_cache_ts) < _DASHBOARD_CACHE_TTL:
        return _dashboard_cache

    pool = get_pool()
    try:
        row, insp_row, crash_row = await asyncio.gather(
            pool.fetchrow("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status_code = 'A') AS active,
                    COUNT(*) FILTER (WHERE email_address IS NOT NULL AND email_address != '') AS with_email,
                    COUNT(*) FILTER (WHERE hm_ind = TRUE) AS hazmat,
                    COUNT(*) FILTER (WHERE carrier_operation = 'A') AS interstate,
                    COUNT(*) FILTER (WHERE carrier_operation = 'B') AS intrastate_hm,
                    COUNT(*) FILTER (WHERE carrier_operation = 'C') AS intrastate_non_hm
                FROM carriers
            """),
            pool.fetchrow("SELECT COUNT(DISTINCT dot_number) AS cnt FROM inspections"),
            pool.fetchrow("SELECT COUNT(DISTINCT dot_number) AS cnt FROM crashes"),
        )
        if not row:
            return {}
        result = {
            "total": row["total"],
            "active": row["active"],
            "inactive": row["total"] - row["active"],
            "withEmail": row["with_email"],
            "hazmat": row["hazmat"],
            "interstate": row["interstate"],
            "intrastate_hm": row["intrastate_hm"],
            "intrastate_non_hm": row["intrastate_non_hm"],
            "with_inspections": insp_row["cnt"] if insp_row else 0,
            "with_crashes": crash_row["cnt"] if crash_row else 0,
        }
        _dashboard_cache = result
        _dashboard_cache_ts = now
        return result
    except Exception as e:
        print(f"[DB] Error fetching dashboard stats: {e}")
        return _dashboard_cache or {}

async def update_carrier_safety(dot_number: str, safety_data: dict) -> bool:
    """No-op: safety data is now in the separate safety table."""
    _ = safety_data
    return True

_MC_RANGE_COLS = """dot_number, mc_number, dockets, legal_name, dba_name,
    phone, email_address, fax,
    power_units, total_drivers,
    phy_street, phy_city, phy_state, phy_zip, phy_country,
    carrier_mailing_street, carrier_mailing_city,
    carrier_mailing_state, carrier_mailing_zip, carrier_mailing_country,
    mcs150_date, mcs150_mileage, mcs150_mileage_year,
    classdef, carrier_operation, hm_ind,
    dun_bradstreet_no,
    status_code, docket1_status_code,
    company_officer_1, company_officer_2,
    fleetsize, add_date, truck_units,
    interstate_beyond_100_miles, interstate_within_100_miles,
    intrastate_beyond_100_miles, intrastate_within_100_miles,
    cargo, other_dockets, equipment"""

async def get_carriers_by_mc_range(start_mc: str, end_mc: str) -> list[dict]:
    """Fetch carriers whose mc_number falls within start_mc..end_mc."""
    pool = get_pool()
    try:
        rows = await pool.fetch(
            f"""
            SELECT {_MC_RANGE_COLS} FROM carriers
            WHERE mc_number IS NOT NULL
              AND mc_number BETWEEN $1 AND $2
            ORDER BY mc_number
            LIMIT 1000
            """,
            int(start_mc),
            int(end_mc),
        )
        return [_carrier_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching MC range: {e}")
        return []

def _user_row_to_dict(row) -> dict:
    d = dict(row)
    d.pop("password_hash", None)
    for key in ("created_at", "updated_at", "blocked_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d

async def fetch_users() -> list[dict]:
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
    pool = get_pool()
    _ALLOWED_COLUMNS = {
        "name", "role", "plan", "daily_limit",
        "records_extracted_today", "last_active",
        "ip_address", "is_online", "is_blocked",
    }
    columns = {k: v for k, v in user_data.items() if k in _ALLOWED_COLUMNS}
    if not columns:
        return False
    set_clauses = []
    values = []
    for idx, (col, val) in enumerate(columns.items(), start=1):
        set_clauses.append(f"{col} = ${idx}")
        values.append(val)
    values.append(user_id)
    query = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ${len(values)}"
    try:
        result = await pool.execute(query, *values)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating user {user_id}: {e}")
        return False

async def delete_user(user_id: str) -> bool:
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

def _to_jsonb(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value)

_NV_COLUMNS = [
    "dot_number", "prefix", "docket_number", "status_code", "carship",
    "carrier_operation", "name", "name_dba", "add_date", "chgn_date",
    "common_stat", "contract_stat", "broker_stat",
    "common_app_pend", "contract_app_pend", "broker_app_pend",
    "common_rev_pend", "contract_rev_pend", "broker_rev_pend",
    "property_chk", "passenger_chk", "hhg_chk", "private_auth_chk", "enterprise_chk",
    "operating_status", "operating_status_indicator",
    "phy_str", "phy_city", "phy_st", "phy_zip", "phy_country", "phy_cnty",
    "mai_str", "mai_city", "mai_st", "mai_zip", "mai_country", "mai_cnty",
    "phy_undeliv", "mai_undeliv",
    "phy_phone", "phy_fax", "mai_phone", "mai_fax", "cell_phone", "email_address",
    "company_officer_1", "company_officer_2",
    "genfreight", "household", "metalsheet", "motorveh", "drivetow", "logpole",
    "bldgmat", "mobilehome", "machlrg", "produce", "liqgas", "intermodal",
    "passengers", "oilfield", "livestock", "grainfeed", "coalcoke", "meat",
    "garbage", "usmail", "chem", "drybulk", "coldfood", "beverages",
    "paperprod", "utility", "farmsupp", "construct", "waterwell",
    "cargoothr", "cargoothr_desc",
    "hm_ind", "bipd_req", "cargo_req", "bond_req", "bipd_file", "cargo_file", "bond_file",
    "owntruck", "owntract", "owntrail", "owncoach",
    "ownschool_1_8", "ownschool_9_15", "ownschool_16", "ownbus_16",
    "ownvan_1_8", "ownvan_9_15", "ownlimo_1_8", "ownlimo_9_15", "ownlimo_16",
    "trmtruck", "trmtract", "trmtrail", "trmcoach",
    "trmschool_1_8", "trmschool_9_15", "trmschool_16", "trmbus_16",
    "trmvan_1_8", "trmvan_9_15", "trmlimo_1_8", "trmlimo_9_15", "trmlimo_16",
    "trptruck", "trptract", "trptrail", "trpcoach",
    "trpschool_1_8", "trpschool_9_15", "trpschool_16", "trpbus_16",
    "trpvan_1_8", "trpvan_9_15", "trplimo_1_8", "trplimo_9_15", "trplimo_16",
    "total_trucks", "total_buses", "total_pwr", "fleetsize",
    "inter_within_100", "inter_beyond_100", "total_inter_drivers",
    "intra_within_100", "intra_beyond_100", "total_intra_drivers",
    "total_drivers", "avg_tld", "total_cdl",
    "review_type", "review_id", "review_date", "recordable_crash_rate",
    "mcs150_mileage", "mcs151_mileage", "mcs150_mileage_year", "mcs150_date",
    "safety_rating", "safety_rating_date",
    "arber", "smartway", "tia", "tia_phone", "tia_contact_name",
    "tia_tool_free", "tia_fax", "tia_email", "tia_website",
    "phy_ups_store", "mai_ups_store", "phy_mail_box", "mai_mail_box",
]

def _new_venture_row_to_dict(row) -> dict:
    d = dict(row)
    if "raw_data" in d:
        d["raw_data"] = _parse_jsonb(d["raw_data"])
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d

async def save_new_venture_entries(entries: list[dict], scrape_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    cols = _NV_COLUMNS + ["raw_data", "scrape_date"]
    col_list = ", ".join(cols)
    placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))

    update_cols = [c for c in cols if c not in ("dot_number", "add_date")]
    on_conflict_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    on_conflict_set += ", updated_at = NOW()"

    insert_sql = f"""
        INSERT INTO new_ventures ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (dot_number, add_date) DO UPDATE SET {on_conflict_set}
    """

    saved = 0
    skipped = 0
    batch_size = 500

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(entries), batch_size):
                    batch = entries[i:i + batch_size]
                    args = []
                    for entry in batch:
                        row_args = []
                        for col in _NV_COLUMNS:
                            val = entry.get(col)
                            row_args.append(val.strip() if isinstance(val, str) else val)
                        row_args.append(_to_jsonb(entry.get("raw_data")))
                        row_args.append(scrape_date)
                        args.append(tuple(row_args))
                    await conn.executemany(insert_sql, args)
                    saved += len(batch)
    except Exception as e:
        print(f"[DB] Error batch-saving new venture entries: {e}")
        skipped = len(entries) - saved

    return {"success": True, "saved": saved, "skipped": skipped}

async def fetch_new_ventures(filters: dict) -> list[dict]:
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("docket_number"):
        conditions.append(f"docket_number ILIKE ${idx}")
        params.append(f"%{filters['docket_number']}%")
        idx += 1

    if filters.get("dot_number"):
        conditions.append(f"dot_number ILIKE ${idx}")
        params.append(f"%{filters['dot_number']}%")
        idx += 1

    if filters.get("company_name"):
        conditions.append(f"(name ILIKE ${idx} OR name_dba ILIKE ${idx})")
        params.append(f"%{filters['company_name']}%")
        idx += 1

    if filters.get("date_from"):
        conditions.append(f"add_date >= ${idx}")
        params.append(filters["date_from"])
        idx += 1
    if filters.get("date_to"):
        conditions.append(f"add_date <= ${idx}")
        params.append(filters["date_to"])
        idx += 1

    active = filters.get("active")
    if active == "active":
        conditions.append(f"((operating_status ILIKE ${idx} AND operating_status NOT ILIKE ${idx + 1}) OR operating_status ILIKE ${idx + 2})")
        params.append("%AUTHORIZED%")
        params.append("%NOT AUTHORIZED%")
        params.append("ACTIVE")
        idx += 3
    elif active == "inactive":
        conditions.append(f"(operating_status ILIKE ${idx} OR operating_status IS NULL OR operating_status = '')")
        params.append("%NOT AUTHORIZED%")
        idx += 1
    elif active == "authorization_pending":
        conditions.append(f"operating_status ILIKE ${idx}")
        params.append("%PENDING%")
        idx += 1
    elif active == "not_authorized":
        conditions.append(f"operating_status ILIKE ${idx}")
        params.append("%NOT AUTHORIZED%")
        idx += 1
    elif active == "true":
        conditions.append(f"((operating_status ILIKE ${idx} AND operating_status NOT ILIKE ${idx + 1}) OR operating_status ILIKE ${idx + 2})")
        params.append("%AUTHORIZED%")
        params.append("%NOT AUTHORIZED%")
        params.append("ACTIVE")
        idx += 3
    elif active == "false":
        conditions.append(f"operating_status NOT ILIKE ${idx}")
        params.append("%AUTHORIZED%")
        idx += 1

    if filters.get("state"):
        states = [s.strip().upper() for s in filters["state"].split("|") if s.strip()]
        if len(states) == 1:
            conditions.append(f"phy_st = ${idx}")
            params.append(states[0])
            idx += 1
        elif states:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(states)))
            conditions.append(f"phy_st IN ({placeholders})")
            for s in states:
                params.append(s)
                idx += 1

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email_address IS NOT NULL AND email_address != ''")
    elif has_email == "false":
        conditions.append("(email_address IS NULL OR email_address = '')")

    if filters.get("carrier_operation"):
        conditions.append(f"carrier_operation ILIKE ${idx}")
        params.append(f"%{filters['carrier_operation']}%")
        idx += 1

    if filters.get("hazmat"):
        if filters["hazmat"] == "true":
            conditions.append("hm_ind = 'Y'")
        elif filters["hazmat"] == "false":
            conditions.append("(hm_ind IS NULL OR hm_ind != 'Y')")

    if filters.get("power_units_min") is not None:
        conditions.append(f"total_pwr >= ${idx}")
        params.append(filters["power_units_min"])
        idx += 1
    if filters.get("power_units_max") is not None:
        conditions.append(f"total_pwr <= ${idx}")
        params.append(filters["power_units_max"])
        idx += 1

    if filters.get("drivers_min") is not None:
        conditions.append(f"total_drivers >= ${idx}")
        params.append(filters["drivers_min"])
        idx += 1
    if filters.get("drivers_max") is not None:
        conditions.append(f"total_drivers <= ${idx}")
        params.append(filters["drivers_max"])
        idx += 1

    if filters.get("bipd_on_file"):
        if filters["bipd_on_file"] == "true":
            conditions.append("bipd_file = 'Y'")
        elif filters["bipd_on_file"] == "false":
            conditions.append("(bipd_file IS NULL OR bipd_file != 'Y')")
    if filters.get("cargo_on_file"):
        if filters["cargo_on_file"] == "true":
            conditions.append("cargo_file = 'Y'")
        elif filters["cargo_on_file"] == "false":
            conditions.append("(cargo_file IS NULL OR cargo_file != 'Y')")
    if filters.get("bond_on_file"):
        if filters["bond_on_file"] == "true":
            conditions.append("bond_file = 'Y'")
        elif filters["bond_on_file"] == "false":
            conditions.append("(bond_file IS NULL OR bond_file != 'Y')")

    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    if is_filtered:
        limit_val = int(filters.get("limit", 10000))
    else:
        limit_val = int(filters.get("limit", 200))

    offset_val = int(filters.get("offset", 0))

    query = f"""
        SELECT * FROM new_ventures
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT {limit_val} OFFSET {offset_val}
    """

    count_query = f"""
        SELECT COUNT(*) as cnt FROM new_ventures
        WHERE {where}
    """

    dates_query = "SELECT DISTINCT add_date FROM new_ventures WHERE add_date IS NOT NULL ORDER BY add_date DESC"

    total_query = "SELECT COUNT(*) as cnt FROM new_ventures"

    try:
        rows = await pool.fetch(query, *params)
        count_row = await pool.fetchrow(count_query, *params)
        filtered_count = count_row["cnt"] if count_row else 0
        date_rows = await pool.fetch(dates_query)
        available_dates = [r["add_date"] for r in date_rows]
        total_row = await pool.fetchrow(total_query)
        total_count = total_row["cnt"] if total_row else 0
        return {
            "data": [_new_venture_row_to_dict(row) for row in rows],
            "filtered_count": filtered_count,
            "total_count": total_count,
            "available_dates": available_dates,
        }
    except Exception as e:
        print(f"[DB] Error fetching new ventures: {e}")
        return {"data": [], "filtered_count": 0, "total_count": 0, "available_dates": []}

async def get_new_venture_count() -> int:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM new_ventures")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting new venture count: {e}")
        return 0

async def get_new_venture_scraped_dates() -> list[str]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            "SELECT DISTINCT add_date FROM new_ventures WHERE add_date IS NOT NULL ORDER BY add_date DESC"
        )
        return [row["add_date"] for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching new venture dates: {e}")
        return []

async def fetch_new_venture_by_id(record_id: str) -> dict | None:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT * FROM new_ventures WHERE id = $1", record_id)
        if row:
            return _new_venture_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error fetching new venture {record_id}: {e}")
        return None

async def delete_new_venture(record_id: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM new_ventures WHERE id = $1", record_id
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting new venture {record_id}: {e}")
        return False

async def fetch_insurance_history(docket_number: str) -> list[dict]:
    """Fetch insurance history by docket_number (e.g. 'MC123456').

    The frontend passes the raw docket_number value which is already
    in the format stored in insurance_history (docket1prefix || docket1).
    """
    pool = get_pool()
    try:
        rows = await pool.fetch(
            """
            SELECT docket_number, dot_number, ins_form_code, ins_type_desc,
                   name_company, policy_no, trans_date, underl_lim_amount,
                   max_cov_amount, effective_date, cancl_effective_date
            FROM insurance_history
            WHERE docket_number = $1
            ORDER BY effective_date DESC
            """,
            docket_number,
        )
        results = []
        for row in rows:
            raw_amount = (row["max_cov_amount"] or "").strip()
            try:
                amount_int = int(raw_amount) * 1000
                coverage = f"${amount_int:,}"
            except (ValueError, TypeError):
                coverage = raw_amount or "N/A"
            cancl = (row["cancl_effective_date"] or "").strip()
            results.append({
                "type": (row["ins_type_desc"] or "").strip(),
                "coverageAmount": coverage,
                "policyNumber": (row["policy_no"] or "").strip(),
                "effectiveDate": (row["effective_date"] or "").strip(),
                "carrier": (row["name_company"] or "").strip(),
                "formCode": (row["ins_form_code"] or "").strip(),
                "transDate": (row["trans_date"] or "").strip(),
                "underlLimAmount": (row["underl_lim_amount"] or "").strip(),
                "canclEffectiveDate": cancl,
                "status": "Cancelled" if cancl else "Active",
            })
        return results
    except Exception as e:
        print(f"[DB] Error fetching insurance history for {docket_number}: {e}")
        return []


# ── Inspections table functions ──────────────────────────────────────────────

def _inspection_row_to_dict(row) -> dict:
    """Convert a database row from the inspections table to a dictionary.

    Maps raw DB column names to the field names the frontend expects:
      reportNumber, date, location, oosViolations, driverViolations,
      vehicleViolations, hazmatViolations, violationList, etc.
    """
    d = dict(row)

    # Total violations = sum of all violation categories
    basic_viol = int(d.get("basic_viol") or 0)
    unsafe_viol = int(d.get("unsafe_viol") or 0)
    fatigued_viol = int(d.get("fatigued_viol") or 0)
    dr_fitness_viol = int(d.get("dr_fitness_viol") or 0)
    subt_alcohol_viol = int(d.get("subt_alcohol_viol") or 0)
    vh_maint_viol = int(d.get("vh_maint_viol") or 0)
    hm_viol_raw = d.get("hm_viol")
    if hm_viol_raw is not None and isinstance(hm_viol_raw, float) and math.isnan(hm_viol_raw):
        hm_viol_raw = None
    hm_viol = int(hm_viol_raw) if hm_viol_raw is not None else 0

    total_violations = basic_viol + unsafe_viol + fatigued_viol + dr_fitness_viol + subt_alcohol_viol + vh_maint_viol + hm_viol

    # Build a violationList array that the frontend can iterate over.
    # Each entry has label, description, and weight.
    violation_list: list[dict] = []
    _VIOL_CATEGORIES = [
        ("unsafe_viol", unsafe_viol, "Unsafe Driving", "Unsafe driving violations"),
        ("fatigued_viol", fatigued_viol, "HOS / Fatigued Driving", "Hours of service / fatigued driving violations"),
        ("dr_fitness_viol", dr_fitness_viol, "Driver Fitness", "Driver fitness violations"),
        ("subt_alcohol_viol", subt_alcohol_viol, "Controlled Substances / Alcohol", "Controlled substances / alcohol violations"),
        ("vh_maint_viol", vh_maint_viol, "Vehicle Maintenance", "Vehicle maintenance violations"),
        ("hm_viol", hm_viol, "Hazardous Materials", "Hazardous materials violations"),
    ]
    for _key, count, label, description in _VIOL_CATEGORIES:
        if count > 0:
            for _ in range(count):
                violation_list.append({
                    "label": label,
                    "description": description,
                    "weight": 1,
                })

    # Location: use report_state + county_code_state
    report_state = d.get("report_state") or ""
    county = d.get("county_code_state") or ""
    location = f"{county}, {report_state}" if county and county != report_state else report_state

    # OOS violations
    oos_total = int(d.get("oos_total") or 0)
    driver_oos = int(d.get("driver_oos_total") or 0)
    vehicle_oos = int(d.get("vehicle_oos_total") or 0)
    hazmat_oos = int(d.get("hazmat_oos_total") or 0)

    result = {
        # Fields the frontend expects
        "reportNumber": d.get("report_number") or "",
        "date": d.get("insp_date") or "",
        "location": location,
        "oosViolations": oos_total,
        "driverViolations": driver_oos,
        "vehicleViolations": vehicle_oos,
        "hazmatViolations": hazmat_oos,
        "violationList": violation_list,
        "totalViolations": total_violations,

        # Keep raw fields for the standalone inspections API
        "unique_id": d.get("unique_id"),
        "report_number": d.get("report_number"),
        "report_state": report_state,
        "dot_number": d.get("dot_number"),
        "insp_date": d.get("insp_date"),
        "insp_level_id": d.get("insp_level_id"),
        "county_code_state": county,
        "time_weight": d.get("time_weight"),
        "driver_oos_total": driver_oos,
        "vehicle_oos_total": vehicle_oos,
        "total_hazmat_sent": d.get("total_hazmat_sent"),
        "oos_total": oos_total,
        "hazmat_oos_total": hazmat_oos,
        "hazmat_placard_req": d.get("hazmat_placard_req"),
        "unit_type_desc": d.get("unit_type_desc"),
        "unit_make": d.get("unit_make"),
        "unit_license": d.get("unit_license"),
        "unit_license_state": d.get("unit_license_state"),
        "vin": d.get("vin"),
        "unit_decal_number": d.get("unit_decal_number"),
        "unit_type_desc2": d.get("unit_type_desc2"),
        "unit_make2": d.get("unit_make2"),
        "unit_license2": d.get("unit_license2"),
        "unit_license_state2": d.get("unit_license_state2"),
        "vin2": d.get("vin2"),
        "unit_decal_number2": d.get("unit_decal_number2"),
        "unsafe_insp": d.get("unsafe_insp"),
        "fatigued_insp": d.get("fatigued_insp"),
        "dr_fitness_insp": d.get("dr_fitness_insp"),
        "subt_alcohol_insp": d.get("subt_alcohol_insp"),
        "vh_maint_insp": d.get("vh_maint_insp"),
        "hm_insp": d.get("hm_insp"),
        "basic_viol": basic_viol,
        "unsafe_viol": unsafe_viol,
        "fatigued_viol": fatigued_viol,
        "dr_fitness_viol": dr_fitness_viol,
        "subt_alcohol_viol": subt_alcohol_viol,
        "vh_maint_viol": vh_maint_viol,
        "hm_viol": hm_viol_raw,
    }
    return result


def _crash_row_to_dict(row) -> dict:
    """Convert a database row from the crashes table to a JSON-serializable dict."""
    d = dict(row)
    report_date = d.get("report_date")
    if report_date is not None:
        d["report_date"] = str(report_date)
    return d


async def fetch_inspections(filters: dict) -> dict:
    """Fetch inspections from the inspections table with optional filters.
    
    Supported filters:
    - dot_number: Filter by DOT number
    - report_number: Filter by report number
    - report_state: Filter by report state
    - insp_date_from: Filter by inspection date (from)
    - insp_date_to: Filter by inspection date (to)
    - unsafe_insp: Filter by unsafe inspection (boolean)
    - fatigued_insp: Filter by fatigued inspection (boolean)
    - dr_fitness_insp: Filter by driver fitness inspection (boolean)
    - subt_alcohol_insp: Filter by substance/alcohol inspection (boolean)
    - vh_maint_insp: Filter by vehicle maintenance inspection (boolean)
    - hm_insp: Filter by hazmat inspection (boolean)
    - oos_min/max: Filter by out-of-service violations
    - driver_oos_min/max: Filter by driver OOS violations
    - vehicle_oos_min/max: Filter by vehicle OOS violations
    - hazmat_oos_min/max: Filter by hazmat OOS violations
    - basic_viol_min/max: Filter by basic violations
    - unsafe_viol_min/max: Filter by unsafe violations
    - fatigued_viol_min/max: Filter by fatigued violations
    - dr_fitness_viol_min/max: Filter by driver fitness violations
    - subt_alcohol_viol_min/max: Filter by substance/alcohol violations
    - vh_maint_viol_min/max: Filter by vehicle maintenance violations
    - hm_viol_min/max: Filter by hazmat violations
    - limit: Number of results to return (default 500, max 5000)
    - offset: Offset for pagination (default 0)
    """
    pool = get_pool()
    
    conditions: list[str] = []
    params: list = []
    idx = 1
    
    # Basic filters
    if filters.get("dot_number"):
        dot_val = filters["dot_number"].strip()
        conditions.append(f"i.dot_number = ${idx}::bigint")
        params.append(int(dot_val))
        idx += 1
    
    if filters.get("report_number"):
        conditions.append(f"i.report_number ILIKE ${idx}")
        params.append(f"%{filters['report_number']}%")
        idx += 1
    
    if filters.get("report_state"):
        conditions.append(f"i.report_state = ${idx}")
        params.append(filters["report_state"].upper())
        idx += 1
    
    # Date range filters
    if filters.get("insp_date_from"):
        conditions.append(f"i.insp_date >= ${idx}")
        params.append(filters["insp_date_from"])
        idx += 1
    
    if filters.get("insp_date_to"):
        conditions.append(f"i.insp_date <= ${idx}")
        params.append(filters["insp_date_to"])
        idx += 1
    
    # Inspection type filters (boolean flags)
    if filters.get("unsafe_insp") == "true":
        conditions.append("i.unsafe_insp = true")
    elif filters.get("unsafe_insp") == "false":
        conditions.append("i.unsafe_insp = false")
    
    if filters.get("fatigued_insp") == "true":
        conditions.append("i.fatigued_insp = true")
    elif filters.get("fatigued_insp") == "false":
        conditions.append("i.fatigued_insp = false")
    
    if filters.get("dr_fitness_insp") == "true":
        conditions.append("i.dr_fitness_insp = true")
    elif filters.get("dr_fitness_insp") == "false":
        conditions.append("i.dr_fitness_insp = false")
    
    if filters.get("subt_alcohol_insp") == "true":
        conditions.append("i.subt_alcohol_insp = true")
    elif filters.get("subt_alcohol_insp") == "false":
        conditions.append("i.subt_alcohol_insp = false")
    
    if filters.get("vh_maint_insp") == "true":
        conditions.append("i.vh_maint_insp = true")
    elif filters.get("vh_maint_insp") == "false":
        conditions.append("i.vh_maint_insp = false")
    
    if filters.get("hm_insp") == "true":
        conditions.append("i.hm_insp = true")
    elif filters.get("hm_insp") == "false":
        conditions.append("i.hm_insp = false")
    
    # OOS violations filters
    if filters.get("oos_min") is not None:
        conditions.append(f"i.oos_total >= ${idx}")
        params.append(int(filters["oos_min"]))
        idx += 1
    
    if filters.get("oos_max") is not None:
        conditions.append(f"i.oos_total <= ${idx}")
        params.append(int(filters["oos_max"]))
        idx += 1
    
    if filters.get("driver_oos_min") is not None:
        conditions.append(f"i.driver_oos_total >= ${idx}")
        params.append(int(filters["driver_oos_min"]))
        idx += 1
    
    if filters.get("driver_oos_max") is not None:
        conditions.append(f"i.driver_oos_total <= ${idx}")
        params.append(int(filters["driver_oos_max"]))
        idx += 1
    
    if filters.get("vehicle_oos_min") is not None:
        conditions.append(f"i.vehicle_oos_total >= ${idx}")
        params.append(int(filters["vehicle_oos_min"]))
        idx += 1
    
    if filters.get("vehicle_oos_max") is not None:
        conditions.append(f"i.vehicle_oos_total <= ${idx}")
        params.append(int(filters["vehicle_oos_max"]))
        idx += 1
    
    if filters.get("hazmat_oos_min") is not None:
        conditions.append(f"i.hazmat_oos_total >= ${idx}")
        params.append(int(filters["hazmat_oos_min"]))
        idx += 1
    
    if filters.get("hazmat_oos_max") is not None:
        conditions.append(f"i.hazmat_oos_total <= ${idx}")
        params.append(int(filters["hazmat_oos_max"]))
        idx += 1
    
    # Violations filters
    if filters.get("basic_viol_min") is not None:
        conditions.append(f"i.basic_viol >= ${idx}")
        params.append(int(filters["basic_viol_min"]))
        idx += 1
    
    if filters.get("basic_viol_max") is not None:
        conditions.append(f"i.basic_viol <= ${idx}")
        params.append(int(filters["basic_viol_max"]))
        idx += 1
    
    if filters.get("unsafe_viol_min") is not None:
        conditions.append(f"i.unsafe_viol >= ${idx}")
        params.append(int(filters["unsafe_viol_min"]))
        idx += 1
    
    if filters.get("unsafe_viol_max") is not None:
        conditions.append(f"i.unsafe_viol <= ${idx}")
        params.append(int(filters["unsafe_viol_max"]))
        idx += 1
    
    if filters.get("fatigued_viol_min") is not None:
        conditions.append(f"i.fatigued_viol >= ${idx}")
        params.append(int(filters["fatigued_viol_min"]))
        idx += 1
    
    if filters.get("fatigued_viol_max") is not None:
        conditions.append(f"i.fatigued_viol <= ${idx}")
        params.append(int(filters["fatigued_viol_max"]))
        idx += 1
    
    if filters.get("dr_fitness_viol_min") is not None:
        conditions.append(f"i.dr_fitness_viol >= ${idx}")
        params.append(int(filters["dr_fitness_viol_min"]))
        idx += 1
    
    if filters.get("dr_fitness_viol_max") is not None:
        conditions.append(f"i.dr_fitness_viol <= ${idx}")
        params.append(int(filters["dr_fitness_viol_max"]))
        idx += 1
    
    if filters.get("subt_alcohol_viol_min") is not None:
        conditions.append(f"i.subt_alcohol_viol >= ${idx}")
        params.append(int(filters["subt_alcohol_viol_min"]))
        idx += 1
    
    if filters.get("subt_alcohol_viol_max") is not None:
        conditions.append(f"i.subt_alcohol_viol <= ${idx}")
        params.append(int(filters["subt_alcohol_viol_max"]))
        idx += 1
    
    if filters.get("vh_maint_viol_min") is not None:
        conditions.append(f"i.vh_maint_viol >= ${idx}")
        params.append(int(filters["vh_maint_viol_min"]))
        idx += 1
    
    if filters.get("vh_maint_viol_max") is not None:
        conditions.append(f"i.vh_maint_viol <= ${idx}")
        params.append(int(filters["vh_maint_viol_max"]))
        idx += 1
    
    if filters.get("hm_viol_min") is not None:
        conditions.append(f"i.hm_viol >= ${idx}")
        params.append(float(filters["hm_viol_min"]))
        idx += 1
    
    if filters.get("hm_viol_max") is not None:
        conditions.append(f"i.hm_viol <= ${idx}")
        params.append(float(filters["hm_viol_max"]))
        idx += 1
    
    # Build WHERE clause
    where = " AND ".join(conditions) if conditions else "TRUE"
    
    limit_val = min(int(filters.get("limit", 500)), 5000)
    offset_val = int(filters.get("offset", 0))
    
    # Select all columns from inspections table
    _LIST_COLS = """i.unique_id, i.report_number, i.report_state, i.dot_number,
        i.insp_date, i.insp_level_id, i.county_code_state, i.time_weight,
        i.driver_oos_total, i.vehicle_oos_total, i.total_hazmat_sent, i.oos_total,
        i.hazmat_oos_total, i.hazmat_placard_req,
        i.unit_type_desc, i.unit_make, i.unit_license, i.unit_license_state,
        i.vin, i.unit_decal_number,
        i.unit_type_desc2, i.unit_make2, i.unit_license2, i.unit_license_state2,
        i.vin2, i.unit_decal_number2,
        i.unsafe_insp, i.fatigued_insp, i.dr_fitness_insp, i.subt_alcohol_insp,
        i.vh_maint_insp, i.hm_insp,
        i.basic_viol, i.unsafe_viol, i.fatigued_viol, i.dr_fitness_viol,
        i.subt_alcohol_viol, i.vh_maint_viol, i.hm_viol"""
    
    query = f"""
        SELECT {_LIST_COLS}
        FROM inspections i
        WHERE {where}
        ORDER BY i.insp_date DESC, i.unique_id DESC
        LIMIT {limit_val} OFFSET {offset_val}
    """
    
    count_query = f"""
        SELECT COUNT(*) as cnt
        FROM inspections i
        WHERE {where}
    """
    
    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(query, *params),
            pool.fetchrow(count_query, *params),
        )
        filtered_count = count_row["cnt"] if count_row else 0
        
        inspection_dicts = [_inspection_row_to_dict(row) for row in rows]
        
        return {
            "data": inspection_dicts,
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching inspections: {e}")
        return {"data": [], "filtered_count": 0}

async def get_inspections_count() -> int:
    """Get total count of inspections."""
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM inspections")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting inspections count: {e}")
        return 0

async def get_inspections_dashboard_stats() -> dict:
    """Get dashboard statistics for inspections."""
    pool = get_pool()
    try:
        row = await pool.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE unsafe_insp = true) AS unsafe_inspections,
                COUNT(*) FILTER (WHERE fatigued_insp = true) AS fatigued_inspections,
                COUNT(*) FILTER (WHERE dr_fitness_insp = true) AS dr_fitness_inspections,
                COUNT(*) FILTER (WHERE subt_alcohol_insp = true) AS substance_alcohol_inspections,
                COUNT(*) FILTER (WHERE vh_maint_insp = true) AS vehicle_maintenance_inspections,
                COUNT(*) FILTER (WHERE hm_insp = true) AS hazmat_inspections,
                COUNT(*) FILTER (WHERE oos_total > 0) AS with_oos_violations,
                AVG(oos_total) AS avg_oos_violations,
                AVG(basic_viol) AS avg_basic_violations,
                SUM(oos_total) AS total_oos_violations
            FROM inspections
        """)
        if not row:
            return {}
        result = {
            "total": row["total"],
            "unsafeInspections": row["unsafe_inspections"],
            "fatiguedInspections": row["fatigued_inspections"],
            "drFitnessInspections": row["dr_fitness_inspections"],
            "substanceAlcoholInspections": row["substance_alcohol_inspections"],
            "vehicleMaintenanceInspections": row["vehicle_maintenance_inspections"],
            "hazmatInspections": row["hazmat_inspections"],
            "withOosViolations": row["with_oos_violations"],
            "avgOosViolations": float(row["avg_oos_violations"]) if row["avg_oos_violations"] else 0,
            "avgBasicViolations": float(row["avg_basic_violations"]) if row["avg_basic_violations"] else 0,
            "totalOosViolations": int(row["total_oos_violations"]) if row["total_oos_violations"] else 0,
        }
        return result
    except Exception as e:
        print(f"[DB] Error fetching inspections dashboard stats: {e}")
        return {}

async def fetch_inspection_by_id(unique_id: int) -> dict | None:
    """Fetch a single inspection by unique_id."""
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT * FROM inspections WHERE unique_id = $1",
            unique_id,
        )
        if row:
            return _inspection_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error fetching inspection {unique_id}: {e}")
        return None

async def fetch_inspections_by_dot(
    dot_number: int, limit: int = 10, offset: int = 0,
) -> dict:
    """Fetch paginated inspections for a DOT number with total count."""
    pool = get_pool()
    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(
                """SELECT * FROM inspections
                   WHERE dot_number = $1
                   ORDER BY insp_date DESC
                   LIMIT $2 OFFSET $3""",
                dot_number, limit, offset,
            ),
            pool.fetchrow(
                "SELECT COUNT(*) AS cnt FROM inspections WHERE dot_number = $1",
                dot_number,
            ),
        )
        return {
            "data": [_inspection_row_to_dict(row) for row in rows],
            "total": count_row["cnt"] if count_row else 0,
        }
    except Exception as e:
        print(f"[DB] Error fetching inspections for DOT {dot_number}: {e}")
        return {"data": [], "total": 0}


# ── Crashes ──────────────────────────────────────────────────────────────────

async def fetch_crashes(filters: dict) -> dict:
    """Fetch crashes from the crashes table with optional filters.

    Supported filters:
    - dot_number: Filter by DOT number
    - report_number: Filter by report number
    - report_state: Filter by report state
    - report_date_from / report_date_to: Date range
    - fatalities_min / fatalities_max: Fatalities range
    - injuries_min / injuries_max: Injuries range
    - tow_away: Filter by tow_away (true/false)
    - not_preventable: Filter by not_preventable (true/false)
    - weather_condition_desc: Filter by weather condition
    - vehicle_id_number: Filter by VIN
    - limit / offset: Pagination
    """
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("dot_number"):
        conditions.append(f"dot_number = ${idx}")
        params.append(filters["dot_number"].strip())
        idx += 1

    if filters.get("report_number"):
        conditions.append(f"report_number ILIKE ${idx}")
        params.append(f"%{filters['report_number']}%")
        idx += 1

    if filters.get("report_state"):
        conditions.append(f"report_state = ${idx}")
        params.append(filters["report_state"].upper())
        idx += 1

    if filters.get("report_date_from"):
        conditions.append(f"report_date >= ${idx}::date")
        params.append(filters["report_date_from"])
        idx += 1

    if filters.get("report_date_to"):
        conditions.append(f"report_date <= ${idx}::date")
        params.append(filters["report_date_to"])
        idx += 1

    if filters.get("fatalities_min") is not None:
        conditions.append(f"fatalities >= ${idx}")
        params.append(int(filters["fatalities_min"]))
        idx += 1

    if filters.get("fatalities_max") is not None:
        conditions.append(f"fatalities <= ${idx}")
        params.append(int(filters["fatalities_max"]))
        idx += 1

    if filters.get("injuries_min") is not None:
        conditions.append(f"injuries >= ${idx}")
        params.append(int(filters["injuries_min"]))
        idx += 1

    if filters.get("injuries_max") is not None:
        conditions.append(f"injuries <= ${idx}")
        params.append(int(filters["injuries_max"]))
        idx += 1

    if filters.get("tow_away") == "true":
        conditions.append("tow_away = true")
    elif filters.get("tow_away") == "false":
        conditions.append("tow_away = false")

    if filters.get("not_preventable") == "true":
        conditions.append("not_preventable = true")
    elif filters.get("not_preventable") == "false":
        conditions.append("not_preventable = false")

    if filters.get("weather_condition_desc"):
        conditions.append(f"weather_condition_desc ILIKE ${idx}")
        params.append(f"%{filters['weather_condition_desc']}%")
        idx += 1

    if filters.get("vehicle_id_number"):
        conditions.append(f"vehicle_id_number ILIKE ${idx}")
        params.append(f"%{filters['vehicle_id_number']}%")
        idx += 1

    where = " AND ".join(conditions) if conditions else "TRUE"

    limit_val = min(int(filters.get("limit", 500)), 5000)
    offset_val = int(filters.get("offset", 0))

    query = f"""
        SELECT report_number, report_seq_no, dot_number, report_date,
            report_state, fatalities, injuries, tow_away, hazmat_released,
            trafficway_desc, access_control_desc, road_surface_condition_desc,
            weather_condition_desc, light_condition_desc,
            vehicle_id_number, vehicle_license_number, vehicle_license_state,
            citation_issued_desc,
            seq_num, not_preventable
        FROM crashes
        WHERE {where}
        ORDER BY report_date DESC NULLS LAST
        LIMIT {limit_val} OFFSET {offset_val}
    """

    count_query = f"""
        SELECT COUNT(*) as cnt
        FROM crashes
        WHERE {where}
    """

    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(query, *params),
            pool.fetchrow(count_query, *params),
        )
        filtered_count = count_row["cnt"] if count_row else 0

        crash_dicts = [_crash_row_to_dict(row) for row in rows]

        return {
            "data": crash_dicts,
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching crashes: {e}")
        return {"data": [], "filtered_count": 0}


async def get_crashes_count() -> int:
    """Get total count of crashes."""
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM crashes")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting crashes count: {e}")
        return 0


async def get_crashes_dashboard_stats() -> dict:
    """Get dashboard statistics for crashes."""
    pool = get_pool()
    try:
        row = await pool.fetchrow("""
            SELECT
                COUNT(*) AS total,
                SUM(fatalities) AS total_fatalities,
                SUM(injuries) AS total_injuries,
                COUNT(*) FILTER (WHERE tow_away = true) AS total_towaway,
                COUNT(*) FILTER (WHERE hazmat_released = true) AS total_hazmat_released,
                COUNT(*) FILTER (WHERE not_preventable = true) AS total_not_preventable,
                COUNT(DISTINCT dot_number) AS unique_carriers
            FROM crashes
        """)
        if not row:
            return {}
        return {
            "total": row["total"],
            "totalFatalities": int(row["total_fatalities"] or 0),
            "totalInjuries": int(row["total_injuries"] or 0),
            "totalTowaway": row["total_towaway"],
            "totalHazmatReleased": row["total_hazmat_released"],
            "totalNotPreventable": row["total_not_preventable"],
            "uniqueCarriers": row["unique_carriers"],
        }
    except Exception as e:
        print(f"[DB] Error fetching crashes dashboard stats: {e}")
        return {}


async def fetch_crash_by_report(report_number: str) -> dict | None:
    """Fetch a single crash by report_number."""
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT * FROM crashes WHERE report_number = $1",
            report_number,
        )
        if row:
            return _crash_row_to_dict(row)
        return None
    except Exception as e:
        print(f"[DB] Error fetching crash {report_number}: {e}")
        return None


async def fetch_crashes_by_dot(
    dot_number: str, limit: int = 10, offset: int = 0,
) -> dict:
    """Fetch paginated crashes for a DOT number with total count."""
    pool = get_pool()
    dn = dot_number.strip()
    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(
                """SELECT * FROM crashes
                   WHERE dot_number = $1
                   ORDER BY report_date DESC
                   LIMIT $2 OFFSET $3""",
                dn, limit, offset,
            ),
            pool.fetchrow(
                "SELECT COUNT(*) AS cnt FROM crashes WHERE dot_number = $1",
                dn,
            ),
        )
        return {
            "data": [_crash_row_to_dict(row) for row in rows],
            "total": count_row["cnt"] if count_row else 0,
        }
    except Exception as e:
        print(f"[DB] Error fetching crashes for DOT {dot_number}: {e}")
        return {"data": [], "total": 0}


async def fetch_safety_by_dot(dot_number: str) -> dict | None:
    """Fetch safety data for a carrier from the safety table.

    Returns a dict with BASIC scores, OOS rates, and inspection totals,
    or None if no record exists.
    """
    pool = get_pool()
    try:
        dot_int = int(dot_number.strip())
    except (ValueError, TypeError):
        return None
    try:
        row = await pool.fetchrow(
            "SELECT * FROM safety WHERE dot_number = $1",
            dot_int,
        )
        if not row:
            return None
        d = dict(row)

        # Calculate OOS rates
        driver_insp = d.get("driver_insp_total") or 0
        driver_oos = d.get("driver_oos_insp_total") or 0
        vehicle_insp = d.get("vehicle_insp_total") or 0
        vehicle_oos = d.get("vehicle_oos_insp_total") or 0

        driver_oos_rate = round((driver_oos / driver_insp) * 100, 1) if driver_insp > 0 else 0.0
        vehicle_oos_rate = round((vehicle_oos / vehicle_insp) * 100, 1) if vehicle_insp > 0 else 0.0

        # Build BASIC scores list
        basic_scores = []
        for key, label in [
            ("unsafe_driv", "Unsafe Driving"),
            ("hos_driv", "HOS Compliance"),
            ("driv_fit", "Driver Fitness"),
            ("contr_subst", "Controlled Substances"),
            ("veh_maint", "Vehicle Maintenance"),
        ]:
            measure_val = d.get(f"{key}_measure")
            basic_scores.append({
                "category": label,
                "measure": str(round(float(measure_val), 1)) if measure_val is not None else "N/A",
                "inspWithViol": d.get(f"{key}_insp_w_viol") or 0,
                "alert": (d.get(f"{key}_ac") or "").strip(),
            })

        return {
            "dot_number": str(d.get("dot_number", "")),
            "type": (d.get("type") or "").strip(),
            "insp_total": d.get("insp_total") or 0,
            "driver_insp_total": driver_insp,
            "driver_oos_insp_total": driver_oos,
            "driver_oos_rate": driver_oos_rate,
            "vehicle_insp_total": vehicle_insp,
            "vehicle_oos_insp_total": vehicle_oos,
            "vehicle_oos_rate": vehicle_oos_rate,
            "oos_rates": [
                {"type": "Driver", "rate": f"{driver_oos_rate}%"},
                {"type": "Vehicle", "rate": f"{vehicle_oos_rate}%"},
            ],
            "basic_scores": basic_scores,
        }
    except Exception as e:
        print(f"[DB] Error fetching safety for DOT {dot_number}: {e}")
        return None
