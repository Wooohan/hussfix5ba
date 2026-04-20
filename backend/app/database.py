import os
import json
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
-- NOTE: carriers table is now populated from the Company Census File (az4n-8mr2)
-- with ~4.4M records.  The table already exists in the database so we do NOT
-- try to CREATE it here.  The schema is managed externally.

CREATE TABLE IF NOT EXISTS carriers (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    "MCS150_DATE" text,
    "ADD_DATE" text,
    "STATUS_CODE" text,
    "DOT_NUMBER" text,
    "DUN_BRADSTREET_NO" text,
    "PHY_OMC_REGION" text,
    "SAFETY_INV_TERR" text,
    "CARRIER_OPERATION" text,
    "BUSINESS_ORG_ID" text,
    "MCS150_MILEAGE" text,
    "MCS150_MILEAGE_YEAR" text,
    "MCS151_MILEAGE" text,
    "TOTAL_CARS" text,
    "MCS150_UPDATE_CODE_ID" text,
    "PRIOR_REVOKE_FLAG" text,
    "PRIOR_REVOKE_DOT_NUMBER" text,
    "PHONE" text,
    "FAX" text,
    "CELL_PHONE" text,
    "COMPANY_OFFICER_1" text,
    "COMPANY_OFFICER_2" text,
    "BUSINESS_ORG_DESC" text,
    "TRUCK_UNITS" text,
    "POWER_UNITS" integer,
    "BUS_UNITS" text,
    "FLEETSIZE" text,
    "REVIEW_ID" text,
    "RECORDABLE_CRASH_RATE" text,
    "MAIL_NATIONALITY_INDICATOR" text,
    "PHY_NATIONALITY_INDICATOR" text,
    "PHY_BARRIO" text,
    "MAIL_BARRIO" text,
    "CARSHIP" text,
    "DOCKET1PREFIX" text,
    "DOCKET1" text,
    "DOCKET2PREFIX" text,
    "DOCKET2" text,
    "DOCKET3PREFIX" text,
    "DOCKET3" text,
    "POINTNUM" text,
    "TOTAL_INTRASTATE_DRIVERS" text,
    "MCSIPSTEP" text,
    "MCSIPDATE" text,
    "HM_Ind" text,
    "INTERSTATE_BEYOND_100_MILES" text,
    "INTERSTATE_WITHIN_100_MILES" text,
    "INTRASTATE_BEYOND_100_MILES" text,
    "INTRASTATE_WITHIN_100_MILES" text,
    "TOTAL_CDL" text,
    "TOTAL_DRIVERS" integer,
    "AVG_DRIVERS_LEASED_PER_MONTH" text,
    "CLASSDEF" text,
    "LEGAL_NAME" text,
    "DBA_NAME" text,
    "PHY_STREET" text,
    "PHY_CITY" text,
    "PHY_COUNTRY" text,
    "PHY_STATE" text,
    "PHY_ZIP" text,
    "PHY_CNTY" text,
    "CARRIER_MAILING_STREET" text,
    "CARRIER_MAILING_STATE" text,
    "CARRIER_MAILING_CITY" text,
    "CARRIER_MAILING_COUNTRY" text,
    "CARRIER_MAILING_ZIP" text,
    "CARRIER_MAILING_CNTY" text,
    "CARRIER_MAILING_UND_DATE" text,
    "DRIVER_INTER_TOTAL" text,
    "EMAIL_ADDRESS" text,
    "REVIEW_TYPE" text,
    "REVIEW_DATE" text,
    "SAFETY_RATING" text,
    "SAFETY_RATING_DATE" text,
    "UNDELIV_PHY" text,
    "CRGO_GENFREIGHT" text,
    "CRGO_HOUSEHOLD" text,
    "CRGO_METALSHEET" text,
    "CRGO_MOTOVEH" text,
    "CRGO_DRIVETOW" text,
    "CRGO_LOGPOLE" text,
    "CRGO_BLDGMAT" text,
    "CRGO_MOBILEHOME" text,
    "CRGO_MACHLRG" text,
    "CRGO_PRODUCE" text,
    "CRGO_LIQGAS" text,
    "CRGO_INTERMODAL" text,
    "CRGO_PASSENGERS" text,
    "CRGO_OILFIELD" text,
    "CRGO_LIVESTOCK" text,
    "CRGO_GRAINFEED" text,
    "CRGO_COALCOKE" text,
    "CRGO_MEAT" text,
    "CRGO_GARBAGE" text,
    "CRGO_USMAIL" text,
    "CRGO_CHEM" text,
    "CRGO_DRYBULK" text,
    "CRGO_COLDFOOD" text,
    "CRGO_BEVERAGES" text,
    "CRGO_PAPERPROD" text,
    "CRGO_UTILITY" text,
    "CRGO_FARMSUPP" text,
    "CRGO_CONSTRUCT" text,
    "CRGO_WATERWELL" text,
    "CRGO_CARGOOTHR" text,
    "CRGO_CARGOOTHR_DESC" text,
    "OWNTRUCK" text,
    "OWNTRACT" text,
    "OWNTRAIL" text,
    "OWNCOACH" text,
    "OWNSCHOOL_1_8" text,
    "OWNSCHOOL_9_15" text,
    "OWNSCHOOL_16" text,
    "OWNBUS_16" text,
    "OWNVAN_1_8" text,
    "OWNVAN_9_15" text,
    "OWNLIMO_1_8" text,
    "OWNLIMO_9_15" text,
    "OWNLIMO_16" text,
    "TRMTRUCK" text,
    "TRMTRACT" text,
    "TRMTRAIL" text,
    "TRMCOACH" text,
    "TRMSCHOOL_1_8" text,
    "TRMSCHOOL_9_15" text,
    "TRMSCHOOL_16" text,
    "TRMBUS_16" text,
    "TRMVAN_1_8" text,
    "TRMVAN_9_15" text,
    "TRMLIMO_1_8" text,
    "TRMLIMO_9_15" text,
    "TRMLIMO_16" text,
    "TRPTRUCK" text,
    "TRPTRACT" text,
    "TRPTRAIL" text,
    "TRPCOACH" text,
    "TRPSCHOOL_1_8" text,
    "TRPSCHOOL_9_15" text,
    "TRPSCHOOL_16" text,
    "TRPBUS_16" text,
    "TRPVAN_1_8" text,
    "TRPVAN_9_15" text,
    "TRPLIMO_1_8" text,
    "TRPLIMO_9_15" text,
    "TRPLIMO_16" text,
    "DOCKET1_STATUS_CODE" text,
    "DOCKET2_STATUS_CODE" text,
    "DOCKET3_STATUS_CODE" text
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

    The carriers table is now the Company Census File schema.
    We upsert on dot_number (bigint) which is the natural key.
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
                dun_bradstreet_no, safety_rating, safety_rating_date,
                status_code, carship, docket1prefix, docket1
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16,
                $17, $18, $19,
                $20, $21, $22,
                $23, $24, $25,
                $26, $27, $28, $29
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
                safety_rating = EXCLUDED.safety_rating,
                safety_rating_date = EXCLUDED.safety_rating_date,
                status_code = EXCLUDED.status_code,
                carship = EXCLUDED.carship,
                docket1prefix = EXCLUDED.docket1prefix,
                docket1 = EXCLUDED.docket1
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
            record.get("safety_rating"),
            record.get("safety_rating_date"),
            record.get("status_code"),
            record.get("carship"),
            record.get("docket1prefix"),
            record.get("docket1"),
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

_CARGO_COL_MAP = {
    "crgo_genfreight": "General Freight",
    "crgo_household": "Household Goods",
    "crgo_metalsheet": "Metal/Sheets/Coils",
    "crgo_motoveh": "Motor Vehicles",
    "crgo_drivetow": "Drive-Away/Tow-Away",
    "crgo_logpole": "Logs/Poles/Lumber",
    "crgo_bldgmat": "Building Materials",
    "crgo_mobilehome": "Mobile Homes",
    "crgo_machlrg": "Machinery/Large Objects",
    "crgo_produce": "Fresh Produce",
    "crgo_liqgas": "Liquids/Gases",
    "crgo_intermodal": "Intermodal Containers",
    "crgo_passengers": "Passengers",
    "crgo_oilfield": "Oilfield Equipment",
    "crgo_livestock": "Livestock",
    "crgo_grainfeed": "Grain/Feed/Hay",
    "crgo_coalcoke": "Coal/Coke",
    "crgo_meat": "Meat",
    "crgo_garbage": "Garbage/Refuse",
    "crgo_usmail": "US Mail",
    "crgo_chem": "Chemicals",
    "crgo_drybulk": "Dry Bulk",
    "crgo_coldfood": "Refrigerated Food",
    "crgo_beverages": "Beverages",
    "crgo_paperprod": "Paper Products",
    "crgo_utility": "Utilities",
    "crgo_farmsupp": "Farm Supplies",
    "crgo_construct": "Construction",
    "crgo_waterwell": "Water Well",
    "crgo_cargoothr": "Other",
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

_CARSHIP_MAP = {
    "C": "CARRIER",
    "B": "BROKER",
    "R": "REGISTRANT",
    "F": "FREIGHT FORWARDER",
    "I": "IEP (Intermodal Equipment Provider)",
    "S": "SHIPPER",
    "T": "CARGO TANK FACILITY",
}

def _build_mc_number(d: dict) -> str:
    """Build a display MC/MX/FF number from all docket fields.

    Returns a comma-separated list of all docket numbers, e.g.
    'MC-1418760, FF-26167'.
    """
    parts = []
    for pfx_key, num_key in [("docket1prefix", "docket1"), ("docket2prefix", "docket2"), ("docket3prefix", "docket3")]:
        prefix = d.get(pfx_key) or ""
        number = d.get(num_key) or ""
        if prefix and number:
            parts.append(f"{prefix}-{number}")
        elif number:
            parts.append(number)
    return ", ".join(parts)

def _build_address(street: str, city: str, state: str, zipcode: str, country: str = "") -> str:
    parts = [p for p in [street, city, state, zipcode] if p]
    addr = ", ".join(parts)
    if country and country != "US":
        addr = f"{addr}, {country}" if addr else country
    return addr

def _parse_carship(raw: str) -> str:
    """Convert semicolon-separated carship codes to labels."""
    if not raw:
        return ""
    codes = [c.strip() for c in raw.split(";")]
    labels = [_CARSHIP_MAP.get(c, c) for c in codes]
    return " / ".join(labels)

def _build_cargo_list(d: dict) -> list[str]:
    """Build a list of human-readable cargo types from crgo_* columns."""
    result = []
    for col, label in _CARGO_COL_MAP.items():
        val = d.get(col)
        if val and val.strip().upper() == "X":
            result.append(label)
    other_desc = d.get("crgo_cargoothr_desc")
    if other_desc and other_desc.strip():
        # Replace generic "Other" with the description if present
        if "Other" in result:
            result.remove("Other")
        result.append(other_desc.strip())
    return result

def _format_mcs150_date(raw: str) -> str:
    """Convert Census mcs150_date (e.g. '20130729 2240') to MM/DD/YYYY."""
    if not raw:
        return ""
    date_part = raw.strip().split()[0] if raw else ""
    if len(date_part) == 8 and date_part.isdigit():
        return f"{date_part[4:6]}/{date_part[6:8]}/{date_part[:4]}"
    return raw

def _carrier_row_to_dict(row) -> dict:
    """Map a Census-schema carriers row to the API response format.

    The API response keeps the same field names the frontend expects
    (mc_number, dot_number, email, phone, physical_address, etc.) so the
    frontend needs minimal changes.
    """
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
    op_code = d.get("carrier_operation") or ""
    carrier_operation_list = [_CARRIER_OP_MAP.get(op_code, op_code)] if op_code else []

    classdef = d.get("classdef") or ""
    operation_classification = [c.strip() for c in classdef.split(";")] if classdef else []

    status_code = d.get("status_code") or ""
    status_label = _STATUS_CODE_MAP.get(status_code, status_code)

    # Operating Authority status (from docket1_status_code)
    docket_status_code = d.get("docket1_status_code") or ""
    _DOCKET_STATUS_MAP = {"A": "AUTHORIZED", "I": "NOT AUTHORIZED", "P": "PENDING"}
    docket_status = _DOCKET_STATUS_MAP.get(docket_status_code, "NOT AUTHORIZED")

    mcs150_date = _format_mcs150_date(d.get("mcs150_date") or "")
    mileage = d.get("mcs150_mileage") or ""
    mileage_year = d.get("mcs150_mileage_year") or ""
    mcs150_mileage = f"{mileage} ({mileage_year})" if mileage and mileage_year else mileage

    # Build operating-territory flags
    territory = []
    if d.get("interstate_beyond_100_miles"):
        territory.append("Interstate (>100 mi)")
    if d.get("interstate_within_100_miles"):
        territory.append("Interstate (<100 mi)")
    if d.get("intrastate_beyond_100_miles"):
        territory.append("Intrastate (>100 mi)")
    if d.get("intrastate_within_100_miles"):
        territory.append("Intrastate (<100 mi)")

    entity_type = _parse_carship(d.get("carship") or "")

    result = {
        "id": str(d.get("id", "")),
        "mc_number": mc_number,
        "dot_number": dot_number,
        "legal_name": d.get("legal_name") or "",
        "dba_name": d.get("dba_name") or "",
        "entity_type": entity_type,
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
        "hm_ind": d.get("hm_ind") or "",
        "duns_number": d.get("dun_bradstreet_no") or "",
        "safety_rating": d.get("safety_rating") or "",
        "safety_rating_date": d.get("safety_rating_date") or "",
        "operating_territory": territory,
        "company_officer_1": d.get("company_officer_1") or "",
        "company_officer_2": d.get("company_officer_2") or "",
        "fleetsize": d.get("fleetsize") or "",
        "add_date": d.get("add_date") or "",
        "truck_units": d.get("truck_units") or "",
        "bus_units": d.get("bus_units") or "",
        # JSONB fields that no longer exist in Census data
        "basic_scores": None,
        "oos_rates": None,
        "insurance_policies": None,
        "inspections": None,
        "crashes": None,
    }

    # Insurance history filings – populated separately via batch fetch
    # in fetch_carriers() after this function returns.
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
        mc_prefix = ""
        mc_num = mc_raw
        for pfx in ("MC", "MX", "FF"):
            if mc_raw.startswith(pfx):
                mc_prefix = pfx
                mc_num = mc_raw[len(pfx):].lstrip("-").strip()
                break
        docket_clauses = []
        if mc_prefix:
            for dk_pfx, dk_num in [("docket1prefix", "docket1"), ("docket2prefix", "docket2"), ("docket3prefix", "docket3")]:
                docket_clauses.append(f"(c.{dk_pfx} = ${idx} AND c.{dk_num} = ${idx + 1})")
                params.extend([mc_prefix, mc_num])
                idx += 2
        else:
            for dk_num in ["docket1", "docket2", "docket3"]:
                docket_clauses.append(f"c.{dk_num} = ${idx}")
                params.append(mc_num)
                idx += 1
        conditions.append(f"({' OR '.join(docket_clauses)})")

    if filters.get("dot_number"):
        dot_val = filters["dot_number"].strip()
        conditions.append(f"c.dot_number = ${idx}::bigint")
        params.append(int(dot_val))
        idx += 1

    if filters.get("legal_name"):
        conditions.append(f"c.legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    entity_type = filters.get("entity_type")
    if entity_type:
        et_upper = entity_type.upper()
        reverse_map = {v: k for k, v in _CARSHIP_MAP.items()}
        code = reverse_map.get(et_upper, et_upper)
        # Use exact match (=) instead of ILIKE with wildcards to leverage
        # B-Tree index on carship – exponentially faster on 4.4M rows.
        conditions.append(f"c.carship = ${idx}")
        params.append(code)
        idx += 1

    active = filters.get("active")
    if active == "true":
        conditions.append("c.docket1_status_code = 'A'")
    elif active == "false":
        conditions.append("(c.docket1_status_code IS NULL OR c.docket1_status_code != 'A')")

    # Years-in-business: compare the raw YYYYMMDD string directly so
    # PG can use a B-tree index on add_date without per-row TO_DATE.
    if filters.get("years_in_business_min"):
        conditions.append(
            f"c.add_date IS NOT NULL AND c.add_date != '' "
            f"AND c.add_date <= to_char(CURRENT_DATE - make_interval(years => ${idx}), 'YYYYMMDD')"
        )
        params.append(int(filters["years_in_business_min"]))
        idx += 1
    if filters.get("years_in_business_max"):
        conditions.append(
            f"c.add_date IS NOT NULL AND c.add_date != '' "
            f"AND c.add_date >= to_char(CURRENT_DATE - make_interval(years => ${idx}), 'YYYYMMDD')"
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
        cargo = filters["cargo"]
        if isinstance(cargo, str):
            cargo = cargo.split(",")
        reverse_cargo = {v: k for k, v in _CARGO_COL_MAP.items()}
        or_clauses = []
        for c in cargo:
            col = reverse_cargo.get(c.strip())
            if col:
                or_clauses.append(f"c.{col} = 'X'")
        if or_clauses:
            conditions.append(f"({' OR '.join(or_clauses)})")

    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("c.hm_ind = 'Y'")
    elif hazmat == "false":
        conditions.append("(c.hm_ind IS NULL OR c.hm_ind != 'Y')")

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

    # ------------------------------------------------------------------
    # Insurance-related filters – independent CTE-based pre-filtering
    # ------------------------------------------------------------------
    # Each independent insurance condition pre-selects qualifying
    # dot_numbers (USDOT) from insurance_history via a CTE, then carriers
    # are filtered with INNER JOINs using LPAD(c.dot_number::text, 8, '0')
    # to match the zero-padded USDOT in insurance_history.dot_number.
    # This is dramatically faster than correlated EXISTS on 4M+ carriers
    # because:
    #   1. insurance_history is scanned once per filter (not per carrier)
    #   2. The dot_number index drives the carrier lookup
    #   3. Each filter is independent so different insurance_history rows
    #      can satisfy different filters

    _INS_TYPE_PATTERN = {"BI&PD": "BIPD%", "CARGO": "CARGO", "BOND": "SURETY", "TRUST FUND": "TRUST FUND"}
    # Use USDOT (dot_number) to match insurance_history instead of docket_number.
    # insurance_history.dot_number stores zero-padded USDOT (e.g. '03169585')
    # while carriers.dot_number stores the integer.  We cast carriers.dot_number
    # to text with LPAD to match the zero-padded format in insurance_history.
    _IH_DOT_EXPR = "LPAD(c.dot_number::text, 8, '0')"
    _ACTIVE_POLICY = "(cancl_effective_date IS NULL OR cancl_effective_date = '')"

    # CTE parts: list of (cte_name, cte_sql)
    _cte_parts: list[tuple[str, str]] = []
    # JOIN clauses for positive insurance filters
    _ins_joins: list[str] = []
    _cte_idx = 0

    def _add_positive_ins_filter(where_body: str):
        """Register an insurance CTE using dot_number (USDOT) and add an INNER JOIN."""
        nonlocal _cte_idx
        name = f"_ifd{_cte_idx}"
        _cte_parts.append((
            name,
            f"SELECT DISTINCT dot_number FROM insurance_history WHERE {where_body}",
        ))
        _ins_joins.append(
            f"INNER JOIN {name} ON {name}.dot_number = {_IH_DOT_EXPR}"
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
                f"WHERE ih.dot_number = {_IH_DOT_EXPR} "
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

    # Select only the columns actually used by _carrier_row_to_dict
    _LIST_COLS = """c.id, c.dot_number, c.legal_name, c.dba_name,
        c.phone, c.email_address, c.fax,
        c.power_units, c.total_drivers,
        c.phy_street, c.phy_city, c.phy_state, c.phy_zip, c.phy_country,
        c.carrier_mailing_street, c.carrier_mailing_city,
        c.carrier_mailing_state, c.carrier_mailing_zip, c.carrier_mailing_country,
        c.mcs150_date, c.mcs150_mileage, c.mcs150_mileage_year,
        c.classdef, c.carrier_operation, c.hm_ind,
        c.dun_bradstreet_no, c.safety_rating, c.safety_rating_date,
        c.status_code, c.carship,
        c.docket1prefix, c.docket1, c.docket2prefix, c.docket2,
        c.docket3prefix, c.docket3, c.docket1_status_code,
        c.company_officer_1, c.company_officer_2,
        c.fleetsize, c.add_date, c.truck_units, c.bus_units,
        c.interstate_beyond_100_miles, c.interstate_within_100_miles,
        c.intrastate_beyond_100_miles, c.intrastate_within_100_miles,
        c.crgo_genfreight, c.crgo_household, c.crgo_metalsheet, c.crgo_motoveh,
        c.crgo_drivetow, c.crgo_logpole, c.crgo_bldgmat, c.crgo_mobilehome,
        c.crgo_machlrg, c.crgo_produce, c.crgo_liqgas, c.crgo_intermodal,
        c.crgo_passengers, c.crgo_oilfield, c.crgo_livestock, c.crgo_grainfeed,
        c.crgo_coalcoke, c.crgo_meat, c.crgo_garbage, c.crgo_usmail,
        c.crgo_chem, c.crgo_drybulk, c.crgo_coldfood, c.crgo_beverages,
        c.crgo_paperprod, c.crgo_utility, c.crgo_farmsupp, c.crgo_construct,
        c.crgo_waterwell, c.crgo_cargoothr, c.crgo_cargoothr_desc"""

    # Build CTE prefix and FROM clause with JOINs
    cte_prefix = ""
    if _cte_parts:
        cte_prefix = "WITH " + ", ".join(
            f"{name} AS ({sql})" for name, sql in _cte_parts
        ) + " "

    from_clause = "carriers c"
    if _ins_joins:
        from_clause += " " + " ".join(_ins_joins)

    query = f"""{cte_prefix}
        SELECT {_LIST_COLS}
        FROM {from_clause}
        WHERE {where}
        ORDER BY c.id DESC
        LIMIT {limit_val} OFFSET {offset_val}
    """

    # ------------------------------------------------------------------
    # Count strategy – exact COUNT(*) for filtered, pg_class for default
    # ------------------------------------------------------------------
    fast_count_query = """
        SELECT reltuples::bigint AS cnt
        FROM pg_class WHERE relname = 'carriers'
    """

    try:
        use_fast_count = not is_filtered
        if use_fast_count:
            rows, count_row = await asyncio.gather(
                pool.fetch(query, *params),
                pool.fetchrow(fast_count_query),
            )
            filtered_count = count_row["cnt"] if count_row else 0
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

        # Batch-fetch insurance history for the returned rows using USDOT
        carrier_dicts = [_carrier_row_to_dict(row) for row in rows]

        # Build USDOT keys (zero-padded to 8 digits to match insurance_history.dot_number)
        dot_keys = []
        for row in rows:
            d = dict(row)
            raw_dot = d.get("dot_number")
            if raw_dot is not None:
                dot_keys.append(str(raw_dot).zfill(8))
            else:
                dot_keys.append("")

        non_empty_dots = [k for k in dot_keys if k]
        if non_empty_dots:
            unique_dots = list(set(non_empty_dots))
            ih_rows = await pool.fetch(
                """
                SELECT dot_number, docket_number, ins_type_desc, max_cov_amount,
                       underl_lim_amount, policy_no, effective_date,
                       ins_form_code, name_company, trans_date,
                       cancl_effective_date
                FROM insurance_history
                WHERE dot_number = ANY($1)
                ORDER BY effective_date DESC
                """,
                unique_dots,
            )
            ih_map: dict[str, list[dict]] = {}
            for ih_row in ih_rows:
                dk = ih_row["dot_number"]
                if dk not in ih_map:
                    ih_map[dk] = []
                ih_map[dk].append(dict(ih_row))

            for i, carrier in enumerate(carrier_dicts):
                dk = dot_keys[i]
                if dk and dk in ih_map:
                    carrier["insurance_history_filings"] = _format_insurance_history(ih_map[dk])

        return {
            "data": carrier_dicts,
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return {"data": [], "filtered_count": 0}

async def delete_carrier(dot_number: str) -> bool:
    """Delete a carrier by DOT number (Census schema uses dot_number as key)."""
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM carriers WHERE dot_number = $1",
            int(dot_number),
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
        row = await pool.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status_code = 'A') AS active,
                COUNT(*) FILTER (WHERE email_address IS NOT NULL AND email_address != '') AS with_email,
                COUNT(*) FILTER (WHERE hm_ind = 'Y') AS hazmat,
                COUNT(*) FILTER (WHERE carrier_operation = 'A') AS interstate,
                COUNT(*) FILTER (WHERE carrier_operation = 'B') AS intrastate_hm,
                COUNT(*) FILTER (WHERE carrier_operation = 'C') AS intrastate_non_hm
            FROM carriers
        """)
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
        }
        _dashboard_cache = result
        _dashboard_cache_ts = now
        return result
    except Exception as e:
        print(f"[DB] Error fetching dashboard stats: {e}")
        return _dashboard_cache or {}

async def update_carrier_safety(dot_number: str, safety_data: dict) -> bool:
    """Update safety-related fields for a carrier by DOT number."""
    pool = get_pool()
    try:
        result = await pool.execute(
            """
            UPDATE carriers
            SET safety_rating = $1,
                safety_rating_date = $2
            WHERE dot_number = $3
            """,
            safety_data.get("safety_rating"),
            safety_data.get("safety_rating_date"),
            int(dot_number),
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating safety for DOT {dot_number}: {e}")
        return False

# Select only the columns actually used by _carrier_row_to_dict (shared
# with fetch_carriers) to avoid fetching 100+ unused columns per row.
_MC_RANGE_COLS = """id, dot_number, legal_name, dba_name,
    phone, email_address, fax,
    power_units, total_drivers,
    phy_street, phy_city, phy_state, phy_zip, phy_country,
    carrier_mailing_street, carrier_mailing_city,
    carrier_mailing_state, carrier_mailing_zip, carrier_mailing_country,
    mcs150_date, mcs150_mileage, mcs150_mileage_year,
    classdef, carrier_operation, hm_ind,
    dun_bradstreet_no, safety_rating, safety_rating_date,
    status_code, carship,
    docket1prefix, docket1, docket2prefix, docket2,
    docket3prefix, docket3, docket1_status_code,
    company_officer_1, company_officer_2,
    fleetsize, add_date, truck_units, bus_units,
    interstate_beyond_100_miles, interstate_within_100_miles,
    intrastate_beyond_100_miles, intrastate_within_100_miles,
    crgo_genfreight, crgo_household, crgo_metalsheet, crgo_motoveh,
    crgo_drivetow, crgo_logpole, crgo_bldgmat, crgo_mobilehome,
    crgo_machlrg, crgo_produce, crgo_liqgas, crgo_intermodal,
    crgo_passengers, crgo_oilfield, crgo_livestock, crgo_grainfeed,
    crgo_coalcoke, crgo_meat, crgo_garbage, crgo_usmail,
    crgo_chem, crgo_drybulk, crgo_coldfood, crgo_beverages,
    crgo_paperprod, crgo_utility, crgo_farmsupp, crgo_construct,
    crgo_waterwell, crgo_cargoothr, crgo_cargoothr_desc"""

async def get_carriers_by_mc_range(start_mc: str, end_mc: str) -> list[dict]:
    """Fetch carriers whose docket1 number falls within start_mc..end_mc."""
    pool = get_pool()
    try:
        rows = await pool.fetch(
            f"""
            SELECT {_MC_RANGE_COLS} FROM carriers
            WHERE docket1 IS NOT NULL
              AND docket1 ~ '^[0-9]+$'
              AND docket1::bigint BETWEEN $1 AND $2
            ORDER BY docket1::bigint
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
