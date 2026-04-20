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

CREATE TABLE IF NOT EXISTS insurance_history (
    id SERIAL PRIMARY KEY,
    docket_number VARCHAR(20),
    dot_number VARCHAR(20),
    ins_form_code VARCHAR(10),
    ins_type_desc VARCHAR(50),
    name_company VARCHAR(100),
    policy_no VARCHAR(50),
    trans_date VARCHAR(15),
    underl_lim_amount VARCHAR(15),
    max_cov_amount VARCHAR(15),
    effective_date VARCHAR(15),
    cancl_effective_date VARCHAR(15),
    effective_date_parsed DATE,
    cancl_date_parsed DATE
);

CREATE INDEX IF NOT EXISTS idx_ih_dot_number ON insurance_history(dot_number);
CREATE INDEX IF NOT EXISTS idx_ih_dot_type ON insurance_history(dot_number, ins_type_desc);
CREATE INDEX IF NOT EXISTS idx_ih_dot_cancl ON insurance_history(dot_number, cancl_effective_date);
CREATE INDEX IF NOT EXISTS idx_ih_effective_date_parsed ON insurance_history(effective_date_parsed);
CREATE INDEX IF NOT EXISTS idx_ih_cancl_date_parsed ON insurance_history(cancl_date_parsed);

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
            \"\"\"
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
            \"\"\",
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
                        \"\"\"
                        INSERT INTO fmcsa_register (number, title, decided, category, date_fetched)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (number, date_fetched) DO UPDATE SET
                            title = EXCLUDED.title,
                            decided = EXCLUDED.decided,
                            category = EXCLUDED.category,
                            updated_at = NOW()
                        \"\"\",
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
    query = f\"\"\"
        SELECT number, title, decided, category, date_fetched
        FROM fmcsa_register
        WHERE {where}
        ORDER BY number
        LIMIT 10000
    \"\"\"

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

    # Insurance history filings (attached by the query via LEFT JOIN LATERAL)
    insurance_history_filings = []
    if "insurance_history_filings" in d and d["insurance_history_filings"]:
        try:
            raw_filings = json.loads(d["insurance_history_filings"])
            insurance_history_filings = _format_insurance_history(raw_filings)
        except (json.JSONDecodeError, TypeError):
            pass

    return {
        "id": str(d.get("id")),
        "mcNumber": mc_number,
        "dotNumber": dot_number,
        "legalName": d.get("legal_name") or "",
        "dbaName": d.get("dba_name") or "",
        "email": d.get("email_address") or "",
        "phone": d.get("phone") or "",
        "fax": d.get("fax") or "",
        "powerUnits": d.get("power_units") or 0,
        "drivers": d.get("total_drivers") or 0,
        "physicalAddress": physical_address,
        "mailingAddress": mailing_address,
        "mcs150Date": _format_mcs150_date(d.get("mcs150_date")),
        "mcs150Mileage": d.get("mcs150_mileage") or "",
        "mcs150MileageYear": d.get("mcs150_mileage_year") or "",
        "operationClassification": operation_classification,
        "carrierOperation": carrier_operation_list,
        "cargoCarried": cargo_carried,
        "hmInd": d.get("hm_ind") == "Y",
        "dunsNumber": d.get("dun_bradstreet_no") or "",
        "safetyRating": d.get("safety_rating") or "",
        "safetyRatingDate": d.get("safety_rating_date") or "",
        "status": status_label,
        "carship": _parse_carship(d.get("carship") or ""),
        "docketStatus": docket_status,
        "companyOfficer1": d.get("company_officer_1") or "",
        "companyOfficer2": d.get("company_officer_2") or "",
        "fleetSize": d.get("fleetsize") or "",
        "addDate": d.get("add_date") or "",
        "truckUnits": d.get("truck_units") or "",
        "busUnits": d.get("bus_units") or "",
        "interstateBeyond100": d.get("interstate_beyond_100_miles") or "",
        "interstateWithin100": d.get("interstate_within_100_miles") or "",
        "intrastateBeyond100": d.get("intrastate_beyond_100_miles") or "",
        "intrastateWithin100": d.get("intrastate_within_100_miles") or "",
        "insuranceHistoryFilings": insurance_history_filings,
    }

async def fetch_carriers(filters: dict) -> dict:
    \"\"\"Fetch carriers from the Census-schema carriers table.

    Insurance data is included by default via LEFT JOIN LATERAL on USDOT.
    If insurance filters are selected, we perform an insurance-first query
    to optimize performance.
    \"\"\"
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    # ------------------------------------------------------------------
    # Insurance-related filters – independent CTE-based pre-filtering
    # ------------------------------------------------------------------
    _INS_TYPE_PATTERN = {"BI&PD": "BIPD%", "CARGO": "CARGO", "BOND": "SURETY", "TRUST FUND": "TRUST FUND"}
    _ACTIVE_POLICY = "(cancl_effective_date IS NULL OR cancl_effective_date = '')"
    
    # CTE parts: list of (cte_name, cte_sql)
    _cte_parts: list[tuple[str, str]] = []
    # JOIN clauses for positive insurance filters
    _ins_joins: list[str] = []
    _cte_idx = 0

    def _add_positive_ins_filter(where_body: str):
        \"\"\"Register an insurance CTE and add an INNER JOIN for it.\"\"\"
        nonlocal _cte_idx
        name = f"_ifd{_cte_idx}"
        _cte_parts.append((
            name,
            f"SELECT DISTINCT dot_number FROM insurance_history WHERE {where_body}",
        ))
        _ins_joins.append(
            f"INNER JOIN {name} ON {name}.dot_number = c.dot_number::text"
        )
        _cte_idx += 1

    # insurance_required
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

    # BIPD / Cargo / Bond / Trust Fund on-file flags
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
                f"WHERE ih.dot_number = c.dot_number::text "
                f"AND ih.ins_type_desc {op} ${idx} "
                f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
            )
            params.append(pattern_val)
            idx += 1

    # BIPD amount range
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

    # Insurance effective-date range (Optimized with parsed DATE column)
    _eff_date_conds: list[str] = []
    if filters.get("ins_effective_date_from"):
        _eff_date_conds.append(f"effective_date_parsed >= ${idx}::date")
        params.append(filters["ins_effective_date_from"])
        idx += 1
    if filters.get("ins_effective_date_to"):
        _eff_date_conds.append(f"effective_date_parsed <= ${idx}::date")
        params.append(filters["ins_effective_date_to"])
        idx += 1
    if _eff_date_conds:
        _add_positive_ins_filter(" AND ".join(_eff_date_conds))

    # Insurance cancellation-date range (Optimized with parsed DATE column)
    _cancl_date_conds: list[str] = []
    if filters.get("ins_cancellation_date_from"):
        _cancl_date_conds.append(f"cancl_date_parsed >= ${idx}::date")
        params.append(filters["ins_cancellation_date_from"])
        idx += 1
    if filters.get("ins_cancellation_date_to"):
        _cancl_date_conds.append(f"cancl_date_parsed <= ${idx}::date")
        params.append(filters["ins_cancellation_date_to"])
        idx += 1
    if _cancl_date_conds:
        _add_positive_ins_filter(" AND ".join(_cancl_date_conds))

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
            f"OR cancl_date_parsed >= CURRENT_DATE)"
        )

    # Next-renewal-date helper (Optimized with parsed DATE column)
    _next_renewal_sql = (
        "CASE "
        "  WHEN MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "         EXTRACT(MONTH FROM effective_date_parsed)::int,"
        "         LEAST(EXTRACT(DAY FROM effective_date_parsed)::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "                 EXTRACT(MONTH FROM effective_date_parsed)::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "       >= CURRENT_DATE "
        "  THEN MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "         EXTRACT(MONTH FROM effective_date_parsed)::int,"
        "         LEAST(EXTRACT(DAY FROM effective_date_parsed)::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "                 EXTRACT(MONTH FROM effective_date_parsed)::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "  ELSE MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,"
        "         EXTRACT(MONTH FROM effective_date_parsed)::int,"
        "         LEAST(EXTRACT(DAY FROM effective_date_parsed)::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,"
        "                 EXTRACT(MONTH FROM effective_date_parsed)::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "END"
    )

    _ACTIVE_POLICY_GUARD = (
        "effective_date_parsed IS NOT NULL "
        "AND (cancl_effective_date IS NULL OR cancl_effective_date = '' "
        "OR cancl_date_parsed >= CURRENT_DATE)"
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

    # Renewal Policy Date range filter
    _renewal_date_conds: list[str] = []
    if filters.get("renewal_date_from"):
        _renewal_date_conds.append(f"({_next_renewal_sql}) >= ${idx}::date")
        params.append(filters["renewal_date_from"])
        idx += 1
    if filters.get("renewal_date_to"):
        _renewal_date_conds.append(f"({_next_renewal_sql}) <= ${idx}::date")
        params.append(filters["renewal_date_to"])
        idx += 1
    if _renewal_date_conds:
        _add_positive_ins_filter(
            f"{_ACTIVE_POLICY_GUARD} AND " + " AND ".join(_renewal_date_conds)
        )

    # ------------------------------------------------------------------
    # Carrier-table filters
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
        conditions.append(f"c.carship = ${idx}")
        params.append(code)
        idx += 1

    active = filters.get("active")
    if active == "true":
        conditions.append("c.docket1_status_code = 'A'")
    elif active == "false":
        conditions.append("(c.docket1_status_code IS NULL OR c.docket1_status_code != 'A')")

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
    # Default / WHERE / LIMIT / OFFSET
    # ------------------------------------------------------------------
    is_filtered = len(conditions) > 0 or len(_ins_joins) > 0
    if not is_filtered:
        conditions.append("c.status_code = 'A'")

    where = " AND ".join(conditions) if conditions else "TRUE"

    limit_val = min(int(filters.get("limit", 500)), 5000)
    offset_val = int(filters.get("offset", 0))

    _LIST_COLS = \"\"\"c.id, c.dot_number, c.legal_name, c.dba_name,
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
        c.crgo_waterwell, c.crgo_cargoothr, c.crgo_cargoothr_desc\"\"\"

    # Build CTE prefix and FROM clause with JOINs
    cte_prefix = ""
    if _cte_parts:
        cte_prefix = "WITH " + ", ".join(
            f"{name} AS ({sql})" for name, sql in _cte_parts
        ) + " "

    from_clause = "carriers c"
    if _ins_joins:
        from_clause += " " + " ".join(_ins_joins)

    # Attach insurance data by default via LEFT JOIN LATERAL
    query = f\"\"\"{cte_prefix}
        SELECT {_LIST_COLS},
          COALESCE(ih_agg.filings, '[]') AS insurance_history_filings
        FROM {from_clause}
        LEFT JOIN LATERAL (
            SELECT json_agg(json_build_object(
              'ins_type_desc', ih.ins_type_desc,
              'max_cov_amount', ih.max_cov_amount,
              'underl_lim_amount', ih.underl_lim_amount,
              'policy_no', ih.policy_no,
              'effective_date', ih.effective_date,
              'ins_form_code', ih.ins_form_code,
              'name_company', ih.name_company,
              'trans_date', ih.trans_date,
              'cancl_effective_date', ih.cancl_effective_date
            ) ORDER BY ih.effective_date DESC) AS filings
            FROM insurance_history ih
            WHERE ih.dot_number = c.dot_number::text
        ) ih_agg ON true
        WHERE {where}
        ORDER BY c.id DESC
        LIMIT {limit_val} OFFSET {offset_val}
    \"\"\"

    fast_count_query = \"\"\"
        SELECT reltuples::bigint AS cnt
        FROM pg_class WHERE relname = 'carriers'
    \"\"\"

    try:
        use_fast_count = not is_filtered
        if use_fast_count:
            rows, count_row = await asyncio.gather(
                pool.fetch(query, *params),
                pool.fetchrow(fast_count_query),
            )
            filtered_count = count_row["cnt"] if count_row else 0
        else:
            count_query = f\"\"\"{cte_prefix}
                SELECT COUNT(*) as cnt
                FROM {from_clause}
                WHERE {where}
            \"\"\"
            rows, count_row = await asyncio.gather(
                pool.fetch(query, *params),
                pool.fetchrow(count_query, *params),
            )
            filtered_count = count_row["cnt"] if count_row else 0

        return {
            "data": [_carrier_row_to_dict(row) for row in rows],
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return {"data": [], "filtered_count": 0}

async def delete_carrier(dot_number: str) -> bool:
    \"\"\"Delete a carrier by DOT number (Census schema uses dot_number as key).\"\"\"
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
    pool = get_pool()
    row = await pool.fetchrow("SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'")
    return row["cnt"] if row else 0

async def get_carrier_dashboard_stats() -> dict:
    global _dashboard_cache, _dashboard_cache_ts
    now = _time.time()
    if _dashboard_cache and (now - _dashboard_cache_ts < _DASHBOARD_CACHE_TTL):
        return _dashboard_cache

    pool = get_pool()
    try:
        # Fast approximate counts
        total_row = await pool.fetchrow("SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'")
        total = total_row["cnt"] if total_row else 0
        
        # For other stats, we might need actual counts or cached values
        # For now, return simplified stats to avoid heavy scans on 4.4M rows
        stats = {
            "total_carriers": total,
            "active_carriers": int(total * 0.8), # Approximation
            "new_ventures_today": 0,
            "recent_scrapes": 0
        }
        _dashboard_cache = stats
        _dashboard_cache_ts = now
        return stats
    except Exception as e:
        print(f"[DB] Error getting dashboard stats: {e}")
        return {"total_carriers": 0, "active_carriers": 0, "new_ventures_today": 0, "recent_scrapes": 0}

async def get_carriers_by_mc_range(start: str, end: str) -> list[dict]:
    pool = get_pool()
    # This is used for batch scraping, usually small ranges
    query = \"\"\"
        SELECT * FROM carriers 
        WHERE (docket1prefix = 'MC' AND docket1::int >= $1::int AND docket1::int <= $2::int)
        LIMIT 1000
    \"\"\"
    rows = await pool.fetch(query, start, end)
    return [_carrier_row_to_dict(row) for row in rows]

async def fetch_users() -> list[dict]:
    pool = get_pool()
    rows = await pool.fetch("SELECT * FROM users ORDER BY created_at DESC")
    return [dict(row) for row in rows]

async def fetch_user_by_email(email: str) -> Optional[dict]:
    pool = get_pool()
    row = await pool.fetchrow("SELECT * FROM users WHERE email = $1", email.lower())
    return dict(row) if row else None

async def get_user_password_hash(email: str) -> Optional[str]:
    pool = get_pool()
    row = await pool.fetchval("SELECT password_hash FROM users WHERE email = $1", email.lower())
    return row

async def create_user(user_data: dict) -> bool:
    pool = get_pool()
    try:
        await pool.execute(
            \"\"\"
            INSERT INTO users (user_id, name, email, password_hash, role, plan, daily_limit)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            \"\"\",
            user_data["user_id"], user_data["name"], user_data["email"].lower(),
            user_data["password_hash"], user_data.get("role", "user"),
            user_data.get("plan", "Free"), user_data.get("daily_limit", 50)
        )
        return True
    except Exception as e:
        print(f"[DB] Error creating user: {e}")
        return False

async def update_user(email: str, update_data: dict) -> bool:
    pool = get_pool()
    fields = []
    values = []
    for i, (k, v) in enumerate(update_data.items(), start=2):
        fields.append(f"{k} = ${i}")
        values.append(v)
    
    if not fields:
        return True
        
    query = f"UPDATE users SET {', '.join(fields)}, updated_at = NOW() WHERE email = $1"
    try:
        await pool.execute(query, email.lower(), *values)
        return True
    except Exception as e:
        print(f"[DB] Error updating user: {e}")
        return False

async def delete_user(email: str) -> bool:
    pool = get_pool()
    try:
        await pool.execute("DELETE FROM users WHERE email = $1", email.lower())
        return True
    except Exception as e:
        print(f"[DB] Error deleting user: {e}")
        return False

async def fetch_blocked_ips() -> list[dict]:
    pool = get_pool()
    rows = await pool.fetch("SELECT * FROM blocked_ips ORDER BY blocked_at DESC")
    return [dict(row) for row in rows]

async def is_ip_blocked(ip: str) -> bool:
    pool = get_pool()
    val = await pool.fetchval("SELECT 1 FROM blocked_ips WHERE ip_address = $1", ip)
    return bool(val)

async def block_ip(ip: str, reason: str = None, blocked_by: str = None) -> bool:
    pool = get_pool()
    try:
        await pool.execute(
            "INSERT INTO blocked_ips (ip_address, reason, blocked_by) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            ip, reason, blocked_by
        )
        return True
    except Exception as e:
        print(f"[DB] Error blocking IP: {e}")
        return False

async def unblock_ip(ip: str) -> bool:
    pool = get_pool()
    try:
        await pool.execute("DELETE FROM blocked_ips WHERE ip_address = $1", ip)
        return True
    except Exception as e:
        print(f"[DB] Error unblocking IP: {e}")
        return False

async def get_fmcsa_categories() -> list[str]:
    pool = get_pool()
    rows = await pool.fetch("SELECT DISTINCT category FROM fmcsa_register WHERE category IS NOT NULL ORDER BY category")
    return [row["category"] for row in rows]

async def delete_fmcsa_entries_before_date(date_str: str) -> int:
    pool = get_pool()
    result = await pool.execute("DELETE FROM fmcsa_register WHERE date_fetched < $1", date_str)
    return int(result.split()[-1])

async def save_new_venture_entries(entries: list[dict], scrape_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}
    
    saved = 0
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for entry in entries:
                    dot = entry.get("dot_number")
                    if not dot: continue
                    
                    # Simplified upsert for new_ventures
                    await conn.execute(
                        \"\"\"
                        INSERT INTO new_ventures (dot_number, name, add_date, scrape_date)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (dot_number, add_date) DO UPDATE SET
                            name = EXCLUDED.name,
                            scrape_date = EXCLUDED.scrape_date,
                            updated_at = NOW()
                        \"\"\",
                        dot, entry.get("name"), entry.get("add_date"), scrape_date
                    )
                    saved += 1
        return {"success": True, "saved": saved}
    except Exception as e:
        print(f"[DB] Error saving new ventures: {e}")
        return {"success": False, "error": str(e)}

async def fetch_new_ventures(filters: dict) -> list[dict]:
    pool = get_pool()
    # Simplified fetch
    rows = await pool.fetch("SELECT * FROM new_ventures ORDER BY add_date DESC LIMIT 1000")
    return [dict(row) for row in rows]

async def fetch_new_venture_by_id(nv_id: str) -> Optional[dict]:
    pool = get_pool()
    row = await pool.fetchrow("SELECT * FROM new_ventures WHERE id = $1", nv_id)
    return dict(row) if row else None

async def get_new_venture_count() -> int:
    pool = get_pool()
    row = await pool.fetchval("SELECT COUNT(*) FROM new_ventures")
    return row or 0

async def get_new_venture_scraped_dates() -> list[str]:
    pool = get_pool()
    rows = await pool.fetch("SELECT DISTINCT scrape_date FROM new_ventures ORDER BY scrape_date DESC")
    return [row["scrape_date"] for row in rows]

async def delete_new_venture(nv_id: str) -> bool:
    pool = get_pool()
    result = await pool.execute("DELETE FROM new_ventures WHERE id = $1", nv_id)
    return not result.endswith("0")

async def fetch_insurance_history(dot_number: str) -> list[dict]:
    pool = get_pool()
    rows = await pool.fetch(
        \"\"\"
        SELECT * FROM insurance_history 
        WHERE dot_number = $1 
        ORDER BY effective_date DESC
        \"\"\",
        dot_number
    )
    return _format_insurance_history([dict(r) for r in rows])
