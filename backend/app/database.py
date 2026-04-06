import os
import json
import asyncio
import asyncpg
from typing import Optional
from datetime import date as _date

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    import warnings
    warnings.warn("DATABASE_URL is not set. Database connections will fail.")

_pool: Optional[asyncpg.Pool] = None

_SCHEMA_SQL = """
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

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_carriers_mc_number ON carriers(mc_number);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number ON carriers(dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_created_at ON carriers(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_carriers_status ON carriers(status);

CREATE INDEX IF NOT EXISTS idx_carriers_legal_name_trgm ON carriers USING gin (legal_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_mc_number_trgm ON carriers USING gin (mc_number gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number_trgm ON carriers USING gin (dot_number gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_fmcsa_register_number ON fmcsa_register(number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_date_fetched ON fmcsa_register(date_fetched DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_register_category ON fmcsa_register(category);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

CREATE INDEX IF NOT EXISTS idx_blocked_ips_ip ON blocked_ips(ip_address);

CREATE INDEX IF NOT EXISTS idx_insurance_history_docket ON insurance_history(docket_number);
CREATE INDEX IF NOT EXISTS idx_insurance_history_docket_type ON insurance_history(docket_number, ins_type_desc);
CREATE INDEX IF NOT EXISTS idx_insurance_history_docket_cancl ON insurance_history(docket_number, cancl_effective_date);
CREATE INDEX IF NOT EXISTS idx_ih_docket_type_cancl ON insurance_history(docket_number, ins_type_desc, cancl_effective_date);
CREATE INDEX IF NOT EXISTS idx_ih_docket_company ON insurance_history(docket_number, name_company);
CREATE INDEX IF NOT EXISTS idx_ih_docket_effective ON insurance_history(docket_number, effective_date);
CREATE INDEX IF NOT EXISTS idx_insurance_history_docket_number ON insurance_history(docket_number);
CREATE INDEX IF NOT EXISTS idx_insurance_history_ins_type_trgm ON insurance_history USING gin (ins_type_desc gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_insurance_history_cancl_effective ON insurance_history(cancl_effective_date);
CREATE INDEX IF NOT EXISTS idx_insurance_history_docket_type_active ON insurance_history(docket_number, ins_type_desc, cancl_effective_date);
CREATE INDEX IF NOT EXISTS idx_insurance_history_effective_date ON insurance_history(effective_date);
CREATE INDEX IF NOT EXISTS idx_insurance_history_max_cov_amount ON insurance_history(max_cov_amount);

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
    total_pwr TEXT,
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

CREATE INDEX IF NOT EXISTS idx_new_ventures_dot_number ON new_ventures(dot_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_docket_number ON new_ventures(docket_number);
CREATE INDEX IF NOT EXISTS idx_new_ventures_add_date ON new_ventures(add_date);
CREATE INDEX IF NOT EXISTS idx_new_ventures_name ON new_ventures(name);
CREATE INDEX IF NOT EXISTS idx_new_ventures_phy_st ON new_ventures(phy_st);
CREATE INDEX IF NOT EXISTS idx_new_ventures_operating_status ON new_ventures(operating_status);
CREATE INDEX IF NOT EXISTS idx_new_ventures_created_at ON new_ventures(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_new_ventures_email ON new_ventures(email_address);
CREATE INDEX IF NOT EXISTS idx_new_ventures_hm_ind ON new_ventures(hm_ind);
CREATE INDEX IF NOT EXISTS idx_new_ventures_carrier_op ON new_ventures(carrier_operation);

CREATE INDEX IF NOT EXISTS idx_new_ventures_name_trgm ON new_ventures USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_name_dba_trgm ON new_ventures USING gin (name_dba gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_dot_trgm ON new_ventures USING gin (dot_number gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_new_ventures_docket_trgm ON new_ventures USING gin (docket_number gin_trgm_ops);

CREATE OR REPLACE FUNCTION update_new_ventures_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_new_ventures_updated_at ON new_ventures;
CREATE TRIGGER update_new_ventures_updated_at BEFORE UPDATE ON new_ventures
    FOR EACH ROW EXECUTE FUNCTION update_new_ventures_updated_at();

INSERT INTO users (user_id, name, email, role, plan, daily_limit, records_extracted_today, ip_address, is_online, is_blocked)
VALUES ('1', 'Admin User', 'wooohan3@gmail.com', 'admin', 'Enterprise', 100000, 0, '192.168.1.1', false, false)
ON CONFLICT (email) DO NOTHING;
"""


async def connect_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
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


_CARGO_MAP = {
    "General Freight":              "crgo_genfreight",
    "Household Goods":              "crgo_household",
    "Metal: Sheets, Coils, Rolls":  "crgo_metalsheet",
    "Motor Vehicles":               "crgo_motoveh",
    "Drive/Tow Away":               "crgo_drivetow",
    "Logs, Poles, Beams, Lumber":   "crgo_logpole",
    "Building Materials":           "crgo_bldgmat",
    "Mobile Homes":                 "crgo_mobilehome",
    "Machinery, Large Objects":     "crgo_machlrg",
    "Fresh Produce":                "crgo_produce",
    "Liquids/Gases":                "crgo_liqgas",
    "Intermodal Cont.":             "crgo_intermodal",
    "Passengers":                   "crgo_passengers",
    "Oilfield Equipment":           "crgo_oilfield",
    "Livestock":                    "crgo_livestock",
    "Grain, Feed, Hay":             "crgo_grainfeed",
    "Coal/Coke":                    "crgo_coalcoke",
    "Meat":                         "crgo_meat",
    "Garbage/Refuse":               "crgo_garbage",
    "US Mail":                      "crgo_usmail",
    "Chemicals":                    "crgo_chem",
    "Commodities Dry Bulk":         "crgo_drybulk",
    "Refrigerated Food":            "crgo_coldfood",
    "Beverages":                    "crgo_beverages",
    "Paper Products":               "crgo_paperprod",
    "Utilities":                    "crgo_utility",
    "Agricultural/Farm Supplies":   "crgo_farmsupp",
    "Construction":                 "crgo_construct",
    "Water Well":                   "crgo_waterwell",
    "Other":                        "crgo_cargoothr",
}

_CLASSIFICATION_KEYWORDS = {
    "Auth. For Hire":     "AUTHORIZED FOR HIRE",
    "Exempt For Hire":    "EXEMPT FOR HIRE",
    "Private(Property)":  "PRIVATE (PROPERTY)",
    "Private(Passenger)": "PRIVATE (PASSENGER)",
    "Migrant":            "MIGRANT",
    "U.S. Mail":          "U. S. MAIL",
    "Federal Government": "FEDERAL GOVERNMENT",
    "State Government":   "STATE GOVERNMENT",
    "Local Government":   "LOCAL GOVERNMENT",
    "Indian Tribe":       "INDIAN TRIBE",
}

_CARRIER_OP_CODE = {
    "Interstate":               "A",
    "Intrastate Only (HM)":     "B",
    "Intrastate Only (Non-HM)": "C",
    "A": "A", "B": "B", "C": "C",
}

_CARRIER_OP_LABEL = {
    "A": "Interstate",
    "B": "Intrastate Only (HM)",
    "C": "Intrastate Only (Non-HM)",
}

_STATUS_LABEL = {"A": "Authorized", "I": "Inactive", "P": "Pending"}


def _carrier_row_to_dict(row) -> dict:
    d = dict(row)

    mc_number = None
    for i in ("1", "2", "3"):
        if d.get(f"docket{i}prefix") == "MC":
            mc_number = d.get(f"docket{i}")
            break

    for i in ("1", "2", "3"):
        pfx = d.get(f"docket{i}prefix") or ""
        num = d.get(f"docket{i}") or ""
        d[f"docket{i}"] = f"{pfx}{num}" if pfx and num else (num or None)

    physical_address = ", ".join(filter(None, [
        d.get("phy_street"), d.get("phy_city"),
        d.get("phy_state"), d.get("phy_zip"), d.get("phy_country"),
    ]))
    mailing_address = ", ".join(filter(None, [
        d.get("carrier_mailing_street"), d.get("carrier_mailing_city"),
        d.get("carrier_mailing_state"), d.get("carrier_mailing_zip"),
        d.get("carrier_mailing_country"),
    ]))

    cargo_carried = [label for label, col in _CARGO_MAP.items() if d.get(col) == "X"]

    classdef = d.get("classdef") or ""
    operation_classification = [s.strip() for s in classdef.split(";") if s.strip()]

    op_code = d.get("carrier_operation")
    carrier_operation = [_CARRIER_OP_LABEL.get(op_code, op_code)] if op_code else []

    status_code = d.get("status_code")
    status = _STATUS_LABEL.get(status_code, status_code)

    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])

    return {
        "id": d.get("id"),
        "mc_number": mc_number,
        "dot_number": d.get("dot_number"),
        "legal_name": d.get("legal_name"),
        "dba_name": d.get("dba_name"),
        "entity_type": d.get("business_org_desc"),
        "status": status,
        "status_code": status_code,
        "email": d.get("email_address"),
        "phone": d.get("phone") or d.get("cell_phone"),
        "fax": d.get("fax"),
        "power_units": d.get("power_units"),
        "drivers": d.get("total_drivers"),
        "fleetsize": d.get("fleetsize"),
        "total_cdl": d.get("total_cdl"),
        "physical_address": physical_address,
        "mailing_address": mailing_address,
        "phy_street": d.get("phy_street"),
        "phy_city": d.get("phy_city"),
        "phy_state": d.get("phy_state"),
        "phy_zip": d.get("phy_zip"),
        "phy_country": d.get("phy_country"),
        "mcs150_date": d.get("mcs150_date"),
        "mcs150_mileage": d.get("mcs150_mileage"),
        "mcs150_mileage_year": d.get("mcs150_mileage_year"),
        "operation_classification": operation_classification,
        "carrier_operation": carrier_operation,
        "cargo_carried": cargo_carried,
        "safety_rating": d.get("safety_rating"),
        "safety_rating_date": d.get("safety_rating_date"),
        "hm_ind": d.get("hm_ind"),
        "company_officer_1": d.get("company_officer_1"),
        "company_officer_2": d.get("company_officer_2"),
        "add_date": d.get("add_date"),
        "docket1": d.get("docket1"),
        "docket2": d.get("docket2"),
        "docket3": d.get("docket3"),
        "docket1_status_code": d.get("docket1_status_code"),
        "docket2_status_code": d.get("docket2_status_code"),
        "docket3_status_code": d.get("docket3_status_code"),
        "review_type": d.get("review_type"),
        "review_date": d.get("review_date"),
        "recordable_crash_rate": d.get("recordable_crash_rate"),
        "crgo_cargoothr_desc": d.get("crgo_cargoothr_desc"),
    }


async def fetch_carriers(filters: dict) -> dict:
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("dot_number"):
        clean = "".join(c for c in filters["dot_number"] if c.isdigit())
        conditions.append(f"dot_number = ${idx}")
        params.append(clean)
        idx += 1

    if filters.get("mc_number"):
        clean = "".join(c for c in filters["mc_number"] if c.isdigit())
        conditions.append(f"(docket1 = ${idx} OR docket2 = ${idx} OR docket3 = ${idx})")
        params.append(clean)
        idx += 1

    if filters.get("legal_name"):
        conditions.append(f"legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    if filters.get("state"):
        states = [s.strip().upper() for s in filters["state"].split("|") if s.strip()]
        placeholders = ", ".join(f"${idx + i}" for i in range(len(states)))
        conditions.append(f"phy_state IN ({placeholders})")
        params.extend(states)
        idx += len(states)

    active = filters.get("active")
    if active == "true":
        conditions.append("status_code = 'A'")
    elif active == "false":
        conditions.append("status_code != 'A'")

    if filters.get("entity_type"):
        conditions.append(f"business_org_desc ILIKE ${idx}")
        params.append(f"%{filters['entity_type']}%")
        idx += 1

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("(email_address IS NOT NULL AND email_address != '')")
    elif has_email == "false":
        conditions.append("(email_address IS NULL OR email_address = '')")

    has_company_rep = filters.get("has_company_rep")
    if has_company_rep == "true":
        conditions.append("(company_officer_1 IS NOT NULL AND company_officer_1 != '')")
    elif has_company_rep == "false":
        conditions.append("(company_officer_1 IS NULL OR company_officer_1 = '')")

    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("hm_ind = 'Y'")
    elif hazmat == "false":
        conditions.append("hm_ind = 'N'")

    if filters.get("carrier_operation"):
        _cop = filters["carrier_operation"]
        ops_raw = [o.strip() for o in (_cop if isinstance(_cop, list) else _cop.split(","))]
        op_codes = [_CARRIER_OP_CODE.get(o, o) for o in ops_raw if _CARRIER_OP_CODE.get(o, o) in ("A", "B", "C")]
        if op_codes:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(op_codes)))
            conditions.append(f"carrier_operation IN ({placeholders})")
            params.extend(op_codes)
            idx += len(op_codes)

    if filters.get("classification"):
        _cls = filters["classification"]
        classes = [c.strip() for c in (_cls if isinstance(_cls, list) else _cls.split(",")) if c.strip()]
        class_conds = []
        for cls in classes:
            mapped = _CLASSIFICATION_KEYWORDS.get(cls, cls)
            class_conds.append(f"classdef ILIKE ${idx}")
            params.append(f"%{mapped}%")
            idx += 1
        if class_conds:
            conditions.append(f"({' OR '.join(class_conds)})")

    if filters.get("cargo"):
        _crg = filters["cargo"]
        cargo_types = [c.strip() for c in (_crg if isinstance(_crg, list) else _crg.split(",")) if c.strip()]
        cargo_conds = []
        for cargo in cargo_types:
            col = _CARGO_MAP.get(cargo)
            if col:
                cargo_conds.append(f"{col} = 'X'")
        if cargo_conds:
            conditions.append(f"({' OR '.join(cargo_conds)})")

    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(power_units, '')::int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(power_units, '')::int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1

    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(total_drivers, '')::int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(total_drivers, '')::int <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    if filters.get("years_in_business_min"):
        cutoff = _date.today().replace(year=_date.today().year - int(filters["years_in_business_min"]))
        cutoff_str = cutoff.strftime("%Y%m%d")
        conditions.append(
            f"(mcs150_date IS NOT NULL AND mcs150_date != '' "
            f"AND SUBSTRING(mcs150_date, 1, 8) <= ${idx})"
        )
        params.append(cutoff_str)
        idx += 1
    if filters.get("years_in_business_max"):
        cutoff = _date.today().replace(year=_date.today().year - int(filters["years_in_business_max"]))
        cutoff_str = cutoff.strftime("%Y%m%d")
        conditions.append(
            f"(mcs150_date IS NOT NULL AND mcs150_date != '' "
            f"AND SUBSTRING(mcs150_date, 1, 8) >= ${idx})"
        )
        params.append(cutoff_str)
        idx += 1

    if filters.get("safety_rating"):
        conditions.append(f"safety_rating ILIKE ${idx}")
        params.append(f"%{filters['safety_rating']}%")
        idx += 1

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    is_filtered = len(conditions) > 0
    limit_val = min(int(filters.get("limit", 200)), 10000)
    offset_val = int(filters.get("offset", 0))

    data_query = f"""
        SELECT
            id, dot_number,
            docket1prefix, docket1, docket2prefix, docket2, docket3prefix, docket3,
            legal_name, dba_name, business_org_desc,
            status_code, carrier_operation, classdef,
            phone, cell_phone, fax, email_address,
            power_units, total_drivers, fleetsize, total_cdl,
            phy_street, phy_city, phy_state, phy_zip, phy_country,
            carrier_mailing_street, carrier_mailing_city, carrier_mailing_state,
            carrier_mailing_zip, carrier_mailing_country,
            mcs150_date, mcs150_mileage, mcs150_mileage_year,
            safety_rating, safety_rating_date, hm_ind,
            company_officer_1, company_officer_2, add_date,
            crgo_genfreight, crgo_household, crgo_metalsheet, crgo_motoveh,
            crgo_drivetow, crgo_logpole, crgo_bldgmat, crgo_mobilehome,
            crgo_machlrg, crgo_produce, crgo_liqgas, crgo_intermodal,
            crgo_passengers, crgo_oilfield, crgo_livestock, crgo_grainfeed,
            crgo_coalcoke, crgo_meat, crgo_garbage, crgo_usmail,
            crgo_chem, crgo_drybulk, crgo_coldfood, crgo_beverages,
            crgo_paperprod, crgo_utility, crgo_farmsupp, crgo_construct,
            crgo_waterwell, crgo_cargoothr, crgo_cargoothr_desc,
            docket1_status_code, docket2_status_code, docket3_status_code,
            review_type, review_date, recordable_crash_rate
        FROM carriers
        {where}
        ORDER BY legal_name
        LIMIT {limit_val} OFFSET {offset_val}
    """

    if not is_filtered:
        count_query = "SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'"
        count_params: list = []
    else:
        count_query = f"SELECT COUNT(*) AS cnt FROM carriers {where}"
        count_params = params

    try:
        async with pool.acquire() as conn:
            # FIX: Use transaction block for SET LOCAL and execute sequentially
            async with conn.transaction():
                await conn.execute("SET LOCAL work_mem = '128MB'")
                rows = await conn.fetch(data_query, *params)
                count_row = await conn.fetchrow(count_query, *count_params)
                
        filtered_count = count_row["cnt"] if count_row else 0
        return {
            "data": [_carrier_row_to_dict(row) for row in rows],
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return {"data": [], "filtered_count": 0}


async def delete_carrier(mc_number: str) -> bool:
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
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            "SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'"
        )
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting carrier count: {e}")
        return 0


async def get_carrier_dashboard_stats() -> dict:
    pool = get_pool()
    try:
        row = await pool.fetchrow("""
            SELECT
                reltuples::bigint AS total
            FROM pg_class WHERE relname = 'carriers'
        """)
        total = row["total"] if row else 0

        stats_row = await pool.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE status_code = 'A') AS active_carriers,
                COUNT(*) FILTER (WHERE status_code != 'A') AS not_authorized,
                COUNT(*) FILTER (WHERE email_address IS NOT NULL AND email_address != '') AS with_email,
                COUNT(*) FILTER (WHERE safety_rating IS NOT NULL AND safety_rating != '') AS with_safety_rating
            FROM carriers
        """)
        if not stats_row:
            return {
                "total": total, "active_carriers": 0, "brokers": 0,
                "with_email": 0, "email_rate": "0",
                "with_safety_rating": 0, "with_insurance": 0,
                "with_inspections": 0, "with_crashes": 0,
                "not_authorized": 0, "other": 0,
            }
        active = stats_row["active_carriers"]
        not_auth = stats_row["not_authorized"]
        with_email = stats_row["with_email"]
        email_rate = f"{(with_email / total * 100):.1f}" if total > 0 else "0"
        return {
            "total": total,
            "active_carriers": active,
            "brokers": 0,
            "with_email": with_email,
            "email_rate": email_rate,
            "with_safety_rating": stats_row["with_safety_rating"],
            "with_insurance": 0,
            "with_inspections": 0,
            "with_crashes": 0,
            "not_authorized": not_auth,
            "other": total - active - not_auth,
        }
    except Exception as e:
        print(f"[DB] Error getting dashboard stats: {e}")
        return {
            "total": 0, "active_carriers": 0, "brokers": 0,
            "with_email": 0, "email_rate": "0",
            "with_safety_rating": 0, "with_insurance": 0,
            "with_inspections": 0, "with_crashes": 0,
            "not_authorized": 0, "other": 0,
        }


async def update_carrier_safety(dot_number: str, safety_data: dict) -> bool:
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

    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(total_pwr, '')::int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(total_pwr, '')::int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1

    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(total_drivers, '')::int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(total_drivers, '')::int <= ${idx}")
        params.append(int(filters["drivers_max"]))
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


async def fetch_insurance_history(mc_number: str) -> list[dict]:
    pool = get_pool()
    docket = f"MC{mc_number}"
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
            docket,
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
        print(f"[DB] Error fetching insurance history for MC {mc_number}: {e}")
        return []
