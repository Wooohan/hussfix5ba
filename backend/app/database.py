"""Database layer for the FMCSA Carrier Search application.

Handles PostgreSQL connections via asyncpg and provides query functions
for carriers, insurance history, inspections, crashes, safety, FMCSA
register, new ventures, users, and blocked IPs.
"""

import os
import json
import math
import asyncio
import time as _time
from datetime import date as _date
from typing import Optional

import asyncpg

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    import warnings
    warnings.warn("DATABASE_URL is not set. Database connections will fail.")

_pool: Optional[asyncpg.Pool] = None
_DASHBOARD_CACHE_TTL = 7 * 24 * 60 * 60  # 1 week

# In-memory dashboard caches (refreshed after TTL expires)
_dashboard_cache: Optional[dict] = None
_dashboard_cache_ts: float = 0.0
_inspections_dashboard_cache: Optional[dict] = None
_inspections_dashboard_cache_ts: float = 0.0
_crashes_dashboard_cache: Optional[dict] = None
_crashes_dashboard_cache_ts: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Schema SQL (reference only — tables are managed externally)
# ─────────────────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS carriers (
    dot_number              BIGINT,
    dockets                 JSONB,
    legal_name              VARCHAR,
    dba_name                VARCHAR,
    status_code             CHAR(1),
    carrier_operation       CHAR(1),
    carship                 CHAR(1),
    classdef                VARCHAR,
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
    phone                   VARCHAR,
    fax                     VARCHAR,
    cell_phone              VARCHAR,
    email_address           VARCHAR,
    company_officer_1       VARCHAR,
    company_officer_2       VARCHAR,
    phy_street              VARCHAR,
    phy_city                VARCHAR,
    phy_state               VARCHAR,
    phy_zip                 VARCHAR,
    phy_country             VARCHAR,
    phy_cnty                VARCHAR,
    phy_nationality_indicator VARCHAR,
    carrier_mailing_street  VARCHAR,
    carrier_mailing_city    VARCHAR,
    carrier_mailing_state   VARCHAR,
    carrier_mailing_zip     VARCHAR,
    carrier_mailing_country VARCHAR,
    carrier_mailing_cnty    VARCHAR,
    dun_bradstreet_no       VARCHAR,
    driver_inter_total      VARCHAR,
    docket1_status_code     VARCHAR,
    docket2_status_code     VARCHAR,
    docket3_status_code     VARCHAR,
    prior_revoke_flag       VARCHAR,
    prior_revoke_dot_number VARCHAR,
    mcsipdate               VARCHAR,
    interstate_beyond_100_miles  BOOLEAN,
    interstate_within_100_miles  BOOLEAN,
    intrastate_beyond_100_miles  BOOLEAN,
    intrastate_within_100_miles  BOOLEAN,
    cargo                   JSONB,
    other_dockets           JSONB,
    equipment               JSONB
);

CREATE TABLE IF NOT EXISTS insurance_history (
    id                   SERIAL PRIMARY KEY,
    docket_number        VARCHAR(20),
    dot_number           BIGINT,
    ins_form_code        VARCHAR(10),
    ins_type_desc        VARCHAR(50),
    name_company         VARCHAR(100),
    policy_no            VARCHAR(50),
    trans_date           VARCHAR(15),
    underl_lim_amount    VARCHAR(15),
    max_cov_amount       VARCHAR(15),
    effective_date       DATE,
    cancl_effective_date DATE,
    mc_num               BIGINT
);

CREATE TABLE IF NOT EXISTS inspections (
    unique_id            BIGINT,
    report_number        TEXT,
    report_state         TEXT,
    dot_number           BIGINT,
    insp_date            TEXT,
    insp_level_id        BIGINT,
    county_code_state    TEXT,
    time_weight          BIGINT,
    driver_oos_total     BIGINT,
    vehicle_oos_total    BIGINT,
    total_hazmat_sent    BIGINT,
    oos_total            BIGINT,
    hazmat_oos_total     BIGINT,
    hazmat_placard_req   BOOLEAN,
    unit_type_desc       TEXT,
    unit_make            TEXT,
    unit_license         TEXT,
    unit_license_state   TEXT,
    vin                  TEXT,
    unit_decal_number    TEXT,
    unit_type_desc2      TEXT,
    unit_make2           TEXT,
    unit_license2        TEXT,
    unit_license_state2  TEXT,
    vin2                 TEXT,
    unit_decal_number2   TEXT,
    unsafe_insp          BOOLEAN,
    fatigued_insp        BOOLEAN,
    dr_fitness_insp      BOOLEAN,
    subt_alcohol_insp    BOOLEAN,
    vh_maint_insp        BOOLEAN,
    hm_insp              BOOLEAN,
    basic_viol           BIGINT,
    unsafe_viol          BIGINT,
    fatigued_viol        BIGINT,
    dr_fitness_viol      BIGINT,
    subt_alcohol_viol    BIGINT,
    vh_maint_viol        BIGINT,
    hm_viol              DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS crashes (
    report_number              TEXT,
    report_seq_no              INTEGER,
    dot_number                 BIGINT,
    report_date                DATE,
    report_state               TEXT,
    fatalities                 INTEGER,
    injuries                   INTEGER,
    tow_away                   BOOLEAN,
    hazmat_released            BOOLEAN,
    trafficway_desc            TEXT,
    access_control_desc        TEXT,
    road_surface_condition_desc TEXT,
    weather_condition_desc     TEXT,
    light_condition_desc       TEXT,
    vehicle_id_number          TEXT,
    vehicle_license_number     TEXT,
    vehicle_license_state      TEXT,
    severity_weight            DOUBLE PRECISION,
    time_weight                DOUBLE PRECISION,
    citation_issued_desc       TEXT,
    seq_num                    INTEGER,
    not_preventable            BOOLEAN
);

CREATE TABLE IF NOT EXISTS safety (
    dot_number              BIGINT,
    insp_total              INTEGER,
    driver_insp_total       INTEGER,
    driver_oos_insp_total   INTEGER,
    vehicle_insp_total      INTEGER,
    vehicle_oos_insp_total  INTEGER,
    unsafe_driv_insp_w_viol INTEGER,
    unsafe_driv_measure     NUMERIC,
    unsafe_driv_ac          VARCHAR(255),
    hos_driv_insp_w_viol    INTEGER,
    hos_driv_measure        NUMERIC,
    hos_driv_ac             VARCHAR(255),
    driv_fit_insp_w_viol    INTEGER,
    driv_fit_measure        NUMERIC,
    driv_fit_ac             VARCHAR(255),
    contr_subst_insp_w_viol INTEGER,
    contr_subst_measure     NUMERIC,
    contr_subst_ac          VARCHAR(255),
    veh_maint_insp_w_viol   INTEGER,
    veh_maint_measure       NUMERIC,
    veh_maint_ac            VARCHAR(255),
    type                    VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS fmcsa_register (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    number       TEXT NOT NULL,
    title        TEXT NOT NULL,
    decided      TEXT,
    category     TEXT,
    date_fetched TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(number, date_fetched)
);

CREATE TABLE IF NOT EXISTS users (
    id                      UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id                 TEXT NOT NULL UNIQUE,
    name                    TEXT NOT NULL,
    email                   TEXT NOT NULL UNIQUE,
    password_hash           TEXT,
    role                    TEXT NOT NULL DEFAULT 'user'
                            CHECK (role IN ('user', 'admin')),
    plan                    TEXT NOT NULL DEFAULT 'Free'
                            CHECK (plan IN ('Free', 'Starter', 'Pro', 'Enterprise')),
    daily_limit             INTEGER NOT NULL DEFAULT 50,
    records_extracted_today INTEGER NOT NULL DEFAULT 0,
    last_active             TEXT DEFAULT 'Never',
    ip_address              TEXT,
    is_online               BOOLEAN DEFAULT false,
    is_blocked              BOOLEAN DEFAULT false,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS blocked_ips (
    id         UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ip_address TEXT NOT NULL UNIQUE,
    reason     TEXT,
    blocked_at TIMESTAMPTZ DEFAULT NOW(),
    blocked_by TEXT
);

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
    common_stat TEXT, contract_stat TEXT, broker_stat TEXT,
    common_app_pend TEXT, contract_app_pend TEXT, broker_app_pend TEXT,
    common_rev_pend TEXT, contract_rev_pend TEXT, broker_rev_pend TEXT,
    property_chk TEXT, passenger_chk TEXT, hhg_chk TEXT,
    private_auth_chk TEXT, enterprise_chk TEXT,
    operating_status TEXT, operating_status_indicator TEXT,
    phy_str TEXT, phy_city TEXT, phy_st TEXT, phy_zip TEXT,
    phy_country TEXT, phy_cnty TEXT,
    mai_str TEXT, mai_city TEXT, mai_st TEXT, mai_zip TEXT,
    mai_country TEXT, mai_cnty TEXT,
    phy_undeliv TEXT, mai_undeliv TEXT,
    phy_phone TEXT, phy_fax TEXT, mai_phone TEXT, mai_fax TEXT,
    cell_phone TEXT, email_address TEXT,
    company_officer_1 TEXT, company_officer_2 TEXT,
    genfreight TEXT, household TEXT, metalsheet TEXT, motorveh TEXT,
    drivetow TEXT, logpole TEXT, bldgmat TEXT, mobilehome TEXT,
    machlrg TEXT, produce TEXT, liqgas TEXT, intermodal TEXT,
    passengers TEXT, oilfield TEXT, livestock TEXT, grainfeed TEXT,
    coalcoke TEXT, meat TEXT, garbage TEXT, usmail TEXT,
    chem TEXT, drybulk TEXT, coldfood TEXT, beverages TEXT,
    paperprod TEXT, utility TEXT, farmsupp TEXT, construct TEXT,
    waterwell TEXT, cargoothr TEXT, cargoothr_desc TEXT,
    hm_ind TEXT,
    bipd_req TEXT, cargo_req TEXT, bond_req TEXT,
    bipd_file TEXT, cargo_file TEXT, bond_file TEXT,
    owntruck TEXT, owntract TEXT, owntrail TEXT, owncoach TEXT,
    ownschool_1_8 TEXT, ownschool_9_15 TEXT, ownschool_16 TEXT,
    ownbus_16 TEXT, ownvan_1_8 TEXT, ownvan_9_15 TEXT,
    ownlimo_1_8 TEXT, ownlimo_9_15 TEXT, ownlimo_16 TEXT,
    trmtruck TEXT, trmtract TEXT, trmtrail TEXT, trmcoach TEXT,
    trmschool_1_8 TEXT, trmschool_9_15 TEXT, trmschool_16 TEXT,
    trmbus_16 TEXT, trmvan_1_8 TEXT, trmvan_9_15 TEXT,
    trmlimo_1_8 TEXT, trmlimo_9_15 TEXT, trmlimo_16 TEXT,
    trptruck TEXT, trptract TEXT, trptrail TEXT, trpcoach TEXT,
    trpschool_1_8 TEXT, trpschool_9_15 TEXT, trpschool_16 TEXT,
    trpbus_16 TEXT, trpvan_1_8 TEXT, trpvan_9_15 TEXT,
    trplimo_1_8 TEXT, trplimo_9_15 TEXT, trplimo_16 TEXT,
    total_trucks TEXT, total_buses TEXT, total_pwr INTEGER,
    fleetsize TEXT,
    inter_within_100 TEXT, inter_beyond_100 TEXT, total_inter_drivers TEXT,
    intra_within_100 TEXT, intra_beyond_100 TEXT, total_intra_drivers TEXT,
    total_drivers TEXT, avg_tld TEXT, total_cdl TEXT,
    review_type TEXT, review_id TEXT, review_date TEXT,
    recordable_crash_rate TEXT,
    mcs150_mileage TEXT, mcs151_mileage TEXT,
    mcs150_mileage_year TEXT, mcs150_date TEXT,
    safety_rating TEXT, safety_rating_date TEXT,
    arber TEXT, smartway TEXT,
    tia TEXT, tia_phone TEXT, tia_contact_name TEXT,
    tia_tool_free TEXT, tia_fax TEXT, tia_email TEXT, tia_website TEXT,
    phy_ups_store TEXT, mai_ups_store TEXT,
    phy_mail_box TEXT, mai_mail_box TEXT,
    raw_data JSONB,
    scrape_date TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(dot_number, add_date)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_carriers_dot_number    ON carriers (dot_number);
CREATE INDEX IF NOT EXISTS idx_carriers_mc_number     ON carriers (mc_number);
CREATE INDEX IF NOT EXISTS idx_carriers_status_lname  ON carriers (status_code, legal_name);
CREATE INDEX IF NOT EXISTS idx_ih_docket_type         ON insurance_history (docket_number, ins_type_desc);
CREATE INDEX IF NOT EXISTS idx_ih_mc_num              ON insurance_history (mc_num);
CREATE INDEX IF NOT EXISTS idx_ih_effective_date      ON insurance_history (effective_date);
CREATE INDEX IF NOT EXISTS idx_ih_cancl_date          ON insurance_history (cancl_effective_date);
CREATE INDEX IF NOT EXISTS idx_insurance_history_dot   ON insurance_history (dot_number);
CREATE INDEX IF NOT EXISTS idx_inspections_dot_number  ON inspections (dot_number);
CREATE INDEX IF NOT EXISTS idx_crashes_dot_number      ON crashes (dot_number);
CREATE INDEX IF NOT EXISTS idx_safety_dot_number       ON safety (dot_number);

-- Timestamp triggers
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_carriers_updated_at ON carriers;
CREATE TRIGGER trg_carriers_updated_at BEFORE UPDATE ON carriers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_fmcsa_register_updated_at ON fmcsa_register;
CREATE TRIGGER trg_fmcsa_register_updated_at BEFORE UPDATE ON fmcsa_register
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
CREATE TRIGGER trg_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
"""


# ─────────────────────────────────────────────────────────────────────────────
# Lookup maps
# ─────────────────────────────────────────────────────────────────────────────

CARGO_TYPES = [
    "General Freight", "Household Goods", "Metal Sheets", "Motor Vehicles",
    "Drive/Tow away", "Logs, Poles, Beams, Lumber", "Building Materials",
    "Mobile Homes", "Machinery, Large Objects", "Fresh Produce",
    "Liquids/Gases", "Intermodal Containers", "Passengers",
    "Oilfield Equipment", "Livestock", "Grain, Feed, Hay", "Coal/Coke",
    "Meat", "Garbage/Refuse", "US Mail", "Chemicals",
    "Commodities Dry Bulk", "Refrigerated Food", "Beverages",
    "Paper Products", "Utilities", "Agricultural/Farm Supplies",
    "Construction", "Water Well", "Other",
]

_CARRIER_OP_MAP = {"A": "Interstate", "B": "Intrastate Only (HM)", "C": "Intrastate Only (Non-HM)"}
_STATUS_MAP = {"A": "Active", "I": "Inactive", "P": "Pending"}
_DOCKET_STATUS_MAP = {"A": "AUTHORIZED", "I": "NOT AUTHORIZED", "P": "PENDING"}

_INS_TYPE_PATTERN = {"BI&PD": "BIPD%", "CARGO": "CARGO", "BOND": "SURETY", "TRUST FUND": "TRUST FUND"}
_INSURANCE_COMPANY_PATTERNS = {
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


# ─────────────────────────────────────────────────────────────────────────────
# Shared column lists
# ─────────────────────────────────────────────────────────────────────────────

_CARRIER_COLS = """c.dot_number, c.mc_number, c.dockets, c.legal_name, c.dba_name,
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

_INSPECTION_COLS = """i.unique_id, i.report_number, i.report_state, i.dot_number,
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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date(s: str) -> _date:
    """Parse 'YYYY-MM-DD' string to datetime.date for asyncpg."""
    y, m, d = s.split("-")
    return _date(int(y), int(m), int(d))


def _safe_int(value, default=0):
    """Safely convert a value to int, returning default on failure."""
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def _safe_float(value):
    """Return float or None; treat NaN as None."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return float(value)


def _str(value) -> str:
    """Safely convert to string, returning '' for None."""
    return (value or "").strip() if isinstance(value, str) else str(value) if value else ""


def _build_address(street, city, state, zipcode, country="") -> str:
    parts = [p for p in [street, city, state, zipcode] if p]
    addr = ", ".join(parts)
    if country and country != "US":
        addr = f"{addr}, {country}" if addr else country
    return addr


def _format_date(value) -> str:
    """Format a date value to MM/DD/YYYY string."""
    if hasattr(value, "strftime"):
        return value.strftime("%m/%d/%Y")
    return str(value) if value else ""


def _add_range_filter(filters, key, column, conditions, params, idx, cast=int):
    """Add min/max range filters for a given column. Returns updated idx."""
    if filters.get(f"{key}_min") is not None:
        conditions.append(f"{column} >= ${idx}")
        params.append(cast(filters[f"{key}_min"]))
        idx += 1
    if filters.get(f"{key}_max") is not None:
        conditions.append(f"{column} <= ${idx}")
        params.append(cast(filters[f"{key}_max"]))
        idx += 1
    return idx


def _add_bool_filter(filters, key, column, conditions):
    """Add a boolean true/false filter."""
    val = filters.get(key)
    if val == "true":
        conditions.append(f"{column} = true")
    elif val == "false":
        conditions.append(f"{column} = false")


# ─────────────────────────────────────────────────────────────────────────────
# Connection pool
# ─────────────────────────────────────────────────────────────────────────────

async def _init_connection(conn: asyncpg.Connection) -> None:
    """Register JSONB codec so JSONB columns are returned as Python dicts."""
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads,
        schema="pg_catalog", format="text",
    )


async def connect_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=10, init=_init_connection,
    )
    async with _pool.acquire() as conn:
        await conn.execute(_SCHEMA_SQL)


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call connect_db() first.")
    return _pool


# ─────────────────────────────────────────────────────────────────────────────
# Insurance history formatting
# ─────────────────────────────────────────────────────────────────────────────

def _format_insurance_filing(row) -> dict:
    """Format a single insurance_history row for the API response."""
    raw_amount = _str(row.get("max_cov_amount"))
    try:
        coverage = f"${int(raw_amount) * 1000:,}"
    except (ValueError, TypeError):
        coverage = raw_amount or "N/A"

    eff = row.get("effective_date")
    cancl = row.get("cancl_effective_date")
    return {
        "type": _str(row.get("ins_type_desc")),
        "coverageAmount": coverage,
        "policyNumber": _str(row.get("policy_no")),
        "effectiveDate": eff.isoformat() if eff else "",
        "carrier": _str(row.get("name_company")),
        "formCode": _str(row.get("ins_form_code")),
        "transDate": _str(row.get("trans_date")),
        "underlLimAmount": _str(row.get("underl_lim_amount")),
        "canclEffectiveDate": cancl.isoformat() if cancl else "",
        "status": "Cancelled" if cancl else "Active",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Row-to-dict converters
# ─────────────────────────────────────────────────────────────────────────────

def _carrier_row_to_dict(row) -> dict:
    """Map a carriers row to the API response format."""
    d = dict(row)

    # MC number display
    mc = d.get("mc_number")
    mc_parts = [f"MC-{mc}"] if mc else []
    other = d.get("other_dockets")
    if other and isinstance(other, list):
        mc_parts.extend(str(od) for od in other if od)
    mc_number = ", ".join(mc_parts)

    dot_number = str(d.get("dot_number") or "")

    # Addresses
    physical_address = _build_address(
        d.get("phy_street") or "", d.get("phy_city") or "",
        d.get("phy_state") or "", d.get("phy_zip") or "",
        d.get("phy_country") or "",
    )
    mailing_address = _build_address(
        d.get("carrier_mailing_street") or "", d.get("carrier_mailing_city") or "",
        d.get("carrier_mailing_state") or "", d.get("carrier_mailing_zip") or "",
        d.get("carrier_mailing_country") or "",
    )

    # Cargo from JSONB (auto-decoded by asyncpg JSONB codec)
    cargo = d.get("cargo")
    cargo_carried = []
    if cargo and isinstance(cargo, dict):
        other_desc = cargo.get("crgo_cargoothr_desc")
        for key, val in cargo.items():
            if key == "crgo_cargoothr_desc":
                continue
            if val and str(val).strip().upper() == "X":
                cargo_carried.append(key)
        if other_desc and str(other_desc).strip():
            if "Other" in cargo_carried:
                cargo_carried.remove("Other")
            cargo_carried.append(str(other_desc).strip())

    # Operation & classification
    op_code = _str(d.get("carrier_operation"))
    carrier_operation = [_CARRIER_OP_MAP.get(op_code, op_code)] if op_code else []
    classdef = d.get("classdef") or ""
    classification = [c.strip() for c in classdef.split(";")] if classdef else []

    # Status
    status_code = _str(d.get("status_code"))
    docket_code = _str(d.get("docket1_status_code"))

    # Territory
    territory = []
    if d.get("interstate_beyond_100_miles"):
        territory.append("Interstate (>100 mi)")
    if d.get("interstate_within_100_miles"):
        territory.append("Interstate (<100 mi)")
    if d.get("intrastate_beyond_100_miles"):
        territory.append("Intrastate (>100 mi)")
    if d.get("intrastate_within_100_miles"):
        territory.append("Intrastate (<100 mi)")

    # MCS150
    mileage = d.get("mcs150_mileage")
    mileage_year = d.get("mcs150_mileage_year")
    mcs150_mileage = f"{mileage} ({mileage_year})" if mileage and mileage_year else str(mileage or "")

    hm_val = d.get("hm_ind")
    equipment = d.get("equipment")

    return {
        "id": dot_number,
        "mc_number": mc_number,
        "dot_number": dot_number,
        "legal_name": d.get("legal_name") or "",
        "dba_name": d.get("dba_name") or "",
        "entity_type": "BROKER" if "broker" in (d.get("classdef") or "").lower() else "CARRIER",
        "status": _STATUS_MAP.get(status_code, status_code),
        "status_code": status_code,
        "authority_status": _DOCKET_STATUS_MAP.get(docket_code, "NOT AUTHORIZED"),
        "email": d.get("email_address") or "",
        "phone": d.get("phone") or "",
        "fax": d.get("fax") or "",
        "power_units": d.get("power_units") or "",
        "drivers": d.get("total_drivers") or "",
        "physical_address": physical_address,
        "mailing_address": mailing_address,
        "phy_state": d.get("phy_state") or "",
        "mcs150_date": _format_date(d.get("mcs150_date")),
        "mcs150_mileage": mcs150_mileage,
        "operation_classification": classification,
        "carrier_operation": carrier_operation,
        "cargo_carried": cargo_carried,
        "hm_ind": "Y" if hm_val is True else ("N" if hm_val is False else ""),
        "duns_number": d.get("dun_bradstreet_no") or "",
        "safety_rating": "",
        "safety_rating_date": "",
        "operating_territory": territory,
        "company_officer_1": d.get("company_officer_1") or "",
        "company_officer_2": d.get("company_officer_2") or "",
        "fleetsize": d.get("fleetsize") or "",
        "add_date": _format_date(d.get("add_date")),
        "truck_units": d.get("truck_units") or "",
        "bus_units": "",
        "equipment": equipment if isinstance(equipment, dict) else {},
        "basic_scores": None,
        "oos_rates": None,
        "insurance_policies": None,
        "inspections": None,
        "crashes": None,
        "insurance_history_filings": [],
    }


def _inspection_row_to_dict(row) -> dict:
    """Convert an inspections row to a dict with both frontend and raw fields."""
    d = dict(row)

    # Violation counts
    basic_viol = _safe_int(d.get("basic_viol"))
    unsafe_viol = _safe_int(d.get("unsafe_viol"))
    fatigued_viol = _safe_int(d.get("fatigued_viol"))
    dr_fitness_viol = _safe_int(d.get("dr_fitness_viol"))
    subt_alcohol_viol = _safe_int(d.get("subt_alcohol_viol"))
    vh_maint_viol = _safe_int(d.get("vh_maint_viol"))
    hm_viol = _safe_int(_safe_float(d.get("hm_viol")))
    total_violations = basic_viol + unsafe_viol + fatigued_viol + dr_fitness_viol + subt_alcohol_viol + vh_maint_viol + hm_viol

    # Violation list for frontend
    violation_list = []
    for count, label, desc in [
        (unsafe_viol, "Unsafe Driving", "Unsafe driving violations"),
        (fatigued_viol, "HOS / Fatigued Driving", "Hours of service / fatigued driving violations"),
        (dr_fitness_viol, "Driver Fitness", "Driver fitness violations"),
        (subt_alcohol_viol, "Controlled Substances / Alcohol", "Controlled substances / alcohol violations"),
        (vh_maint_viol, "Vehicle Maintenance", "Vehicle maintenance violations"),
        (hm_viol, "Hazardous Materials", "Hazardous materials violations"),
    ]:
        for _ in range(count):
            violation_list.append({"label": label, "description": desc, "weight": 1})

    # Location
    state = d.get("report_state") or ""
    county = d.get("county_code_state") or ""
    location = f"{county}, {state}" if county and county != state else state

    oos_total = _safe_int(d.get("oos_total"))
    driver_oos = _safe_int(d.get("driver_oos_total"))
    vehicle_oos = _safe_int(d.get("vehicle_oos_total"))
    hazmat_oos = _safe_int(d.get("hazmat_oos_total"))

    return {
        "reportNumber": d.get("report_number") or "",
        "date": d.get("insp_date") or "",
        "location": location,
        "oosViolations": oos_total,
        "driverViolations": driver_oos,
        "vehicleViolations": vehicle_oos,
        "hazmatViolations": hazmat_oos,
        "violationList": violation_list,
        "totalViolations": total_violations,
        "unique_id": d.get("unique_id"),
        "report_number": d.get("report_number"),
        "report_state": state,
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
        "hm_viol": _safe_float(d.get("hm_viol")),
    }


def _crash_row_to_dict(row) -> dict:
    """Convert a crashes row to a JSON-serializable dict."""
    d = dict(row)
    report_date = d.get("report_date")
    if report_date is not None:
        d["report_date"] = str(report_date)
    for col in ("severity_weight", "time_weight"):
        d[col] = _safe_float(d.get(col))
    return d


def _user_row_to_dict(row) -> dict:
    d = dict(row)
    d.pop("password_hash", None)
    for key in ("created_at", "updated_at", "blocked_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


def _new_venture_row_to_dict(row) -> dict:
    d = dict(row)
    raw = d.get("raw_data")
    if raw and isinstance(raw, str):
        try:
            d["raw_data"] = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            pass
    for key in ("created_at", "updated_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "id" in d and d["id"] is not None:
        d["id"] = str(d["id"])
    return d


# ═════════════════════════════════════════════════════════════════════════════
# Carrier CRUD
# ═════════════════════════════════════════════════════════════════════════════

async def upsert_carrier(record: dict) -> bool:
    pool = get_pool()
    dot = record.get("dot_number") or record.get("dot")
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


async def delete_carrier(dot_number: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute(
            "DELETE FROM carriers WHERE dot_number = $1", int(dot_number.strip())
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting carrier DOT {dot_number}: {e}")
        return False


async def update_carrier_insurance(dot_number: str, policies: list) -> bool:
    """No-op — insurance is managed via the insurance_history table."""
    return True


async def update_carrier_safety(dot_number: str, safety_data: dict) -> bool:
    """No-op — safety data is managed via the safety table."""
    return True


async def get_carrier_count() -> int:
    """Fast estimated count using pg_class (instant on large tables)."""
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'"
    )
    return row["cnt"] if row else 0


async def get_carriers_by_mc_range(start_mc: str, end_mc: str) -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            f"SELECT {_CARRIER_COLS} FROM carriers c "
            "WHERE mc_number IS NOT NULL AND mc_number BETWEEN $1 AND $2 "
            "ORDER BY mc_number LIMIT 1000",
            int(start_mc), int(end_mc),
        )
        return [_carrier_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching MC range: {e}")
        return []


# ═════════════════════════════════════════════════════════════════════════════
# Carrier search (main endpoint)
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_carriers(filters: dict) -> dict:
    """Fetch carriers with optional filters.

    Uses CTE+JOIN for insurance/safety filters (hash joins via nestloop=off)
    and runs the main query + COUNT in parallel for fast responses.
    """
    pool = get_pool()
    conditions: list[str] = []
    params: list = []
    idx = 1

    # ── Direct carrier-table filters ─────────────────────────────────────

    if filters.get("mc_number"):
        mc_raw = filters["mc_number"].strip().upper()
        mc_num = mc_raw
        for pfx in ("MC", "MX", "FF"):
            if mc_raw.startswith(pfx):
                mc_num = mc_raw[len(pfx):].lstrip("-").strip()
                break
        try:
            conditions.append(f"c.mc_number = ${idx}")
            params.append(int(mc_num))
            idx += 1
        except ValueError:
            conditions.append(f"c.mc_number::text = ${idx}")
            params.append(mc_num)
            idx += 1

    if filters.get("dot_number"):
        dot_val = filters["dot_number"].strip()
        try:
            conditions.append(f"c.dot_number = ${idx}")
            params.append(int(dot_val))
            idx += 1
        except ValueError:
            conditions.append(f"c.dot_number::text = ${idx}")
            params.append(dot_val)
            idx += 1

    if filters.get("legal_name"):
        conditions.append(f"c.legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    active = filters.get("active")
    if active == "true":
        conditions.append("c.docket1_status_code = 'A'")
    elif active == "false":
        conditions.append("(c.docket1_status_code IS NULL OR c.docket1_status_code != 'A')")

    if filters.get("years_in_business_min"):
        conditions.append(f"c.add_date IS NOT NULL AND c.add_date <= CURRENT_DATE - make_interval(years => ${idx})")
        params.append(int(filters["years_in_business_min"]))
        idx += 1
    if filters.get("years_in_business_max"):
        conditions.append(f"c.add_date IS NOT NULL AND c.add_date >= CURRENT_DATE - make_interval(years => ${idx})")
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

    entity_type = filters.get("entity_type")
    if entity_type:
        if entity_type.upper() == "BROKER":
            conditions.append("c.classdef ILIKE '%broker%'")
        elif entity_type.upper() == "CARRIER":
            conditions.append("(c.classdef IS NULL OR c.classdef NOT ILIKE '%broker%')")

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

    idx = _add_range_filter(filters, "power_units", "c.power_units", conditions, params, idx)
    idx = _add_range_filter(filters, "drivers", "c.total_drivers", conditions, params, idx)

    # ── CTE+JOIN filters (safety & insurance) ────────────────────────────
    cte_idx = 0
    ctes: list[tuple[str, str]] = []
    joins: list[str] = []

    def add_safety_cte(name, table, having, join_col="dot_number"):
        ctes.append((name, f"SELECT {join_col} FROM {table} GROUP BY {join_col} HAVING {having}"))
        joins.append(f"INNER JOIN {name} ON {name}.{join_col} = c.{join_col}")

    def add_insurance_cte(where_body, extra_where=""):
        nonlocal cte_idx
        name = f"_ifd{cte_idx}"
        extra = f" AND {extra_where}" if extra_where else ""
        ctes.append((name, f"SELECT DISTINCT mc_num FROM insurance_history WHERE mc_num IS NOT NULL AND ({where_body}){extra}"))
        joins.append(f"INNER JOIN {name} ON {name}.mc_num = c.mc_number")
        cte_idx += 1

    # OOS violations
    _oos_conds = []
    idx = _add_range_filter(filters, "oos", "SUM(oos_total)", _oos_conds, params, idx)
    if _oos_conds:
        add_safety_cte("_oos_f", "inspections", " AND ".join(_oos_conds))

    # Inspections count
    _insp_conds = []
    idx = _add_range_filter(filters, "inspections", "COUNT(*)", _insp_conds, params, idx)
    if _insp_conds:
        add_safety_cte("_insp_f", "inspections", " AND ".join(_insp_conds))

    # Crashes count
    _crash_conds = []
    idx = _add_range_filter(filters, "crashes", "COUNT(*)", _crash_conds, params, idx)
    if _crash_conds:
        add_safety_cte("_crash_f", "crashes", " AND ".join(_crash_conds))

    # Injuries
    _inj_conds = []
    idx = _add_range_filter(filters, "injuries", "SUM(injuries)", _inj_conds, params, idx)
    if _inj_conds:
        add_safety_cte("_injuries_f", "crashes", " AND ".join(_inj_conds))

    # Fatalities
    _fat_conds = []
    idx = _add_range_filter(filters, "fatalities", "SUM(fatalities)", _fat_conds, params, idx)
    if _fat_conds:
        add_safety_cte("_fatalities_f", "crashes", " AND ".join(_fat_conds))

    # Towaway
    _tow_conds = []
    idx = _add_range_filter(filters, "toway", "COUNT(*) FILTER (WHERE tow_away = true)", _tow_conds, params, idx)
    if _tow_conds:
        add_safety_cte("_toway_f", "crashes", " AND ".join(_tow_conds))

    # Insurance required
    _ACTIVE_POLICY = "(cancl_effective_date IS NULL)"

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
        add_insurance_cte(" OR ".join(or_parts))

    # BIPD / Cargo / Bond / Trust Fund on-file
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
            add_insurance_cte(f"ins_type_desc {op} ${idx} AND {_ACTIVE_POLICY}")
            params.append(pattern_val)
            idx += 1
        elif val == "0":
            conditions.append(
                f"NOT EXISTS (SELECT 1 FROM insurance_history ih "
                f"WHERE ih.mc_num = c.mc_number "
                f"AND ih.ins_type_desc {op} ${idx} "
                f"AND ih.cancl_effective_date IS NULL)"
            )
            params.append(pattern_val)
            idx += 1

    # BIPD amount range
    _bipd_conds = []
    if filters.get("bipd_min"):
        raw_min = int(filters["bipd_min"])
        compare_min = raw_min // 1000 if raw_min >= 10000 else raw_min
        _bipd_conds.append(f"NULLIF(REPLACE(max_cov_amount, ',', ''), '')::numeric >= ${idx}")
        params.append(compare_min)
        idx += 1
    if filters.get("bipd_max"):
        raw_max = int(filters["bipd_max"])
        compare_max = raw_max // 1000 if raw_max >= 10000 else raw_max
        _bipd_conds.append(f"NULLIF(REPLACE(max_cov_amount, ',', ''), '')::numeric <= ${idx}")
        params.append(compare_max)
        idx += 1
    if _bipd_conds:
        add_insurance_cte(" AND ".join(_bipd_conds))

    # Effective date range
    _eff_conds = []
    if filters.get("ins_effective_date_from"):
        _eff_conds.append(f"effective_date >= ${idx}")
        params.append(_parse_date(filters["ins_effective_date_from"]))
        idx += 1
    if filters.get("ins_effective_date_to"):
        _eff_conds.append(f"effective_date <= ${idx}")
        params.append(_parse_date(filters["ins_effective_date_to"]))
        idx += 1
    if _eff_conds:
        add_insurance_cte("effective_date IS NOT NULL AND " + " AND ".join(_eff_conds))

    # Cancellation date range
    _cancl_conds = []
    if filters.get("ins_cancellation_date_from"):
        _cancl_conds.append(f"cancl_effective_date >= ${idx}")
        params.append(_parse_date(filters["ins_cancellation_date_from"]))
        idx += 1
    if filters.get("ins_cancellation_date_to"):
        _cancl_conds.append(f"cancl_effective_date <= ${idx}")
        params.append(_parse_date(filters["ins_cancellation_date_to"]))
        idx += 1
    if _cancl_conds:
        add_insurance_cte("cancl_effective_date IS NOT NULL AND " + " AND ".join(_cancl_conds))

    # Insurance company
    if filters.get("insurance_company"):
        companies = filters["insurance_company"]
        if isinstance(companies, str):
            companies = companies.split(",")
        or_parts = []
        for company in companies:
            patterns = _INSURANCE_COMPANY_PATTERNS.get(company.strip().upper(), [f"{company.strip().upper()}%"])
            for pattern in patterns:
                or_parts.append(f"UPPER(name_company) LIKE ${idx}")
                params.append(pattern)
                idx += 1
        add_insurance_cte(
            f"({' OR '.join(or_parts)}) "
            f"AND (cancl_effective_date IS NULL OR cancl_effective_date >= CURRENT_DATE)"
        )

    # Next-renewal-date SQL (computes next anniversary of effective_date)
    _next_renewal_sql = (
        "CASE "
        "  WHEN MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "         EXTRACT(MONTH FROM effective_date)::int,"
        "         LEAST(EXTRACT(DAY FROM effective_date)::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "                 EXTRACT(MONTH FROM effective_date)::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "       >= CURRENT_DATE "
        "  THEN MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "         EXTRACT(MONTH FROM effective_date)::int,"
        "         LEAST(EXTRACT(DAY FROM effective_date)::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int,"
        "                 EXTRACT(MONTH FROM effective_date)::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "  ELSE MAKE_DATE("
        "         EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,"
        "         EXTRACT(MONTH FROM effective_date)::int,"
        "         LEAST(EXTRACT(DAY FROM effective_date)::int,"
        "               EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE("
        "                 EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,"
        "                 EXTRACT(MONTH FROM effective_date)::int, 1))"
        "                 + INTERVAL '1 MONTH - 1 DAY'))::int))"
        "END"
    )

    _ACTIVE_POLICY_GUARD = (
        "effective_date IS NOT NULL "
        "AND (cancl_effective_date IS NULL OR cancl_effective_date >= CURRENT_DATE)"
    )

    # Renewal monthly filter
    if filters.get("renewal_policy_months"):
        months = int(filters["renewal_policy_months"])
        add_insurance_cte(
            f"{_ACTIVE_POLICY_GUARD} AND "
            f"({_next_renewal_sql}) BETWEEN CURRENT_DATE AND "
            f"(DATE_TRUNC('MONTH', CURRENT_DATE + MAKE_INTERVAL(months => ${idx})) + INTERVAL '1 MONTH - 1 DAY')::date"
        )
        params.append(months)
        idx += 1

    # Renewal date range
    _renewal_conds = []
    _renewal_month_pre = ""
    if filters.get("renewal_date_from"):
        _renewal_conds.append(f"({_next_renewal_sql}) >= ${idx}")
        params.append(_parse_date(filters["renewal_date_from"]))
        idx += 1
    if filters.get("renewal_date_to"):
        _renewal_conds.append(f"({_next_renewal_sql}) <= ${idx}")
        params.append(_parse_date(filters["renewal_date_to"]))
        idx += 1
    if _renewal_conds:
        # Month pre-filter to skip ~90% of rows
        if filters.get("renewal_date_from") and filters.get("renewal_date_to"):
            from_d = _parse_date(filters["renewal_date_from"])
            to_d = _parse_date(filters["renewal_date_to"])
            if from_d.month <= to_d.month:
                months = list(range(from_d.month, to_d.month + 1))
            else:
                months = list(range(from_d.month, 13)) + list(range(1, to_d.month + 1))
            _renewal_month_pre = f"EXTRACT(MONTH FROM effective_date) IN ({','.join(str(m) for m in months)})"
        add_insurance_cte(
            f"{_ACTIVE_POLICY_GUARD} AND " + " AND ".join(_renewal_conds),
            extra_where=_renewal_month_pre,
        )

    # ── Build and execute query ──────────────────────────────────────────

    has_cte = bool(ctes)
    is_filtered = len(conditions) > 0 or has_cte
    if not is_filtered:
        conditions.append("c.status_code = 'A'")

    where = " AND ".join(conditions) if conditions else "TRUE"
    limit_val = min(int(filters.get("limit", 500)), 5000)
    offset_val = int(filters.get("offset", 0))

    if has_cte:
        cte_prefix = "WITH " + ", ".join(f"{name} AS ({sql})" for name, sql in ctes)
        from_clause = "carriers c " + " ".join(joins)
    else:
        cte_prefix = ""
        from_clause = "carriers c"

    query = f"""{cte_prefix}
        SELECT {_CARRIER_COLS}
        FROM {from_clause}
        WHERE {where}
        ORDER BY c.legal_name ASC
        LIMIT {limit_val} OFFSET {offset_val}
    """

    # Count: pg_class for unfiltered, exact COUNT for filtered
    fast_count_query = "SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'carriers'"

    try:
        t0 = _time.monotonic()

        if not is_filtered:
            rows, count_row = await asyncio.gather(
                pool.fetch(query, *params),
                pool.fetchrow(fast_count_query),
            )
            filtered_count = count_row["cnt"] if count_row else 0
        else:
            count_query = f"""{cte_prefix}
                SELECT COUNT(*) as cnt FROM {from_clause} WHERE {where}
            """

            async def _run_with_hints(q, conn, fetch_one=False):
                if has_cte:
                    await conn.execute("SET LOCAL enable_nestloop = off")
                if fetch_one:
                    return await conn.fetchrow(q, *params)
                return await conn.fetch(q, *params)

            async with pool.acquire() as c1, pool.acquire() as c2:
                async with c1.transaction(), c2.transaction():
                    rows, count_row = await asyncio.gather(
                        _run_with_hints(query, c1),
                        _run_with_hints(count_query, c2, fetch_one=True),
                    )
            filtered_count = count_row["cnt"] if count_row else 0

        t1 = _time.monotonic()

        # Build carrier dicts
        carrier_dicts = [_carrier_row_to_dict(row) for row in rows]

        t2 = _time.monotonic()

        # Batch-fetch insurance history
        docket_keys = [f"MC{dict(row).get('mc_number')}" if dict(row).get("mc_number") else "" for row in rows]
        unique_keys = list(set(k for k in docket_keys if k))
        if unique_keys:
            try:
                ih_rows = await pool.fetch(
                    """SELECT docket_number, ins_type_desc, max_cov_amount,
                              underl_lim_amount, policy_no, effective_date,
                              ins_form_code, name_company, trans_date,
                              cancl_effective_date
                       FROM insurance_history
                       WHERE docket_number = ANY($1::text[])
                       ORDER BY effective_date DESC""",
                    unique_keys,
                )
                ih_map: dict[str, list[dict]] = {}
                for ih_row in ih_rows:
                    ih_map.setdefault(ih_row["docket_number"], []).append(dict(ih_row))
                for i, carrier in enumerate(carrier_dicts):
                    dk = docket_keys[i]
                    if dk and dk in ih_map:
                        carrier["insurance_history_filings"] = [
                            _format_insurance_filing(r) for r in ih_map[dk]
                        ]
            except Exception as e:
                print(f"[DB] Insurance batch-fetch error: {e}")

        t3 = _time.monotonic()
        print(f"[PERF] query={int((t1-t0)*1000)}ms dict={int((t2-t1)*1000)}ms insurance={int((t3-t2)*1000)}ms total={int((t3-t0)*1000)}ms rows={len(rows)}")

        return {"data": carrier_dicts, "filtered_count": filtered_count}
    except Exception as e:
        print(f"[DB] Error fetching carriers: {e}")
        return {"data": [], "filtered_count": 0}


# ═════════════════════════════════════════════════════════════════════════════
# Dashboard stats
# ═════════════════════════════════════════════════════════════════════════════

async def get_carrier_dashboard_stats() -> dict:
    """Carrier dashboard stats (cached for 1 week)."""
    global _dashboard_cache, _dashboard_cache_ts
    now = _time.time()
    if _dashboard_cache and (now - _dashboard_cache_ts) < _DASHBOARD_CACHE_TTL:
        return _dashboard_cache

    pool = get_pool()

    async def _safe_fetch(sql, label):
        try:
            return await pool.fetchrow(sql)
        except Exception as e:
            print(f"[DB] dashboard '{label}' failed: {e}")
            return None

    async def _safe_fetch_all(sql, label):
        try:
            return await pool.fetch(sql)
        except Exception as e:
            print(f"[DB] dashboard '{label}' failed: {e}")
            return []

    row, insp_row, crash_row, safety_row, insurance_row, monthly_rows = await asyncio.gather(
        _safe_fetch("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status_code = 'A') AS active,
                COUNT(*) FILTER (WHERE email_address IS NOT NULL AND email_address != '') AS with_email,
                COUNT(*) FILTER (WHERE hm_ind = TRUE) AS hazmat,
                COUNT(*) FILTER (WHERE carrier_operation = 'A') AS interstate,
                COUNT(*) FILTER (WHERE carrier_operation = 'B') AS intrastate_hm,
                COUNT(*) FILTER (WHERE carrier_operation = 'C') AS intrastate_non_hm,
                COUNT(*) FILTER (WHERE docket1_status_code = 'A') AS authorized,
                COUNT(*) FILTER (WHERE classdef IS NOT NULL AND classdef ILIKE '%broker%') AS brokers
            FROM carriers
        """, "carriers_agg"),
        _safe_fetch("SELECT COUNT(DISTINCT dot_number) AS cnt FROM inspections", "inspections"),
        _safe_fetch("SELECT COUNT(DISTINCT dot_number) AS cnt FROM crashes", "crashes"),
        _safe_fetch("SELECT COUNT(DISTINCT dot_number) AS cnt FROM safety", "safety"),
        _safe_fetch("SELECT COUNT(DISTINCT dot_number) AS cnt FROM insurance_history WHERE dot_number IS NOT NULL", "insurance"),
        _safe_fetch_all("""
            SELECT TO_CHAR(add_date, 'YYYY-MM') AS month, COUNT(*) AS count
            FROM carriers WHERE add_date IS NOT NULL
            GROUP BY TO_CHAR(add_date, 'YYYY-MM')
            ORDER BY month DESC LIMIT 12
        """, "monthly"),
    )

    d = dict(row) if row else {}
    total = d.get("total") or 0
    active = d.get("active") or 0
    authorized = d.get("authorized") or 0
    brokers = d.get("brokers") or 0
    with_email = d.get("with_email") or 0
    not_authorized = max(total - authorized, 0)

    # Monthly carrier additions (newest first → reverse for chart chronological order)
    monthly = [{"month": r["month"], "count": r["count"]} for r in monthly_rows] if monthly_rows else []
    monthly.reverse()

    # Compute trend: compare latest month to the one before it
    trend_pct = 0.0
    if len(monthly) >= 2:
        prev = monthly[-2]["count"]
        curr = monthly[-1]["count"]
        if prev > 0:
            trend_pct = round((curr - prev) / prev * 100, 1)

    result = {
        "total": total,
        "active_carriers": active,
        "brokers": brokers,
        "with_email": with_email,
        "email_rate": f"{(with_email / total * 100):.1f}" if total else "0",
        "with_safety_rating": (safety_row["cnt"] if safety_row else 0) or 0,
        "with_insurance": (insurance_row["cnt"] if insurance_row else 0) or 0,
        "with_inspections": (insp_row["cnt"] if insp_row else 0) or 0,
        "with_crashes": (crash_row["cnt"] if crash_row else 0) or 0,
        "not_authorized": not_authorized,
        "other": max(total - active - not_authorized, 0),
        "active": active,
        "inactive": max(total - active, 0),
        "withEmail": with_email,
        "hazmat": d.get("hazmat") or 0,
        "interstate": d.get("interstate") or 0,
        "intrastate_hm": d.get("intrastate_hm") or 0,
        "intrastate_non_hm": d.get("intrastate_non_hm") or 0,
        "monthly_additions": monthly,
        "trend_pct": trend_pct,
    }
    if total > 0:
        _dashboard_cache = result
        _dashboard_cache_ts = now
    return result


# ═════════════════════════════════════════════════════════════════════════════
# Insurance history
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_insurance_history(docket_number: str) -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch(
            """SELECT docket_number, dot_number, ins_form_code, ins_type_desc,
                      name_company, policy_no, trans_date, underl_lim_amount,
                      max_cov_amount, effective_date, cancl_effective_date
               FROM insurance_history
               WHERE docket_number = $1
               ORDER BY effective_date DESC""",
            docket_number,
        )
        return [_format_insurance_filing(dict(row)) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching insurance history for {docket_number}: {e}")
        return []


# ═════════════════════════════════════════════════════════════════════════════
# Inspections
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_inspections(filters: dict) -> dict:
    """Fetch inspections with optional filters and pagination."""
    pool = get_pool()
    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("dot_number"):
        conditions.append(f"i.dot_number = ${idx}::bigint")
        params.append(int(filters["dot_number"].strip()))
        idx += 1

    if filters.get("report_number"):
        conditions.append(f"i.report_number ILIKE ${idx}")
        params.append(f"%{filters['report_number']}%")
        idx += 1

    if filters.get("report_state"):
        conditions.append(f"i.report_state = ${idx}")
        params.append(filters["report_state"].upper())
        idx += 1

    if filters.get("insp_date_from"):
        conditions.append(f"i.insp_date >= ${idx}")
        params.append(filters["insp_date_from"])
        idx += 1
    if filters.get("insp_date_to"):
        conditions.append(f"i.insp_date <= ${idx}")
        params.append(filters["insp_date_to"])
        idx += 1

    # Boolean inspection type filters
    for key in ("unsafe_insp", "fatigued_insp", "dr_fitness_insp", "subt_alcohol_insp", "vh_maint_insp", "hm_insp"):
        _add_bool_filter(filters, key, f"i.{key}", conditions)

    # Numeric range filters
    for key, col in [
        ("oos", "i.oos_total"), ("driver_oos", "i.driver_oos_total"),
        ("vehicle_oos", "i.vehicle_oos_total"), ("hazmat_oos", "i.hazmat_oos_total"),
        ("basic_viol", "i.basic_viol"), ("unsafe_viol", "i.unsafe_viol"),
        ("fatigued_viol", "i.fatigued_viol"), ("dr_fitness_viol", "i.dr_fitness_viol"),
        ("subt_alcohol_viol", "i.subt_alcohol_viol"), ("vh_maint_viol", "i.vh_maint_viol"),
    ]:
        idx = _add_range_filter(filters, key, col, conditions, params, idx)

    if filters.get("hm_viol_min") is not None:
        conditions.append(f"i.hm_viol >= ${idx}")
        params.append(float(filters["hm_viol_min"]))
        idx += 1
    if filters.get("hm_viol_max") is not None:
        conditions.append(f"i.hm_viol <= ${idx}")
        params.append(float(filters["hm_viol_max"]))
        idx += 1

    where = " AND ".join(conditions) if conditions else "TRUE"
    limit_val = min(int(filters.get("limit", 500)), 5000)
    offset_val = int(filters.get("offset", 0))

    query = f"SELECT {_INSPECTION_COLS} FROM inspections i WHERE {where} ORDER BY i.insp_date DESC, i.unique_id DESC LIMIT {limit_val} OFFSET {offset_val}"
    count_query = f"SELECT COUNT(*) as cnt FROM inspections i WHERE {where}"

    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(query, *params),
            pool.fetchrow(count_query, *params),
        )
        return {
            "data": [_inspection_row_to_dict(row) for row in rows],
            "filtered_count": count_row["cnt"] if count_row else 0,
        }
    except Exception as e:
        print(f"[DB] Error fetching inspections: {e}")
        return {"data": [], "filtered_count": 0}


async def get_inspections_count() -> int:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM inspections")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting inspections count: {e}")
        return 0


async def get_inspections_dashboard_stats() -> dict:
    """Inspections dashboard stats (cached for 1 week)."""
    global _inspections_dashboard_cache, _inspections_dashboard_cache_ts
    now = _time.time()
    if _inspections_dashboard_cache and (now - _inspections_dashboard_cache_ts) < _DASHBOARD_CACHE_TTL:
        return _inspections_dashboard_cache

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
        _inspections_dashboard_cache = result
        _inspections_dashboard_cache_ts = now
        return result
    except Exception as e:
        print(f"[DB] Error fetching inspections dashboard stats: {e}")
        return _inspections_dashboard_cache or {}


async def fetch_inspection_by_id(unique_id: int) -> dict | None:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT * FROM inspections WHERE unique_id = $1", unique_id)
        return _inspection_row_to_dict(row) if row else None
    except Exception as e:
        print(f"[DB] Error fetching inspection {unique_id}: {e}")
        return None


async def fetch_inspections_by_dot(dot_number: int, limit: int = 10, offset: int = 0) -> dict:
    """Fetch paginated inspections for a DOT number."""
    pool = get_pool()
    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(
                "SELECT * FROM inspections WHERE dot_number = $1 ORDER BY insp_date DESC LIMIT $2 OFFSET $3",
                dot_number, limit, offset,
            ),
            pool.fetchrow(
                "SELECT COUNT(*) AS cnt FROM inspections WHERE dot_number = $1", dot_number,
            ),
        )
        return {
            "data": [_inspection_row_to_dict(row) for row in rows],
            "total": count_row["cnt"] if count_row else 0,
        }
    except Exception as e:
        print(f"[DB] Error fetching inspections for DOT {dot_number}: {e}")
        return {"data": [], "total": 0}


# ═════════════════════════════════════════════════════════════════════════════
# Crashes
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_crashes(filters: dict) -> dict:
    """Fetch crashes with optional filters and pagination."""
    pool = get_pool()
    conditions: list[str] = []
    params: list = []
    idx = 1

    if filters.get("dot_number"):
        try:
            conditions.append(f"dot_number = ${idx}")
            params.append(int(filters["dot_number"].strip()))
            idx += 1
        except ValueError:
            conditions.append(f"dot_number::text = ${idx}")
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

    idx = _add_range_filter(filters, "fatalities", "fatalities", conditions, params, idx)
    idx = _add_range_filter(filters, "injuries", "injuries", conditions, params, idx)

    _add_bool_filter(filters, "tow_away", "tow_away", conditions)
    _add_bool_filter(filters, "not_preventable", "not_preventable", conditions)

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
            citation_issued_desc, seq_num, not_preventable
        FROM crashes WHERE {where}
        ORDER BY report_date DESC NULLS LAST
        LIMIT {limit_val} OFFSET {offset_val}
    """
    count_query = f"SELECT COUNT(*) as cnt FROM crashes WHERE {where}"

    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(query, *params),
            pool.fetchrow(count_query, *params),
        )
        return {
            "data": [_crash_row_to_dict(row) for row in rows],
            "filtered_count": count_row["cnt"] if count_row else 0,
        }
    except Exception as e:
        print(f"[DB] Error fetching crashes: {e}")
        return {"data": [], "filtered_count": 0}


async def get_crashes_count() -> int:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM crashes")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting crashes count: {e}")
        return 0


async def get_crashes_dashboard_stats() -> dict:
    """Crashes dashboard stats (cached for 1 week)."""
    global _crashes_dashboard_cache, _crashes_dashboard_cache_ts
    now = _time.time()
    if _crashes_dashboard_cache and (now - _crashes_dashboard_cache_ts) < _DASHBOARD_CACHE_TTL:
        return _crashes_dashboard_cache

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
        result = {
            "total": row["total"],
            "totalFatalities": int(row["total_fatalities"] or 0),
            "totalInjuries": int(row["total_injuries"] or 0),
            "totalTowaway": row["total_towaway"],
            "totalHazmatReleased": row["total_hazmat_released"],
            "totalNotPreventable": row["total_not_preventable"],
            "uniqueCarriers": row["unique_carriers"],
        }
        _crashes_dashboard_cache = result
        _crashes_dashboard_cache_ts = now
        return result
    except Exception as e:
        print(f"[DB] Error fetching crashes dashboard stats: {e}")
        return _crashes_dashboard_cache or {}


async def fetch_crash_by_report(report_number: str) -> dict | None:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT * FROM crashes WHERE report_number = $1", report_number)
        return _crash_row_to_dict(row) if row else None
    except Exception as e:
        print(f"[DB] Error fetching crash {report_number}: {e}")
        return None


async def fetch_crashes_by_dot(dot_number: str, limit: int = 10, offset: int = 0) -> dict:
    """Fetch paginated crashes for a DOT number."""
    pool = get_pool()
    try:
        dn_int = int(dot_number.strip())
    except (TypeError, ValueError):
        return {"data": [], "total": 0}
    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(
                "SELECT * FROM crashes WHERE dot_number = $1 ORDER BY report_date DESC LIMIT $2 OFFSET $3",
                dn_int, limit, offset,
            ),
            pool.fetchrow(
                "SELECT COUNT(*) AS cnt FROM crashes WHERE dot_number = $1", dn_int,
            ),
        )
        return {
            "data": [_crash_row_to_dict(row) for row in rows],
            "total": count_row["cnt"] if count_row else 0,
        }
    except Exception as e:
        print(f"[DB] Error fetching crashes for DOT {dot_number}: {e}")
        return {"data": [], "total": 0}


# ═════════════════════════════════════════════════════════════════════════════
# Safety
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_safety_by_dot(dot_number: str) -> dict | None:
    """Fetch safety/BASIC scores for a carrier."""
    pool = get_pool()
    try:
        dot_int = int(dot_number.strip())
    except (ValueError, TypeError):
        return None
    try:
        row = await pool.fetchrow("SELECT * FROM safety WHERE dot_number = $1", dot_int)
        if not row:
            return None
        d = dict(row)

        driver_insp = d.get("driver_insp_total") or 0
        driver_oos = d.get("driver_oos_insp_total") or 0
        vehicle_insp = d.get("vehicle_insp_total") or 0
        vehicle_oos = d.get("vehicle_oos_insp_total") or 0
        driver_oos_rate = round((driver_oos / driver_insp) * 100, 1) if driver_insp > 0 else 0.0
        vehicle_oos_rate = round((vehicle_oos / vehicle_insp) * 100, 1) if vehicle_insp > 0 else 0.0

        basic_scores = []
        for key, label in [
            ("unsafe_driv", "Unsafe Driving"),
            ("hos_driv", "HOS Compliance"),
            ("driv_fit", "Driver Fitness"),
            ("contr_subst", "Controlled Substances"),
            ("veh_maint", "Vehicle Maintenance"),
        ]:
            measure = d.get(f"{key}_measure")
            basic_scores.append({
                "category": label,
                "measure": str(round(float(measure), 1)) if measure is not None else "N/A",
                "inspWithViol": d.get(f"{key}_insp_w_viol") or 0,
                "alert": _str(d.get(f"{key}_ac")),
            })

        return {
            "dot_number": str(d.get("dot_number", "")),
            "type": _str(d.get("type")),
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


# ═════════════════════════════════════════════════════════════════════════════
# FMCSA Register
# ═════════════════════════════════════════════════════════════════════════════

async def save_fmcsa_register_entries(entries: list[dict], extracted_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    saved = 0
    skipped = 0
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(entries), 500):
                    batch = entries[i:i + 500]
                    args = [
                        (e["number"], e.get("title", ""), e.get("decided", "N/A"),
                         e.get("category", ""), extracted_date)
                        for e in batch
                    ]
                    await conn.executemany(
                        """INSERT INTO fmcsa_register (number, title, decided, category, date_fetched)
                           VALUES ($1, $2, $3, $4, $5)
                           ON CONFLICT (number, date_fetched) DO UPDATE SET
                               title = EXCLUDED.title, decided = EXCLUDED.decided,
                               category = EXCLUDED.category, updated_at = NOW()""",
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
    rows = await pool.fetch(
        f"SELECT number, title, decided, category, date_fetched FROM fmcsa_register WHERE {where} ORDER BY number LIMIT 10000",
        *params,
    )
    return [dict(row) for row in rows]


async def get_fmcsa_extracted_dates() -> list[str]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT date_fetched FROM fmcsa_register ORDER BY date_fetched DESC"
    )
    return [row["date_fetched"] for row in rows]


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
        result = await pool.execute("DELETE FROM fmcsa_register WHERE date_fetched < $1", date)
        parts = result.split(" ")
        return int(parts[-1]) if len(parts) > 1 else 0
    except Exception as e:
        print(f"[DB] Error deleting FMCSA entries: {e}")
        return 0


# ═════════════════════════════════════════════════════════════════════════════
# New Ventures
# ═════════════════════════════════════════════════════════════════════════════

async def save_new_venture_entries(entries: list[dict], scrape_date: str) -> dict:
    pool = get_pool()
    if not entries:
        return {"success": True, "saved": 0, "skipped": 0}

    cols = _NV_COLUMNS + ["raw_data", "scrape_date"]
    col_list = ", ".join(cols)
    placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
    update_cols = [c for c in cols if c not in ("dot_number", "add_date")]
    on_conflict_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols) + ", updated_at = NOW()"

    insert_sql = f"""
        INSERT INTO new_ventures ({col_list}) VALUES ({placeholders})
        ON CONFLICT (dot_number, add_date) DO UPDATE SET {on_conflict_set}
    """

    saved = 0
    skipped = 0
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(entries), 500):
                    batch = entries[i:i + 500]
                    args = []
                    for entry in batch:
                        row_args = [
                            entry.get(col, "").strip() if isinstance(entry.get(col), str) else entry.get(col)
                            for col in _NV_COLUMNS
                        ]
                        row_args.append(json.dumps(entry.get("raw_data")) if entry.get("raw_data") else None)
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
    if active in ("active", "true"):
        conditions.append(f"((operating_status ILIKE ${idx} AND operating_status NOT ILIKE ${idx + 1}) OR operating_status ILIKE ${idx + 2})")
        params.extend(["%AUTHORIZED%", "%NOT AUTHORIZED%", "ACTIVE"])
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
            params.extend(states)
            idx += len(states)

    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email_address IS NOT NULL AND email_address != ''")
    elif has_email == "false":
        conditions.append("(email_address IS NULL OR email_address = '')")

    if filters.get("carrier_operation"):
        conditions.append(f"carrier_operation ILIKE ${idx}")
        params.append(f"%{filters['carrier_operation']}%")
        idx += 1

    if filters.get("hazmat") == "true":
        conditions.append("hm_ind = 'Y'")
    elif filters.get("hazmat") == "false":
        conditions.append("(hm_ind IS NULL OR hm_ind != 'Y')")

    idx = _add_range_filter(filters, "power_units", "total_pwr", conditions, params, idx)
    idx = _add_range_filter(filters, "drivers", "total_drivers", conditions, params, idx)

    for key, col in [("bipd_on_file", "bipd_file"), ("cargo_on_file", "cargo_file"), ("bond_on_file", "bond_file")]:
        if filters.get(key) == "true":
            conditions.append(f"{col} = 'Y'")
        elif filters.get(key) == "false":
            conditions.append(f"({col} IS NULL OR {col} != 'Y')")

    where = " AND ".join(conditions) if conditions else "TRUE"
    limit_val = int(filters.get("limit", 200 if not conditions else 10000))
    offset_val = int(filters.get("offset", 0))

    try:
        rows = await pool.fetch(
            f"SELECT * FROM new_ventures WHERE {where} ORDER BY created_at DESC LIMIT {limit_val} OFFSET {offset_val}",
            *params,
        )
        count_row = await pool.fetchrow(f"SELECT COUNT(*) as cnt FROM new_ventures WHERE {where}", *params)
        date_rows = await pool.fetch("SELECT DISTINCT add_date FROM new_ventures WHERE add_date IS NOT NULL ORDER BY add_date DESC")
        total_row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM new_ventures")
        return {
            "data": [_new_venture_row_to_dict(row) for row in rows],
            "filtered_count": count_row["cnt"] if count_row else 0,
            "total_count": total_row["cnt"] if total_row else 0,
            "available_dates": [r["add_date"] for r in date_rows],
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
        return _new_venture_row_to_dict(row) if row else None
    except Exception as e:
        print(f"[DB] Error fetching new venture {record_id}: {e}")
        return None


async def delete_new_venture(record_id: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute("DELETE FROM new_ventures WHERE id = $1", record_id)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting new venture {record_id}: {e}")
        return False


# ═════════════════════════════════════════════════════════════════════════════
# Users & Auth
# ═════════════════════════════════════════════════════════════════════════════

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
        return _user_row_to_dict(row) if row else None
    except Exception as e:
        print(f"[DB] Error fetching user by email: {e}")
        return None


async def create_user(user_data: dict) -> Optional[dict]:
    pool = get_pool()
    try:
        row = await pool.fetchrow(
            """INSERT INTO users (user_id, name, email, password_hash, role, plan,
                                  daily_limit, records_extracted_today, last_active,
                                  ip_address, is_online, is_blocked)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
               RETURNING *""",
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
        return _user_row_to_dict(row) if row else None
    except Exception as e:
        print(f"[DB] Error creating user: {e}")
        return None


async def update_user(user_id: str, user_data: dict) -> bool:
    pool = get_pool()
    _ALLOWED = {"name", "role", "plan", "daily_limit", "records_extracted_today",
                "last_active", "ip_address", "is_online", "is_blocked"}
    columns = {k: v for k, v in user_data.items() if k in _ALLOWED}
    if not columns:
        return False
    set_clauses = []
    values = []
    for i, (col, val) in enumerate(columns.items(), start=1):
        set_clauses.append(f"{col} = ${i}")
        values.append(val)
    values.append(user_id)
    try:
        result = await pool.execute(
            f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ${len(values)}", *values
        )
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error updating user {user_id}: {e}")
        return False


async def delete_user(user_id: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute("DELETE FROM users WHERE user_id = $1", user_id)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error deleting user {user_id}: {e}")
        return False


async def get_user_password_hash(email: str) -> Optional[str]:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT password_hash FROM users WHERE email = $1", email.lower())
        return row["password_hash"] if row and row["password_hash"] else None
    except Exception as e:
        print(f"[DB] Error fetching password hash: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# Blocked IPs
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_blocked_ips() -> list[dict]:
    pool = get_pool()
    try:
        rows = await pool.fetch("SELECT * FROM blocked_ips ORDER BY blocked_at DESC")
        return [_user_row_to_dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching blocked IPs: {e}")
        return []


async def block_ip(ip_address: str, reason: str) -> bool:
    pool = get_pool()
    try:
        await pool.execute(
            "INSERT INTO blocked_ips (ip_address, reason) VALUES ($1, $2) ON CONFLICT (ip_address) DO NOTHING",
            ip_address, reason or "No reason provided",
        )
        return True
    except Exception as e:
        print(f"[DB] Error blocking IP {ip_address}: {e}")
        return False


async def unblock_ip(ip_address: str) -> bool:
    pool = get_pool()
    try:
        result = await pool.execute("DELETE FROM blocked_ips WHERE ip_address = $1", ip_address)
        return not result.endswith("0")
    except Exception as e:
        print(f"[DB] Error unblocking IP {ip_address}: {e}")
        return False


async def is_ip_blocked(ip_address: str) -> bool:
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT ip_address FROM blocked_ips WHERE ip_address = $1", ip_address)
        return row is not None
    except Exception as e:
        print(f"[DB] Error checking IP block status: {e}")
        return False
