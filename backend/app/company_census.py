"""
Query functions for the company_census table.

Maps census fields to the same carrier format the frontend expects,
so the UI stays unchanged while serving ~4.4M pre-loaded records
instead of manually scraped data.
"""

import asyncio
import json
from typing import Optional

from app.database import get_pool


# ── Cargo column → human-readable label ──────────────────────────────────────
_CARGO_COLUMNS: list[tuple[str, str]] = [
    ("crgo_genfreight", "General Freight"),
    ("crgo_household", "Household Goods"),
    ("crgo_metalsheet", "Metal: Sheets, Coils, Rolls"),
    ("crgo_motoveh", "Motor Vehicles"),
    ("crgo_drivetow", "Drive/Tow Away"),
    ("crgo_logpole", "Logs, Poles, Beams, Lumber"),
    ("crgo_bldgmat", "Building Materials"),
    ("crgo_mobilehome", "Mobile Homes"),
    ("crgo_machlrg", "Machinery, Large Objects"),
    ("crgo_produce", "Fresh Produce"),
    ("crgo_liqgas", "Liquids/Gases"),
    ("crgo_intermodal", "Intermodal Containers"),
    ("crgo_passengers", "Passengers"),
    ("crgo_oilfield", "Oilfield Equipment"),
    ("crgo_livestock", "Livestock"),
    ("crgo_grainfeed", "Grain, Feed, Hay"),
    ("crgo_coalcoke", "Coal/Coke"),
    ("crgo_meat", "Meat"),
    ("crgo_garbage", "Garbage/Refuse"),
    ("crgo_usmail", "US Mail"),
    ("crgo_chem", "Chemicals"),
    ("crgo_drybulk", "Dry Bulk"),
    ("crgo_coldfood", "Refrigerated Food"),
    ("crgo_beverages", "Beverages"),
    ("crgo_paperprod", "Paper Products"),
    ("crgo_utility", "Utilities"),
    ("crgo_farmsupp", "Farm Supplies"),
    ("crgo_construct", "Construction"),
    ("crgo_waterwell", "Water Well"),
    ("crgo_cargoothr", "Other"),
]

# carrier_operation code → label
_CARRIER_OP_MAP = {
    "A": "Authorized For Hire",
    "B": "Exempt For Hire",
    "C": "Private (Property)",
    "D": "Private (Passengers, Business)",
    "E": "Private (Passengers, Non-Business)",
    "F": "Federal Government",
    "G": "State Government",
    "H": "Local Government",
    "I": "Indian Tribal Government",
    "J": "U.S. Mail",
    "K": "Migrant Workers",
    "L": "Other",
}

# carship code → entity type label
_CARSHIP_MAP = {
    "C": "CARRIER",
    "S": "SHIPPER",
    "B": "BROKER",
    "I": "INTERMODAL",
    "T": "CARGO TANK",
    "F": "FREIGHT FORWARDER",
    "R": "REGISTRANT",
}

# status_code → status label
_STATUS_MAP = {
    "A": "AUTHORIZED",
    "I": "NOT AUTHORIZED",
    "P": "PENDING",
}


def _build_entity_type(carship: Optional[str]) -> str:
    """Convert carship codes (e.g. 'C;B') to entity type string."""
    if not carship:
        return ""
    parts = carship.split(";")
    labels = [_CARSHIP_MAP.get(p.strip(), p.strip()) for p in parts]
    return " / ".join(labels)


def _build_cargo_list(row: dict) -> list[str]:
    """Build a list of cargo descriptions from crgo_* flag columns."""
    cargo = []
    for col, label in _CARGO_COLUMNS:
        if row.get(col) == "X":
            cargo.append(label)
    # If there is an "other" description, append it
    other_desc = row.get("crgo_cargoothr_desc")
    if other_desc:
        cargo.append(other_desc)
    return cargo


def _build_physical_address(row: dict) -> str:
    """Build a formatted physical address."""
    parts = [
        row.get("phy_street") or "",
        row.get("phy_city") or "",
        row.get("phy_state") or "",
        row.get("phy_zip") or "",
    ]
    street = parts[0]
    city_state_zip = ", ".join(p for p in parts[1:] if p)
    if street and city_state_zip:
        return f"{street}, {city_state_zip}"
    return street or city_state_zip


def _build_mailing_address(row: dict) -> str:
    """Build a formatted mailing address."""
    parts = [
        row.get("carrier_mailing_street") or "",
        row.get("carrier_mailing_city") or "",
        row.get("carrier_mailing_state") or "",
        row.get("carrier_mailing_zip") or "",
    ]
    street = parts[0]
    city_state_zip = ", ".join(p for p in parts[1:] if p)
    if street and city_state_zip:
        return f"{street}, {city_state_zip}"
    return street or city_state_zip


def _build_operation_classification(classdef: Optional[str]) -> list[str]:
    """Split classdef (e.g. 'AUTHORIZED FOR HIRE;PRIVATE PROPERTY') into a list."""
    if not classdef:
        return []
    return [c.strip() for c in classdef.split(";") if c.strip()]


def _build_carrier_operation(code: Optional[str]) -> list[str]:
    """Convert carrier_operation code to list of operation labels."""
    if not code:
        return []
    return [_CARRIER_OP_MAP.get(code, code)]


def _format_phone(phone: Optional[str]) -> Optional[str]:
    """Format a 10-digit phone string into (XXX) XXX-XXXX."""
    if not phone or len(phone) != 10 or not phone.isdigit():
        return phone
    return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"


def _format_mcs150_date(raw: Optional[str]) -> Optional[str]:
    """Convert '20231020 1657' to '10/20/2023'."""
    if not raw or len(raw) < 8:
        return raw
    date_part = raw[:8]
    try:
        return f"{date_part[4:6]}/{date_part[6:8]}/{date_part[:4]}"
    except Exception:
        return raw


def _census_row_to_carrier(row: dict) -> dict:
    """
    Convert a company_census row to the carrier dict format
    the frontend expects.
    """
    row_dict = dict(row)

    mc_number = None
    if row_dict.get("docket1prefix") == "MC":
        mc_number = row_dict.get("docket1")
    elif row_dict.get("docket2prefix") == "MC":
        mc_number = row_dict.get("docket2")
    elif row_dict.get("docket3prefix") == "MC":
        mc_number = row_dict.get("docket3")

    dot_number = row_dict.get("dot_number")

    status_code = (row_dict.get("status_code") or "").upper()
    status = _STATUS_MAP.get(status_code, status_code)

    cargo_carried = _build_cargo_list(row_dict)
    if row_dict.get("hm_ind") == "Y":
        cargo_carried.append("Hazardous Materials")

    return {
        "mc_number": str(mc_number) if mc_number else None,
        "dot_number": str(dot_number) if dot_number else None,
        "legal_name": row_dict.get("legal_name"),
        "dba_name": row_dict.get("dba_name"),
        "entity_type": _build_entity_type(row_dict.get("carship")),
        "status": status,
        "email": row_dict.get("email_address"),
        "phone": _format_phone(row_dict.get("phone")),
        "power_units": row_dict.get("power_units"),
        "drivers": row_dict.get("total_drivers"),
        "non_cmv_units": row_dict.get("bus_units"),
        "physical_address": _build_physical_address(row_dict),
        "mailing_address": _build_mailing_address(row_dict),
        "date_scraped": None,
        "mcs150_date": _format_mcs150_date(row_dict.get("mcs150_date")),
        "mcs150_mileage": row_dict.get("mcs150_mileage"),
        "operation_classification": _build_operation_classification(row_dict.get("classdef")),
        "carrier_operation": _build_carrier_operation(row_dict.get("carrier_operation")),
        "cargo_carried": cargo_carried,
        "out_of_service_date": None,
        "state_carrier_id": None,
        "duns_number": row_dict.get("dun_bradstreet_no"),
        "safety_rating": row_dict.get("safety_rating"),
        "safety_rating_date": row_dict.get("safety_rating_date"),
        "basic_scores": None,
        "oos_rates": None,
        "insurance_policies": None,
        "insurance_history_filings": [],
        "inspections": None,
        "crashes": None,
        "created_at": None,
        "updated_at": None,
        "id": str(row_dict.get("id")) if row_dict.get("id") is not None else None,
    }


# ── The SELECT columns we need from company_census ───────────────────────────
_CENSUS_SELECT_COLS = """
    id, dot_number, legal_name, dba_name, status_code, email_address,
    phone, power_units, total_drivers, bus_units,
    phy_street, phy_city, phy_state, phy_zip,
    carrier_mailing_street, carrier_mailing_city,
    carrier_mailing_state, carrier_mailing_zip,
    mcs150_date, mcs150_mileage, dun_bradstreet_no,
    carrier_operation, classdef, carship, hm_ind,
    safety_rating, safety_rating_date,
    docket1prefix, docket1, docket2prefix, docket2, docket3prefix, docket3,
    crgo_genfreight, crgo_household, crgo_metalsheet, crgo_motoveh,
    crgo_drivetow, crgo_logpole, crgo_bldgmat, crgo_mobilehome,
    crgo_machlrg, crgo_produce, crgo_liqgas, crgo_intermodal,
    crgo_passengers, crgo_oilfield, crgo_livestock, crgo_grainfeed,
    crgo_coalcoke, crgo_meat, crgo_garbage, crgo_usmail, crgo_chem,
    crgo_drybulk, crgo_coldfood, crgo_beverages, crgo_paperprod,
    crgo_utility, crgo_farmsupp, crgo_construct, crgo_waterwell,
    crgo_cargoothr, crgo_cargoothr_desc
"""


async def fetch_census_carriers(filters: dict) -> dict:
    """
    Query company_census with the same filter interface the frontend uses,
    returning data in the same carrier format.
    """
    pool = get_pool()

    conditions: list[str] = []
    params: list = []
    idx = 1

    # ── MC Number ─────────────────────────────────────────────────────────
    if filters.get("mc_number"):
        conditions.append(
            f"(docket1prefix = 'MC' AND docket1 ILIKE ${idx})"
        )
        params.append(f"%{filters['mc_number']}%")
        idx += 1

    # ── DOT Number ────────────────────────────────────────────────────────
    if filters.get("dot_number"):
        conditions.append(f"dot_number::text ILIKE ${idx}")
        params.append(f"%{filters['dot_number']}%")
        idx += 1

    # ── Legal Name ────────────────────────────────────────────────────────
    if filters.get("legal_name"):
        conditions.append(f"legal_name ILIKE ${idx}")
        params.append(f"%{filters['legal_name']}%")
        idx += 1

    # ── Entity Type (carship) ─────────────────────────────────────────────
    entity_type = filters.get("entity_type")
    if entity_type:
        et_upper = entity_type.upper()
        # Map frontend entity type labels back to carship codes
        reverse_map = {v: k for k, v in _CARSHIP_MAP.items()}
        code = reverse_map.get(et_upper)
        if code:
            conditions.append(f"carship LIKE ${idx}")
            params.append(f"%{code}%")
        else:
            conditions.append(f"carship ILIKE ${idx}")
            params.append(f"%{et_upper}%")
        idx += 1

    # ── Active status ─────────────────────────────────────────────────────
    active = filters.get("active")
    if active == "true":
        conditions.append("status_code = 'A'")
    elif active == "false":
        conditions.append("status_code != 'A'")

    # ── State ─────────────────────────────────────────────────────────────
    if filters.get("state"):
        states = filters["state"].split("|")
        or_clauses = []
        for s in states:
            or_clauses.append(f"phy_state = ${idx}")
            params.append(s.strip().upper())
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Has Email ─────────────────────────────────────────────────────────
    has_email = filters.get("has_email")
    if has_email == "true":
        conditions.append("email_address IS NOT NULL AND email_address != ''")
    elif has_email == "false":
        conditions.append("(email_address IS NULL OR email_address = '')")

    # ── Has Company Rep (DBA name) ────────────────────────────────────────
    has_company_rep = filters.get("has_company_rep")
    if has_company_rep == "true":
        conditions.append("dba_name IS NOT NULL AND dba_name != ''")
    elif has_company_rep == "false":
        conditions.append("(dba_name IS NULL OR dba_name = '')")

    # ── Classification (classdef) ─────────────────────────────────────────
    if filters.get("classification"):
        classifications = filters["classification"]
        if isinstance(classifications, str):
            classifications = classifications.split(",")
        or_clauses = []
        for cls in classifications:
            or_clauses.append(f"classdef ILIKE ${idx}")
            params.append(f"%{cls.strip()}%")
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Carrier Operation ─────────────────────────────────────────────────
    if filters.get("carrier_operation"):
        ops = filters["carrier_operation"]
        if isinstance(ops, str):
            ops = ops.split(",")
        # Map labels back to codes
        reverse_op = {v.upper(): k for k, v in _CARRIER_OP_MAP.items()}
        or_clauses = []
        for op in ops:
            code = reverse_op.get(op.strip().upper(), op.strip())
            or_clauses.append(f"carrier_operation = ${idx}")
            params.append(code)
            idx += 1
        conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Cargo ─────────────────────────────────────────────────────────────
    if filters.get("cargo"):
        cargo_items = filters["cargo"]
        if isinstance(cargo_items, str):
            cargo_items = cargo_items.split(",")
        # Map cargo labels to column names
        label_to_col = {label: col for col, label in _CARGO_COLUMNS}
        or_clauses = []
        for item in cargo_items:
            item_stripped = item.strip()
            col = label_to_col.get(item_stripped)
            if col:
                or_clauses.append(f"{col} = 'X'")
            elif item_stripped.upper() == "HAZARDOUS MATERIALS":
                or_clauses.append("hm_ind = 'Y'")
        if or_clauses:
            conditions.append(f"({' OR '.join(or_clauses)})")

    # ── Hazmat ────────────────────────────────────────────────────────────
    hazmat = filters.get("hazmat")
    if hazmat == "true":
        conditions.append("hm_ind = 'Y'")
    elif hazmat == "false":
        conditions.append("(hm_ind = 'N' OR hm_ind IS NULL)")

    # ── Power Units range ─────────────────────────────────────────────────
    if filters.get("power_units_min"):
        conditions.append(f"NULLIF(power_units, '')::int >= ${idx}")
        params.append(int(filters["power_units_min"]))
        idx += 1
    if filters.get("power_units_max"):
        conditions.append(f"NULLIF(power_units, '')::int <= ${idx}")
        params.append(int(filters["power_units_max"]))
        idx += 1

    # ── Drivers range ─────────────────────────────────────────────────────
    if filters.get("drivers_min"):
        conditions.append(f"NULLIF(total_drivers, '')::int >= ${idx}")
        params.append(int(filters["drivers_min"]))
        idx += 1
    if filters.get("drivers_max"):
        conditions.append(f"NULLIF(total_drivers, '')::int <= ${idx}")
        params.append(int(filters["drivers_max"]))
        idx += 1

    # ── Years in business (mcs150_date) ───────────────────────────────────
    if filters.get("years_in_business_min"):
        conditions.append(
            f"mcs150_date IS NOT NULL AND mcs150_date != '' "
            f"AND TO_DATE(LEFT(mcs150_date, 8), 'YYYYMMDD') <= (CURRENT_DATE - make_interval(years => ${idx}))"
        )
        params.append(int(filters["years_in_business_min"]))
        idx += 1
    if filters.get("years_in_business_max"):
        conditions.append(
            f"mcs150_date IS NOT NULL AND mcs150_date != '' "
            f"AND TO_DATE(LEFT(mcs150_date, 8), 'YYYYMMDD') >= (CURRENT_DATE - make_interval(years => ${idx}))"
        )
        params.append(int(filters["years_in_business_max"]))
        idx += 1

    # ── Insurance-related filters (join insurance_history via docket) ─────
    # Build the docket expression for joining
    _DOCKET_EXPR = "CASE WHEN docket1prefix = 'MC' THEN 'MC' || docket1 ELSE NULL END"

    if filters.get("insurance_required"):
        _INS_TYPE_PATTERN = {"BI&PD": "BIPD%", "CARGO": "CARGO", "BOND": "SURETY", "TRUST FUND": "TRUST FUND"}
        ins_types = filters["insurance_required"]
        if isinstance(ins_types, str):
            ins_types = ins_types.split(",")
        or_clauses = []
        for itype in ins_types:
            pattern = _INS_TYPE_PATTERN.get(itype, itype)
            or_clauses.append(
                f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = {_DOCKET_EXPR} "
                f"AND ih.ins_type_desc LIKE ${idx} AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
            )
            params.append(pattern)
            idx += 1
        conditions.append(f"docket1prefix = 'MC' AND ({' OR '.join(or_clauses)})")

    bipd_on_file = filters.get("bipd_on_file")
    if bipd_on_file == "1":
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} AND ih.ins_type_desc LIKE ${idx} "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("BIPD%")
        idx += 1
    elif bipd_on_file == "0":
        conditions.append(
            f"(docket1prefix != 'MC' OR docket1prefix IS NULL OR NOT EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} AND ih.ins_type_desc LIKE ${idx} "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '')))"
        )
        params.append("BIPD%")
        idx += 1

    cargo_on_file = filters.get("cargo_on_file")
    if cargo_on_file == "1":
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} AND ih.ins_type_desc = ${idx} "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("CARGO")
        idx += 1
    elif cargo_on_file == "0":
        conditions.append(
            f"(docket1prefix != 'MC' OR docket1prefix IS NULL OR NOT EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} AND ih.ins_type_desc = ${idx} "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '')))"
        )
        params.append("CARGO")
        idx += 1

    bond_on_file = filters.get("bond_on_file")
    if bond_on_file == "1":
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} AND ih.ins_type_desc = ${idx} "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("SURETY")
        idx += 1
    elif bond_on_file == "0":
        conditions.append(
            f"(docket1prefix != 'MC' OR docket1prefix IS NULL OR NOT EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} AND ih.ins_type_desc = ${idx} "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '')))"
        )
        params.append("SURETY")
        idx += 1

    trust_fund_on_file = filters.get("trust_fund_on_file")
    if trust_fund_on_file == "1":
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} AND ih.ins_type_desc = ${idx} "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = ''))"
        )
        params.append("TRUST FUND")
        idx += 1
    elif trust_fund_on_file == "0":
        conditions.append(
            f"(docket1prefix != 'MC' OR docket1prefix IS NULL OR NOT EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} AND ih.ins_type_desc = ${idx} "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '')))"
        )
        params.append("TRUST FUND")
        idx += 1

    # ── BIPD amount range ─────────────────────────────────────────────────
    if filters.get("bipd_min"):
        raw_min = int(filters["bipd_min"])
        compare_min = raw_min // 1000 if raw_min >= 10000 else raw_min
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND NULLIF(REPLACE(ih.max_cov_amount, ',', ''), '')::numeric >= ${idx})"
        )
        params.append(compare_min)
        idx += 1
    if filters.get("bipd_max"):
        raw_max = int(filters["bipd_max"])
        compare_max = raw_max // 1000 if raw_max >= 10000 else raw_max
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND NULLIF(REPLACE(ih.max_cov_amount, ',', ''), '')::numeric <= ${idx})"
        )
        params.append(compare_max)
        idx += 1

    # ── Insurance effective date range ────────────────────────────────────
    if filters.get("ins_effective_date_from"):
        parts = filters["ins_effective_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND TO_DATE(ih.effective_date, 'MM/DD/YYYY') >= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("ins_effective_date_to"):
        parts = filters["ins_effective_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND TO_DATE(ih.effective_date, 'MM/DD/YYYY') <= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_to_db_fmt)
        idx += 1

    # ── Insurance cancellation date range ─────────────────────────────────
    if filters.get("ins_cancellation_date_from"):
        parts = filters["ins_cancellation_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND ih.cancl_effective_date IS NOT NULL AND ih.cancl_effective_date != '' "
            f"AND ih.cancl_effective_date LIKE '%/%/%' "
            f"AND TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("ins_cancellation_date_to"):
        parts = filters["ins_cancellation_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND ih.cancl_effective_date IS NOT NULL AND ih.cancl_effective_date != '' "
            f"AND ih.cancl_effective_date LIKE '%/%/%' "
            f"AND TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') <= TO_DATE(${idx}, 'MM/DD/YYYY'))"
        )
        params.append(date_to_db_fmt)
        idx += 1

    # ── Insurance company filter ──────────────────────────────────────────
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
        or_clauses = []
        for company in companies:
            company_upper = company.strip().upper()
            patterns = _INSURANCE_COMPANY_PATTERNS.get(company_upper, [f"{company_upper}%"])
            for pattern in patterns:
                or_clauses.append(
                    f"EXISTS (SELECT 1 FROM insurance_history ih WHERE ih.docket_number = {_DOCKET_EXPR} "
                    f"AND UPPER(ih.name_company) LIKE ${idx} "
                    f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '' "
                    f"OR TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE))"
                )
                params.append(pattern)
                idx += 1
        conditions.append(f"docket1prefix = 'MC' AND ({' OR '.join(or_clauses)})")

    # ── Renewal policy months filter ──────────────────────────────────────
    if filters.get("renewal_policy_months"):
        months = int(filters["renewal_policy_months"])
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '' "
            f"OR TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") BETWEEN CURRENT_DATE AND (DATE_TRUNC('MONTH', CURRENT_DATE + MAKE_INTERVAL(months => ${idx})) + INTERVAL '1 MONTH - 1 DAY')::date"
            f")"
        )
        params.append(months)
        idx += 1

    # ── Renewal date range filter ─────────────────────────────────────────
    if filters.get("renewal_date_from"):
        parts = filters["renewal_date_from"].split("-")
        date_from_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '' "
            f"OR TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") >= TO_DATE(${idx}, 'MM/DD/YYYY')"
            f")"
        )
        params.append(date_from_db_fmt)
        idx += 1
    if filters.get("renewal_date_to"):
        parts = filters["renewal_date_to"].split("-")
        date_to_db_fmt = f"{parts[1]}/{parts[2]}/{parts[0]}"
        conditions.append(
            f"docket1prefix = 'MC' AND EXISTS (SELECT 1 FROM insurance_history ih "
            f"WHERE ih.docket_number = {_DOCKET_EXPR} "
            f"AND ih.effective_date IS NOT NULL AND ih.effective_date LIKE '%/%/%' "
            f"AND (ih.cancl_effective_date IS NULL OR ih.cancl_effective_date = '' "
            f"OR TO_DATE(ih.cancl_effective_date, 'MM/DD/YYYY') >= CURRENT_DATE) "
            f"AND ("
            f"  CASE "
            f"    WHEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"         >= CURRENT_DATE "
            f"    THEN MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"    ELSE MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"         EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"         LEAST(EXTRACT(DAY FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, "
            f"           EXTRACT(DAY FROM (DATE_TRUNC('MONTH', MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, "
            f"             EXTRACT(MONTH FROM TO_DATE(ih.effective_date, 'MM/DD/YYYY'))::int, 1)) + INTERVAL '1 MONTH - 1 DAY'))::int)) "
            f"  END"
            f") <= TO_DATE(${idx}, 'MM/DD/YYYY')"
            f")"
        )
        params.append(date_to_db_fmt)
        idx += 1

    # ── Build WHERE clause ────────────────────────────────────────────────
    where = " AND ".join(conditions) if conditions else "TRUE"

    is_filtered = len(conditions) > 0
    if is_filtered:
        limit_val = int(filters.get("limit", 500))
    else:
        limit_val = int(filters.get("limit", 200))

    offset_val = int(filters.get("offset", 0))

    # ── Insurance history join (for carriers with MC numbers) ─────────────
    _DOCKET_EXPR_C = "CASE WHEN c.docket1prefix = 'MC' THEN 'MC' || c.docket1 ELSE NULL END"

    query = f"""
        SELECT {_CENSUS_SELECT_COLS},
          COALESCE(ih_agg.filings, '[]'::jsonb) AS _ih_filings
        FROM company_census c
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(jsonb_build_object(
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
            WHERE ih.docket_number = {_DOCKET_EXPR_C}
        ) ih_agg ON true
        WHERE {where}
        ORDER BY c.id
        LIMIT {limit_val} OFFSET {offset_val}
    """

    count_query = f"""
        SELECT COUNT(*) as cnt FROM company_census c
        WHERE {where}
    """

    try:
        rows, count_row = await asyncio.gather(
            pool.fetch(query, *params),
            pool.fetchrow(count_query, *params),
        )
        filtered_count = count_row["cnt"] if count_row else 0

        data = []
        for row in rows:
            carrier = _census_row_to_carrier(row)
            # Attach insurance history filings
            ih_raw = row.get("_ih_filings")
            if ih_raw and ih_raw != "[]":
                if isinstance(ih_raw, str):
                    carrier["insurance_history_filings"] = json.loads(ih_raw)
                else:
                    carrier["insurance_history_filings"] = ih_raw
            data.append(carrier)

        return {
            "data": data,
            "filtered_count": filtered_count,
        }
    except Exception as e:
        print(f"[DB] Error fetching census carriers: {e}")
        import traceback
        traceback.print_exc()
        return {"data": [], "filtered_count": 0}


async def get_census_carrier_count() -> int:
    """Get total count of records in company_census."""
    pool = get_pool()
    try:
        row = await pool.fetchrow("SELECT COUNT(*) as cnt FROM company_census")
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[DB] Error getting census carrier count: {e}")
        return 0


async def get_census_dashboard_stats() -> dict:
    """Get dashboard statistics from company_census."""
    pool = get_pool()
    empty = {
        "total": 0, "active_carriers": 0, "brokers": 0,
        "with_email": 0, "email_rate": "0",
        "with_safety_rating": 0, "with_insurance": 0,
        "with_inspections": 0, "with_crashes": 0,
        "not_authorized": 0, "other": 0,
    }
    try:
        row = await pool.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status_code = 'A') AS active_carriers,
                COUNT(*) FILTER (WHERE carship LIKE '%B%') AS brokers,
                COUNT(*) FILTER (WHERE email_address IS NOT NULL AND email_address != '') AS with_email,
                COUNT(*) FILTER (WHERE safety_rating IS NOT NULL AND safety_rating != '') AS with_safety_rating,
                COUNT(*) FILTER (WHERE status_code = 'I') AS not_authorized,
                COUNT(*) FILTER (WHERE hm_ind = 'Y') AS hazmat_carriers
            FROM company_census
        """)
        if not row:
            return empty
        total = row["total"]
        active = row["active_carriers"]
        not_auth = row["not_authorized"]
        with_email = row["with_email"]
        email_rate = f"{(with_email / total * 100):.1f}" if total > 0 else "0"
        return {
            "total": total,
            "active_carriers": active,
            "brokers": row["brokers"],
            "with_email": with_email,
            "email_rate": email_rate,
            "with_safety_rating": row["with_safety_rating"],
            "with_insurance": 0,
            "with_inspections": 0,
            "with_crashes": 0,
            "not_authorized": not_auth,
            "other": total - active - not_auth,
        }
    except Exception as e:
        print(f"[DB] Error getting census dashboard stats: {e}")
        return empty


async def get_census_carriers_by_mc_range(start: str, end: str) -> list[dict]:
    """Get carriers within an MC number range from company_census."""
    pool = get_pool()
    try:
        rows = await pool.fetch(
            f"""
            SELECT {_CENSUS_SELECT_COLS}
            FROM company_census c
            WHERE docket1prefix = 'MC'
              AND docket1 ~ '^[0-9]+$'
              AND docket1::bigint BETWEEN $1 AND $2
            ORDER BY docket1::bigint
            LIMIT 1000
            """,
            int(start),
            int(end),
        )
        return [_census_row_to_carrier(row) for row in rows]
    except Exception as e:
        print(f"[DB] Error fetching census carriers by range: {e}")
        return []
