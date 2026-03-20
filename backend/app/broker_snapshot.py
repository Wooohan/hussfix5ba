"""BrokerSnapshot.com scraper for New Ventures data.
Fetches newly-added carrier data from BrokerSnapshot by date,
downloads the CSV export, parses it, and returns structured records.
Uses curl_cffi with Chrome TLS impersonation to bypass anti-bot detection.
"""
import os
import csv
import io
import re
import asyncio
import time
import random
from typing import Optional
from curl_cffi import requests as curl_requests
# ── Configuration (from environment) ─────────────────────────────────────────
BS_EMAIL = os.getenv("BROKER_SNAPSHOT_EMAIL", "")
BS_PASSWORD = os.getenv("BROKER_SNAPSHOT_PASSWORD", "")
BS_PROXIES = os.getenv("BROKER_SNAPSHOT_PROXIES", "")  # comma-separated host:port
BS_PROXY_USER = os.getenv("BROKER_SNAPSHOT_PROXY_USER", "")
BS_PROXY_PASS = os.getenv("BROKER_SNAPSHOT_PROXY_PASS", "")
# Webshare rotating proxy (preferred – single endpoint that rotates IPs)
WEBSHARE_PROXY_URL = os.getenv("WEBSHARE_PROXY_URL", "")  # full URL e.g. http://user:pass@host:port
BASE_URL = "https://brokersnapshot.com"
# ── CSV column → DB field mapping ────────────────────────────────────────────
# BrokerSnapshot CSV uses raw internal column names (e.g. "name", "email_address",
# "phy_str"). We map both human-readable and raw names to DB columns.
_COLUMN_MAP: dict[str, str] = {
    # ── DOT number ──
    "DOT#": "dot_number",
    "DOT #": "dot_number",
    "USDOT": "dot_number",
    "USDOT Number": "dot_number",
    "USDOT#": "dot_number",
    "dot_number": "dot_number",
    # ── MC number ──
    "MC#": "mc_number",
    "MC #": "mc_number",
    "MC/MX#": "mc_number",
    "MC/MX #": "mc_number",
    "MC Number": "mc_number",
    # ── Names ──
    "Legal Name": "legal_name",
    "Company Name": "legal_name",
    "name": "legal_name",
    "DBA Name": "dba_name",
    "DBA": "dba_name",
    "name_dba": "dba_name",
    # ── Entity / Status ──
    "Entity Type": "entity_type",
    "carship": "entity_type",
    "Operating Status": "status",
    "Status": "status",
    "OperatingStatus": "status",
    # ── Contact ──
    "Email": "email",
    "email_address": "email",
    "Phone": "phone",
    "Phone Number": "phone",
    "phy_phone": "phone",
    # ── Address (human-readable single-field) ──
    "Physical Address": "physical_address",
    "Mailing Address": "mailing_address",
    # ── Power / Drivers ──
    "Power Units": "power_units",
    "total_pwr": "power_units",
    "Drivers": "drivers",
    "Total Drivers": "drivers",
    "total_drivers": "drivers",
    # ── Cargo ──
    "Cargo Carried": "cargo_carried",
    "Cargo Transported": "cargo_carried",
    # ── Hazmat ──
    "Hazmat": "hazmat_indicator",
    "Hazmat Indicator": "hazmat_indicator",
    "HM Indicator": "hazmat_indicator",
    "hm_ind": "hazmat_indicator",
    # ── Classification / Operation ──
    "Operation Classification": "operation_classification",
    "Op. Classification": "operation_classification",
    "Carrier Operation": "carrier_operation",
    "carrier_operation": "carrier_operation",
    # ── Safety ──
    "Safety Rating": "safety_rating",
    "safety_rating": "safety_rating",
    "Safety Rating Type Code": "safety_rating_type_code",
    "Safety Type Code": "safety_rating_type_code",
    "Safety Rating Date": "safety_rating_effective_date",
    "Safety Effective Date": "safety_rating_effective_date",
    "safety_rating_date": "safety_rating_effective_date",
    "Latest Review Type": "safety_rating_latest_review_type",
    "review_type": "safety_rating_latest_review_type",
    "Latest Review Date": "safety_rating_latest_review_date",
    "review_date": "safety_rating_latest_review_date",
    # ── MCSIP ──
    "MCSIP Step": "mcsip_step_number",
    "MCSIP Step Number": "mcsip_step_number",
    # ── Interstate / Intrastate drivers ──
    "Interstate Within 100 Miles": "interstate_within_100",
    "inter_within_100": "interstate_within_100",
    "Interstate Beyond 100 Miles": "interstate_beyond_100",
    "inter_beyond_100": "interstate_beyond_100",
    "Interstate Total": "interstate_total",
    "total_inter_drivers": "interstate_total",
    "Intrastate Within 100 Miles": "intrastate_within_100",
    "intra_within_100": "intrastate_within_100",
    "Intrastate Beyond 100 Miles": "intrastate_beyond_100",
    "intra_beyond_100": "intrastate_beyond_100",
    "Intrastate Total": "intrastate_total",
    "total_intra_drivers": "intrastate_total",
    "Avg Trip Leased Drivers": "avg_trip_leased_drivers",
    "avg_tld": "avg_trip_leased_drivers",
    # ── Grand total / CDL ──
    "Grand Total": "grand_total_drivers",
    "Grand Total Drivers": "grand_total_drivers",
    "Grand Total (Interstate and Intrastate)": "grand_total_drivers",
    "Total CDL": "total_cdl",
    "Total with CDL": "total_cdl",
    "total_cdl": "total_cdl",
    "Total Non-CDL": "total_non_cdl",
    "Total with Non-CDL": "total_non_cdl",
    # ── Trucks / Fleet ──
    "Total Trucks": "total_trucks",
    "Total Number of Trucks": "total_trucks",
    "total_trucks": "total_trucks",
    "Total Power Units": "total_power_units",
    "Total Number of Power Units": "total_power_units",
    "Fleet Size Code": "fleet_size_code",
    "Fleet Size": "fleet_size_code",
    "fleetsize": "fleet_size_code",
    # ── Equipment owned/leased ──
    "Owned Truck": "owned_trucks",
    "Owned Trucks": "owned_trucks",
    "owntruck": "owned_trucks",
    "Term Leased Truck": "term_leased_trucks",
    "Term Leased Trucks": "term_leased_trucks",
    "trmtruck": "term_leased_trucks",
    "Trip Leased Truck": "trip_leased_trucks",
    "Trip Leased Trucks": "trip_leased_trucks",
    "trptruck": "trip_leased_trucks",
    "Owned Tractor": "owned_tractors",
    "Owned Tractors": "owned_tractors",
    "owntract": "owned_tractors",
    "Term Leased Tractor": "term_leased_tractors",
    "Term Leased Tractors": "term_leased_tractors",
    "trmtract": "term_leased_tractors",
    "Trip Leased Tractor": "trip_leased_tractors",
    "Trip Leased Tractors": "trip_leased_tractors",
    "trptract": "trip_leased_tractors",
    "Owned Trailer": "owned_trailers",
    "Owned Trailers": "owned_trailers",
    "owntrail": "owned_trailers",
    "Term Leased Trailer": "term_leased_trailers",
    "Term Leased Trailers": "term_leased_trailers",
    "trmtrail": "term_leased_trailers",
    "Trip Leased Trailer": "trip_leased_trailers",
    "Trip Leased Trailers": "trip_leased_trailers",
    "trptrail": "trip_leased_trailers",
    # ── Accident / MCS-150 ──
    "Recordable Accident Rate": "recordable_accident_rate",
    "recordable_crash_rate": "recordable_accident_rate",
    "Preventable Recordable Accident Rate": "preventable_accident_rate",
    "MCS-150 Mileage": "mcs150_mileage",
    "mcs150_mileage": "mcs150_mileage",
    "MCS-150 Mileage (Year)": "mcs150_mileage_year",
    "MCS-150 Mileage Year": "mcs150_mileage_year",
    "mcs150_mileage_year": "mcs150_mileage_year",
    "MCS-150 Date": "mcs150_date",
    "MCS-150 date": "mcs150_date",
    "MCS-150 Form Date": "mcs150_date",
    "mcs150_date": "mcs150_date",
    # ── Dates / IDs ──
    "Added Date": "date_added",
    "Date Added": "date_added",
    "add_date": "date_added",
    "State Carrier ID": "state_carrier_id",
    "DUNS Number": "duns_number",
    "Out of Service Date": "out_of_service_date",
}
# Raw CSV cargo boolean columns → human-readable cargo labels.
# When a row has "X" in one of these columns, we include its label.
_CARGO_COLUMNS: dict[str, str] = {
    "genfreight": "General Freight",
    "household": "Household Goods",
    "metalsheet": "Metal/Sheets/Coils",
    "motorveh": "Motor Vehicles",
    "drivetow": "Drive/Tow Away",
    "logpole": "Logs/Poles/Beams",
    "bldgmat": "Building Materials",
    "mobilehome": "Mobile Homes",
    "machlrg": "Machinery/Large Objects",
    "produce": "Fresh Produce",
    "liqgas": "Liquids/Gases",
    "intermodal": "Intermodal Containers",
    "passengers": "Passengers",
    "oilfield": "Oilfield Equipment",
    "livestock": "Livestock",
    "grainfeed": "Grain/Feed/Hay",
    "coalcoke": "Coal/Coke",
    "meat": "Meat",
    "garbage": "Garbage/Refuse",
    "usmail": "US Mail",
    "chem": "Chemicals",
    "drybulk": "Dry Bulk",
    "coldfood": "Refrigerated Food",
    "beverages": "Beverages",
    "paperprod": "Paper Products",
    "utility": "Utilities",
    "farmsupp": "Farm Supplies",
    "construct": "Construction",
    "waterwell": "Water Well",
    "cargoothr": "Other",
}
# ── Physical / mailing address raw columns for combining ─────────────────────
_PHY_ADDR_COLS = ("phy_str", "phy_city", "phy_st", "phy_zip", "phy_country")
_MAI_ADDR_COLS = ("mai_str", "mai_city", "mai_st", "mai_zip", "mai_country")
# ── Operation classification raw columns ─────────────────────────────────────
_OP_CLASS_COLUMNS: dict[str, str] = {
    "property_chk": "Property",
    "passenger_chk": "Passenger",
    "hhg_chk": "Household Goods",
    "private_auth_chk": "Private (Property)",
    "enterprise_chk": "Enterprise",
}
# All valid DB columns for new_ventures (used to filter unknown CSV columns)
_VALID_COLUMNS = {
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
    "date_added", "state_carrier_id", "duns_number", "out_of_service_date",
}
def _get_all_proxy_urls() -> list[str]:
    """Return all proxy URLs, with Webshare rotating proxy first.
    Priority order:
    1. Webshare rotating proxy (single endpoint, auto-rotates IPs)
    2. Individual static proxies from BS_PROXIES (shuffled)
    """
    urls: list[str] = []
    # Webshare rotating proxy (preferred)
    if WEBSHARE_PROXY_URL:
        urls.append(WEBSHARE_PROXY_URL.strip())
    # Static proxy list (fallback)
    if BS_PROXIES and BS_PROXY_USER:
        hosts = [h.strip() for h in BS_PROXIES.split(",") if h.strip()]
        random.shuffle(hosts)
        urls.extend(f"http://{BS_PROXY_USER}:{BS_PROXY_PASS}@{h}" for h in hosts)
    return urls
def _get_proxy_url() -> Optional[str]:
    """Build a proxy URL from environment config, or None."""
    urls = _get_all_proxy_urls()
    return urls[0] if urls else None
def _normalise_column(raw: str) -> Optional[str]:
    """Map a raw CSV header to a DB column name."""
    # Strip BOM and whitespace
    stripped = raw.strip().lstrip("\ufeff")
    if stripped in _COLUMN_MAP:
        return _COLUMN_MAP[stripped]
    # Try case-insensitive match
    for key, val in _COLUMN_MAP.items():
        if key.lower() == stripped.lower():
            return val
    return None
def _clean_val(val: str) -> str:
    """Strip whitespace and dash placeholders."""
    v = val.strip()
    if v in ("─", "-", "—", "N/A", "n/a", ""):
        return ""
    return v
def _combine_address(row: dict, cols: tuple) -> str:
    """Combine multiple address columns into one string."""
    parts = []
    for col in cols:
        val = _clean_val(row.get(col, ""))
        if val:
            parts.append(val)
    return ", ".join(parts)
def _collect_cargo(row: dict) -> str:
    """Build comma-separated cargo string from boolean cargo columns."""
    labels = []
    for col, label in _CARGO_COLUMNS.items():
        val = row.get(col, "").strip().upper()
        if val in ("X", "Y", "YES", "1", "TRUE"):
            labels.append(label)
    # Also include custom cargo description if present
    other_desc = _clean_val(row.get("cargoothr_desc", ""))
    if other_desc and other_desc not in labels:
        labels.append(other_desc)
    return ", ".join(labels)
def _collect_op_classification(row: dict) -> str:
    """Build comma-separated operation classification from boolean columns."""
    labels = []
    for col, label in _OP_CLASS_COLUMNS.items():
        val = row.get(col, "").strip().upper()
        if val in ("X", "Y", "YES", "1", "TRUE"):
            labels.append(label)
    return ", ".join(labels)
def _build_mc_number(row: dict) -> str:
    """Build MC number from prefix + docket_number raw columns."""
    prefix = _clean_val(row.get("prefix", ""))
    docket = _clean_val(row.get("docket_number", ""))
    if prefix and docket:
        return f"{prefix}{docket}"
    return docket
def _has_raw_columns(fieldnames: list[str]) -> bool:
    """Detect whether the CSV uses BrokerSnapshot raw internal column names."""
    raw_indicators = {"phy_str", "email_address", "carship", "owntruck", "hm_ind"}
    clean_names = {f.strip().lstrip("\ufeff") for f in fieldnames}
    return bool(raw_indicators & clean_names)
def parse_csv(csv_bytes: bytes, added_date: str) -> list[dict]:
    """Parse a BrokerSnapshot CSV export into a list of record dicts.
    Handles two CSV formats:
    1. Human-readable headers (e.g. "Legal Name", "DOT#")
    2. Raw internal headers (e.g. "name", "dot_number", "phy_str")
    """
    # Decode with BOM handling
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        print("[BrokerSnapshot] CSV has no headers")
        return []
    # Clean BOM from fieldnames
    reader.fieldnames = [f.lstrip("\ufeff") for f in reader.fieldnames]
    print(f"[BrokerSnapshot] CSV has {len(reader.fieldnames)} columns")
    print(f"[BrokerSnapshot] First 10 columns: {reader.fieldnames[:10]}")
    is_raw = _has_raw_columns(reader.fieldnames)
    print(f"[BrokerSnapshot] Raw format detected: {is_raw}")
    # Build column mapping for directly-mappable columns
    col_map: dict[str, str] = {}
    for raw_col in reader.fieldnames:
        db_col = _normalise_column(raw_col)
        if db_col and db_col in _VALID_COLUMNS:
            col_map[raw_col] = db_col
    print(f"[BrokerSnapshot] Mapped {len(col_map)} columns to DB fields")
    records: list[dict] = []
    for row in reader:
        record: dict[str, str] = {}
        # Map directly-mappable columns
        for csv_col, db_col in col_map.items():
            record[db_col] = _clean_val(row.get(csv_col, ""))
        if is_raw:
            # Combine address fields if not already mapped
            if not record.get("physical_address"):
                addr = _combine_address(row, _PHY_ADDR_COLS)
                if addr:
                    record["physical_address"] = addr
            if not record.get("mailing_address"):
                addr = _combine_address(row, _MAI_ADDR_COLS)
                if addr:
                    record["mailing_address"] = addr
            # Build cargo from boolean columns
            if not record.get("cargo_carried"):
                cargo = _collect_cargo(row)
                if cargo:
                    record["cargo_carried"] = cargo
            # Build operation classification from boolean columns
            if not record.get("operation_classification"):
                op_class = _collect_op_classification(row)
                if op_class:
                    record["operation_classification"] = op_class
            # Build MC number from prefix + docket_number
            if not record.get("mc_number"):
                mc = _build_mc_number(row)
                if mc:
                    record["mc_number"] = mc
            # Grand total drivers (use total_drivers if not already set)
            if not record.get("grand_total_drivers") and record.get("drivers"):
                record["grand_total_drivers"] = record["drivers"]
            # total_power_units from total_pwr
            if not record.get("total_power_units") and record.get("power_units"):
                record["total_power_units"] = record["power_units"]
        # Ensure date_added is set
        if not record.get("date_added"):
            record["date_added"] = added_date
        # Skip rows without a DOT number (invalid)
        if not record.get("dot_number"):
            continue
        records.append(record)
    print(f"[BrokerSnapshot] Parsed {len(records)} valid records")
    return records
def _try_login_with_proxy(
    proxy_url: Optional[str],
) -> tuple[Optional[curl_requests.Session], Optional[str], bool]:
    """Attempt to login to BrokerSnapshot using a specific proxy.
    Returns:
        (session, None, False) on success.
        (None, error_reason, False) on real failure (bad credentials).
        (None, None, True) if the proxy is blocked (should try next).
    """
    proxies: dict = {}
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}
    session = curl_requests.Session(impersonate="chrome")
    # 1. Get login page
    try:
        login_page = session.get(f"{BASE_URL}/LogIn", proxies=proxies, timeout=20)
    except Exception as e:
        return None, f"Connection error: {e}", False
    # Check for Cloudflare block (403)
    if login_page.status_code == 403 or "Attention Required" in login_page.text:
        return None, None, True  # Signal to try next proxy
    # Check for CAPTCHA page (no email field = CAPTCHA-only page)
    has_email_field = bool(re.findall(r'name="email"', login_page.text))
    if not has_email_field:
        if "recaptcha" in login_page.text.lower():
            return None, None, True  # CAPTCHA page, try next proxy
        return None, "Login page has no email field and no CAPTCHA", False
    # Extract __AntiForgeryToken
    token_match = re.findall(
        r'name="__AntiForgeryToken"[^>]*value="([^"]*)"', login_page.text
    )
    if not token_match:
        token_match = re.findall(
            r'value="([^"]*)"[^>]*name="__AntiForgeryToken"', login_page.text
        )
    anti_forgery_token = token_match[0] if token_match else ""
    # 2. Login
    login_data: dict[str, str] = {
        "email": BS_EMAIL,
        "password": BS_PASSWORD,
        "ReturnUrl": "",
    }
    if anti_forgery_token:
        login_data["__AntiForgeryToken"] = anti_forgery_token
    resp = session.post(
        f"{BASE_URL}/LogIn",
        data=login_data,
        proxies=proxies,
        allow_redirects=True,
        timeout=20,
    )
    if "LogOff" not in resp.text:
        # Check if it's a proxy/block issue vs credentials
        if resp.status_code == 403 or "Attention Required" in resp.text:
            return None, None, True  # Blocked, try next proxy
        return None, "Login failed. Check credentials or proxy.", False
    return session, None, False
def _scrape_broker_snapshot_sync(
    added_date: str,
    on_progress: Optional[callable] = None,
) -> dict:
    """Synchronous BrokerSnapshot scraper using curl_cffi with Chrome TLS impersonation.
    Tries multiple proxies to find one that isn't blocked by Cloudflare/CAPTCHA.
    Args:
        added_date: Date string in YYYY-MM-DD format.
        on_progress: Optional callback(percent: int, message: str).
    Returns:
        dict with keys: success, count, records, error
    """
    if not BS_EMAIL or not BS_PASSWORD:
        return {
            "success": False,
            "error": "BrokerSnapshot credentials not configured. "
                     "Set BROKER_SNAPSHOT_EMAIL and BROKER_SNAPSHOT_PASSWORD environment variables.",
            "count": 0,
            "records": [],
        }
    if on_progress:
        on_progress(5, "Connecting to BrokerSnapshot...")
    # Get all proxy URLs (shuffled) and also try without proxy as last resort
    proxy_urls: list[Optional[str]] = _get_all_proxy_urls()
    if not proxy_urls:
        proxy_urls = [None]  # No proxies configured, try direct
    else:
        proxy_urls.append(None)  # Also try direct as last resort
    # Try each proxy until one works
    session = None
    last_error = "All proxies failed or blocked"
    proxies_used: dict = {}
    for i, proxy_url in enumerate(proxy_urls):
        proxy_label = proxy_url.split("@")[-1] if proxy_url else "direct"
        print(f"[BrokerSnapshot] Trying proxy {i+1}/{len(proxy_urls)}: {proxy_label}")
        if on_progress:
            on_progress(5 + i, f"Trying proxy {i+1}/{len(proxy_urls)}...")
        sess, error, is_blocked = _try_login_with_proxy(proxy_url)
        if sess is not None:
            # Login succeeded
            session = sess
            proxies_used = {"http": proxy_url, "https": proxy_url} if proxy_url else {}
            print(f"[BrokerSnapshot] Login successful via {proxy_label}")
            break
        elif is_blocked:
            # Proxy blocked, try next
            print(f"[BrokerSnapshot] Proxy {proxy_label} blocked, trying next...")
            continue
        else:
            # Real error (bad credentials, etc.)
            last_error = error or "Unknown login error"
            print(f"[BrokerSnapshot] Login error via {proxy_label}: {last_error}")
            continue
    if session is None:
        return {
            "success": False,
            "error": f"BrokerSnapshot login failed. {last_error}",
            "count": 0,
            "records": [],
        }
    try:
        if on_progress:
            on_progress(20, "Login successful. Triggering export...")
        # 3. Trigger export
        resp = session.get(
            f"{BASE_URL}/SearchCompanies/GenerateExport",
            params={"added-date": added_date},
            proxies=proxies_used,
            timeout=30,
        )
        result = resp.json()
        if not result.get("Success"):
            return {
                "success": False,
                "error": f"Export failed: {result.get('Message', 'Unknown error')}",
                "count": 0,
                "records": [],
            }
        if on_progress:
            on_progress(30, "Export started, polling for completion...")
        # 4. Poll until ready
        key = None
        max_polls = 60
        for poll in range(max_polls):
            resp = session.get(
                f"{BASE_URL}/SearchCompanies/GetStatusExport",
                params={"added-date": added_date},
                proxies=proxies_used,
                timeout=20,
            )
            d = resp.json().get("Data")
            if d and d.get("FileName"):
                key = d["FileName"]
                break
            pct = d.get("Percent", 0) if d else 0
            if on_progress:
                on_progress(30 + int(pct * 0.4), f"Generating export... {pct}%")
            time.sleep(3)
        if not key:
            return {
                "success": False,
                "error": "Export timed out. BrokerSnapshot did not generate the file in time.",
                "count": 0,
                "records": [],
            }
        if on_progress:
            on_progress(75, "Downloading CSV...")
        # 5. Download CSV
        resp = session.get(
            f"{BASE_URL}/SearchCompanies/DownloadExport",
            params={"Key": key},
            proxies=proxies_used,
            timeout=60,
        )
        csv_bytes = resp.content
        if not csv_bytes or len(csv_bytes) < 50:
            return {
                "success": False,
                "error": "Downloaded file is empty or too small.",
                "count": 0,
                "records": [],
            }
        print(f"[BrokerSnapshot] Downloaded {len(csv_bytes):,} bytes")
        if on_progress:
            on_progress(85, "Parsing CSV data...")
        # 6. Parse CSV
        records = parse_csv(csv_bytes, added_date)
        if on_progress:
            on_progress(100, f"Done! {len(records)} records parsed.")
        return {
            "success": True,
            "count": len(records),
            "records": records,
        }
    except Exception as e:
        print(f"[BrokerSnapshot] Scrape error: {e}")
        return {
            "success": False,
            "error": f"Scrape failed: {str(e)}",
            "count": 0,
            "records": [],
        }
async def scrape_broker_snapshot(
    added_date: str,
    on_progress: Optional[callable] = None,
) -> dict:
    """Async wrapper that runs the synchronous curl_cffi scraper in a thread.
    curl_cffi is synchronous, so we offload the blocking I/O to a thread
    to keep the FastAPI event loop responsive.
    """
    return await asyncio.to_thread(
        _scrape_broker_snapshot_sync, added_date, on_progress
    )
