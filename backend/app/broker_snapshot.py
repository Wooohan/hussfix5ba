import asyncio
import csv
import io
import os
import random
import time
from functools import partial

from curl_cffi import requests as curl_requests

# ── credentials ────────────────────────────────────────────────────────────
BS_EMAIL = os.getenv("BROKER_SNAPSHOT_EMAIL", "")
BS_PASSWORD = os.getenv("BROKER_SNAPSHOT_PASSWORD", "")

BASE_URL = "https://brokersnapshot.com"

# ── proxy configuration ───────────────────────────────────────────────────
# Comma-separated list of proxy host:port pairs
# e.g. "31.59.20.176:6754,23.95.150.145:6114,198.23.239.134:6540"
PROXY_LIST_RAW = os.getenv("BROKER_SNAPSHOT_PROXY_LIST", "")
PROXY_USER = os.getenv("BROKER_SNAPSHOT_PROXY_USER", "")
PROXY_PASS = os.getenv("BROKER_SNAPSHOT_PROXY_PASS", "")

PROXY_HOSTS: list[str] = [
    h.strip() for h in PROXY_LIST_RAW.split(",") if h.strip()
]

# Max number of proxy rotation attempts before giving up
MAX_PROXY_RETRIES = int(os.getenv("BROKER_SNAPSHOT_MAX_RETRIES", "5"))


def _get_proxy_url(host: str) -> str:
    """Return a proxy URL string for a specific host."""
    return f"http://{PROXY_USER}:{PROXY_PASS}@{host}"


def _create_session(proxy_host: str | None = None) -> curl_requests.Session:
    """Create a curl_cffi session that impersonates Chrome, optionally with a proxy."""
    session = curl_requests.Session(impersonate="chrome")
    if proxy_host:
        proxy_url = _get_proxy_url(proxy_host)
        session.proxies = {"http": proxy_url, "https": proxy_url}
    return session


# ── column-name mapping from CSV headers to DB fields ──────────────────────
# BrokerSnapshot exports use lowercase snake_case headers (e.g. dot_number,
# name, phy_phone).  We also keep the human-readable variants so the parser
# works regardless of which format the site returns.
_CSV_TO_DB: dict[str, str] = {
    # ── DOT / MC ──
    "DOT Number": "dot_number",
    "USDOT Number": "dot_number",
    "DOT": "dot_number",
    "dot_number": "dot_number",
    "MC/MX Number": "mc_number",
    "MC Number": "mc_number",
    "MC/MX/FF Number(s)": "mc_number",
    "docket_number": "mc_number",
    # ── names ──
    "Legal Name": "company_name",
    "Company Name": "company_name",
    "name": "company_name",
    "DBA Name": "dba_name",
    "name_dba": "dba_name",
    # ── entity / status ──
    "Entity Type": "entity_type",
    "Operating Status": "operating_status",
    "OperatingStatus": "operating_status",
    # ── addresses ──
    "Physical Address": "physical_address",
    "phy_str": "physical_address",
    "Mailing Address": "mailing_address",
    "mai_str": "mailing_address",
    "State": "state",
    "phy_st": "state",
    "phy_city": "physical_city",
    "phy_zip": "physical_zip",
    "phy_country": "physical_country",
    "mai_city": "mailing_city",
    "mai_st": "mailing_state",
    "mai_zip": "mailing_zip",
    "mai_country": "mailing_country",
    # ── contact ──
    "Phone": "phone",
    "phy_phone": "phone",
    "Email": "email",
    "email_address": "email",
    "cell_phone": "cell_phone",
    "phy_fax": "fax",
    # ── officers ──
    "company_officer_1": "company_officer_1",
    "company_officer_2": "company_officer_2",
    # ── dates ──
    "Added Date": "added_date",
    "add_date": "added_date",
    "MCS-150 Date": "mcs150_date",
    "mcs150_date": "mcs150_date",
    "MCS-150 Mileage": "mcs150_mileage",
    "mcs150_mileage": "mcs150_mileage",
    "MCS-150 Mileage (Year)": "mcs150_mileage_year",
    "MCS150 Mileage Year": "mcs150_mileage_year",
    "mcs150_mileage_year": "mcs150_mileage_year",
    # ── hazmat / safety ──
    "Hazmat Indicator": "hazmat_indicator",
    "Hazmat": "hazmat_indicator",
    "hm_ind": "hazmat_indicator",
    "Safety Rating Type": "safety_rating_type",
    "safety_rating": "safety_rating_type",
    "Safety Rating Date": "safety_rating_date",
    "Safety Rating Effective Date": "safety_rating_date",
    "safety_rating_date": "safety_rating_date",
    "Safety Review Type": "safety_review_type",
    "Latest Review Type": "safety_review_type",
    "review_type": "safety_review_type",
    "Safety Review Date": "safety_review_date",
    "Latest Review Date": "safety_review_date",
    "review_date": "safety_review_date",
    # ── drivers ──
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
    "Avg Trip Leased Drivers/Month": "avg_trip_leased_drivers",
    "Avg Number Trip Leased Drivers / Month": "avg_trip_leased_drivers",
    "avg_tld": "avg_trip_leased_drivers",
    "Grand Total Drivers": "grand_total_drivers",
    "Grand Total (Interstate and Intrastate)": "grand_total_drivers",
    "total_drivers": "grand_total_drivers",
    "Total CDL": "total_cdl",
    "Total with CDL": "total_cdl",
    "total_cdl": "total_cdl",
    # ── fleet ──
    "Total Trucks": "total_trucks",
    "Total Number of Trucks": "total_trucks",
    "total_trucks": "total_trucks",
    "Total Power Units": "total_power_units",
    "Total Number of Power Units": "total_power_units",
    "total_pwr": "total_power_units",
    "Fleet Size Code": "fleet_size_code",
    "fleetsize": "fleet_size_code",
    "Owned Truck": "owned_truck",
    "owntruck": "owned_truck",
    "Term Leased Truck": "term_leased_truck",
    "trmtruck": "term_leased_truck",
    "Trip Leased Truck": "trip_leased_truck",
    "trptruck": "trip_leased_truck",
    "Owned Tractor": "owned_tractor",
    "owntract": "owned_tractor",
    "Term Leased Tractor": "term_leased_tractor",
    "trmtract": "term_leased_tractor",
    "Trip Leased Tractor": "trip_leased_tractor",
    "trptract": "trip_leased_tractor",
    "Owned Trailer": "owned_trailer",
    "owntrail": "owned_trailer",
    "Term Leased Trailer": "term_leased_trailer",
    "trmtrail": "term_leased_trailer",
    "Trip Leased Trailer": "trip_leased_trailer",
    "trptrail": "trip_leased_trailer",
    # ── accident rates ──
    "Recordable Accident Rate": "recordable_accident_rate",
    "recordable_crash_rate": "recordable_accident_rate",
    "Preventable Recordable Accident Rate": "preventable_accident_rate",
    "Preventable Accident Rate": "preventable_accident_rate",
    # ── operation / cargo ──
    "Operation Classification": "operation_classification",
    "Carrier Operation": "carrier_operation",
    "carrier_operation": "carrier_operation",
    "Cargo Carried": "cargo_carried",
    "Cargo Transported": "cargo_carried",
    # ── authority status columns ──
    "status_code": "status_code",
    "carship": "carship",
    "broker_stat": "broker_stat",
    "common_stat": "common_stat",
    "contract_stat": "contract_stat",
    # ── insurance requirements ──
    "bipd_req": "bipd_required",
    "cargo_req": "cargo_required",
    "bond_req": "bond_required",
    "bipd_file": "bipd_filed",
    "cargo_file": "cargo_filed",
    "bond_file": "bond_filed",
}

# Fields that are stored as TEXT[] in Postgres
_ARRAY_FIELDS = {"operation_classification", "carrier_operation", "cargo_carried"}

# Fields that are boolean
_BOOL_FIELDS = {"hazmat_indicator"}


def _normalise_value(db_field: str, raw: str) -> object:
    """Convert a raw CSV string to the correct Python type for *db_field*."""
    val = raw.strip() if raw else ""
    if not val or val in ("-", "--", "N/A", "n/a"):
        if db_field in _ARRAY_FIELDS:
            return []
        if db_field in _BOOL_FIELDS:
            return False
        return ""

    if db_field in _ARRAY_FIELDS:
        # CSV may use pipe, comma, or semicolon
        for sep in ("|", ";"):
            if sep in val:
                return [s.strip() for s in val.split(sep) if s.strip()]
        return [val]

    if db_field in _BOOL_FIELDS:
        return val.lower() in ("yes", "true", "1", "y")

    return val


def _parse_csv(csv_bytes: bytes, added_date: str) -> list[dict]:
    """Parse downloaded CSV bytes into a list of DB-ready dicts."""
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    records: list[dict] = []
    for row in reader:
        record: dict = {}
        for csv_col, db_field in _CSV_TO_DB.items():
            if csv_col in row:
                record[db_field] = _normalise_value(db_field, row[csv_col])

        # Ensure dot_number is present (required)
        if not record.get("dot_number"):
            continue

        # Default company_name if missing
        if not record.get("company_name"):
            record["company_name"] = "Unknown"

        # Ensure array fields exist
        for af in _ARRAY_FIELDS:
            if af not in record:
                record[af] = []

        # Ensure bool fields exist
        for bf in _BOOL_FIELDS:
            if bf not in record:
                record[bf] = False

        # Tag with the requested added_date if not already set
        if not record.get("added_date"):
            record["added_date"] = added_date

        records.append(record)

    return records


# ── login with proxy rotation ─────────────────────────────────────────────

def _try_login(session: curl_requests.Session) -> tuple[bool, str]:
    """Attempt to log in to BrokerSnapshot. Returns (success, error_msg)."""
    try:
        login_page = session.get(f"{BASE_URL}/LogIn", timeout=30)
        print(f"[BrokerSnapshot] Login page status: {login_page.status_code}, length: {len(login_page.text)}")

        if login_page.status_code == 403:
            return False, f"Login page blocked by Cloudflare (403)"

        login_resp = session.post(
            f"{BASE_URL}/LogIn",
            data={
                "email": BS_EMAIL,
                "password": BS_PASSWORD,
                "ReturnUrl": "",
            },
            allow_redirects=True,
            timeout=30,
        )

        if login_resp.status_code == 403:
            return False, f"Login POST blocked by Cloudflare (403)"

        if "LogOff" in login_resp.text:
            return True, ""

        snippet = login_resp.text[:300] if login_resp.text else "(empty)"
        return False, f"Login failed. Status: {login_resp.status_code}, URL: {login_resp.url}, Body preview: {snippet}"

    except Exception as exc:
        return False, f"Login exception: {exc}"


def _login_with_retry() -> tuple[curl_requests.Session | None, str]:
    """Try logging in with different proxies until one works.
    Returns (session, error_msg). session is None on total failure.
    """
    if not PROXY_HOSTS or not PROXY_USER or not PROXY_PASS:
        # No proxies configured — try direct connection
        print("[BrokerSnapshot] WARNING: No proxy configured, using direct connection")
        session = _create_session()
        ok, err = _try_login(session)
        if ok:
            return session, ""
        return None, err

    # Shuffle proxies and try each one
    hosts = list(PROXY_HOSTS)
    random.shuffle(hosts)
    retries = min(MAX_PROXY_RETRIES, len(hosts))
    errors: list[str] = []

    for i in range(retries):
        proxy_host = hosts[i]
        print(f"[BrokerSnapshot] Attempt {i+1}/{retries} with proxy: {proxy_host}")

        session = _create_session(proxy_host)
        ok, err = _try_login(session)

        if ok:
            print(f"[BrokerSnapshot] Logged in via proxy {proxy_host}")
            return session, ""

        errors.append(f"Proxy {proxy_host}: {err}")
        print(f"[BrokerSnapshot] Attempt {i+1} failed: {err}")

        # Wait a bit before retrying with next proxy
        if i < retries - 1:
            time.sleep(2)

    all_errors = "; ".join(errors)
    return None, f"All {retries} proxy attempts failed. Details: {all_errors}"


# ── synchronous fetch (runs in thread) ─────────────────────────────────────

def _fetch_sync(added_date: str) -> dict:
    """Synchronous version using curl_cffi + proxy rotation to bypass Cloudflare."""

    # Step 1: Login with proxy rotation
    session, login_err = _login_with_retry()
    if session is None:
        return {"success": False, "error": login_err}

    print(f"[BrokerSnapshot] Logged in as {BS_EMAIL}")

    try:
        # 2. Trigger export
        export_resp = session.get(
            f"{BASE_URL}/SearchCompanies/GenerateExport",
            params={"added-date": added_date},
            timeout=30,
        )
        export_data = export_resp.json()
        if not export_data.get("Success"):
            msg = export_data.get("Message", "Unknown error")
            return {"success": False, "error": f"Export failed: {msg}"}

        print(f"[BrokerSnapshot] Export started for {added_date}")

        # 3. Poll until the file is ready (max ~5 minutes)
        filename = None
        for _ in range(150):
            time.sleep(2)
            status_resp = session.get(
                f"{BASE_URL}/SearchCompanies/GetStatusExport",
                params={"added-date": added_date},
                timeout=30,
            )
            status_data = status_resp.json().get("Data")
            if status_data and status_data.get("FileName"):
                filename = status_data["FileName"]
                break
            if status_data and status_data.get("Percent"):
                print(f"[BrokerSnapshot]   {status_data['Percent']}%...")

        if not filename:
            return {"success": False, "error": "Export timed out after 5 minutes."}

        # 4. Download CSV
        dl_resp = session.get(
            f"{BASE_URL}/SearchCompanies/DownloadExport",
            params={"Key": filename},
            timeout=60,
        )
        csv_bytes = dl_resp.content
        print(f"[BrokerSnapshot] Downloaded {len(csv_bytes):,} bytes")

        # 5. Parse
        records = _parse_csv(csv_bytes, added_date)
        print(f"[BrokerSnapshot] Parsed {len(records)} records")

        return {"success": True, "records": records, "count": len(records)}

    except Exception as exc:
        return {"success": False, "error": f"Unexpected error: {exc}"}


# ── public async entry-point ───────────────────────────────────────────────

async def fetch_broker_snapshot(added_date: str) -> dict:
    """Fetch the broker-snapshot CSV for *added_date* and return parsed records.

    Returns ``{"success": True, "records": [...], "count": N}`` on success,
    or ``{"success": False, "error": "..."}`` on failure.
    """
    if not BS_EMAIL or not BS_PASSWORD:
        return {
            "success": False,
            "error": (
                "Broker Snapshot credentials not configured. "
                "Set BROKER_SNAPSHOT_EMAIL and BROKER_SNAPSHOT_PASSWORD env vars."
            ),
        }

    # Run the synchronous curl_cffi call in a thread to avoid blocking
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(_fetch_sync, added_date))
