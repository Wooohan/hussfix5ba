"""BrokerSnapshot.com scraper for New Ventures data.

Fetches newly-added carrier data from BrokerSnapshot by date,
downloads the CSV export, parses it, and returns structured records.
"""

import os
import csv
import io
import asyncio
import time
import random
from typing import Optional

import httpx

# ── Configuration (from environment) ─────────────────────────────────────────
BS_EMAIL = os.getenv("BROKER_SNAPSHOT_EMAIL", "")
BS_PASSWORD = os.getenv("BROKER_SNAPSHOT_PASSWORD", "")

BS_PROXIES = os.getenv("BROKER_SNAPSHOT_PROXIES", "")  # comma-separated host:port
BS_PROXY_USER = os.getenv("BROKER_SNAPSHOT_PROXY_USER", "")
BS_PROXY_PASS = os.getenv("BROKER_SNAPSHOT_PROXY_PASS", "")

BASE_URL = "https://brokersnapshot.com"

# ── CSV column → DB field mapping ────────────────────────────────────────────
# BrokerSnapshot CSV columns vary but typically include these.
# We normalise them to snake_case DB column names.
_COLUMN_MAP: dict[str, str] = {
    "DOT#": "dot_number",
    "DOT #": "dot_number",
    "USDOT": "dot_number",
    "USDOT Number": "dot_number",
    "USDOT#": "dot_number",
    "MC#": "mc_number",
    "MC #": "mc_number",
    "MC/MX#": "mc_number",
    "MC/MX #": "mc_number",
    "MC Number": "mc_number",
    "Legal Name": "legal_name",
    "Company Name": "legal_name",
    "DBA Name": "dba_name",
    "DBA": "dba_name",
    "Entity Type": "entity_type",
    "Operating Status": "status",
    "Status": "status",
    "Email": "email",
    "Phone": "phone",
    "Phone Number": "phone",
    "Physical Address": "physical_address",
    "Mailing Address": "mailing_address",
    "Power Units": "power_units",
    "Drivers": "drivers",
    "Total Drivers": "drivers",
    "Cargo Carried": "cargo_carried",
    "Cargo Transported": "cargo_carried",
    "Hazmat": "hazmat_indicator",
    "Hazmat Indicator": "hazmat_indicator",
    "HM Indicator": "hazmat_indicator",
    "Operation Classification": "operation_classification",
    "Op. Classification": "operation_classification",
    "Carrier Operation": "carrier_operation",
    "Safety Rating": "safety_rating",
    "Safety Rating Type Code": "safety_rating_type_code",
    "Safety Type Code": "safety_rating_type_code",
    "Safety Rating Date": "safety_rating_effective_date",
    "Safety Effective Date": "safety_rating_effective_date",
    "Latest Review Type": "safety_rating_latest_review_type",
    "Latest Review Date": "safety_rating_latest_review_date",
    "MCSIP Step": "mcsip_step_number",
    "MCSIP Step Number": "mcsip_step_number",
    "Interstate Within 100 Miles": "interstate_within_100",
    "Interstate Beyond 100 Miles": "interstate_beyond_100",
    "Interstate Total": "interstate_total",
    "Intrastate Within 100 Miles": "intrastate_within_100",
    "Intrastate Beyond 100 Miles": "intrastate_beyond_100",
    "Intrastate Total": "intrastate_total",
    "Avg Trip Leased Drivers": "avg_trip_leased_drivers",
    "Grand Total": "grand_total_drivers",
    "Grand Total Drivers": "grand_total_drivers",
    "Grand Total (Interstate and Intrastate)": "grand_total_drivers",
    "Total CDL": "total_cdl",
    "Total with CDL": "total_cdl",
    "Total Non-CDL": "total_non_cdl",
    "Total with Non-CDL": "total_non_cdl",
    "Total Trucks": "total_trucks",
    "Total Number of Trucks": "total_trucks",
    "Total Power Units": "total_power_units",
    "Total Number of Power Units": "total_power_units",
    "Fleet Size Code": "fleet_size_code",
    "Fleet Size": "fleet_size_code",
    "Owned Truck": "owned_trucks",
    "Owned Trucks": "owned_trucks",
    "Term Leased Truck": "term_leased_trucks",
    "Term Leased Trucks": "term_leased_trucks",
    "Trip Leased Truck": "trip_leased_trucks",
    "Trip Leased Trucks": "trip_leased_trucks",
    "Owned Tractor": "owned_tractors",
    "Owned Tractors": "owned_tractors",
    "Term Leased Tractor": "term_leased_tractors",
    "Term Leased Tractors": "term_leased_tractors",
    "Trip Leased Tractor": "trip_leased_tractors",
    "Trip Leased Tractors": "trip_leased_tractors",
    "Owned Trailer": "owned_trailers",
    "Owned Trailers": "owned_trailers",
    "Term Leased Trailer": "term_leased_trailers",
    "Term Leased Trailers": "term_leased_trailers",
    "Trip Leased Trailer": "trip_leased_trailers",
    "Trip Leased Trailers": "trip_leased_trailers",
    "Recordable Accident Rate": "recordable_accident_rate",
    "Preventable Recordable Accident Rate": "preventable_accident_rate",
    "MCS-150 Mileage": "mcs150_mileage",
    "MCS-150 Mileage (Year)": "mcs150_mileage_year",
    "MCS-150 Mileage Year": "mcs150_mileage_year",
    "MCS-150 Date": "mcs150_date",
    "MCS-150 date": "mcs150_date",
    "MCS-150 Form Date": "mcs150_date",
    "Added Date": "date_added",
    "Date Added": "date_added",
    "State Carrier ID": "state_carrier_id",
    "DUNS Number": "duns_number",
    "Out of Service Date": "out_of_service_date",
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


def _get_proxy_url() -> Optional[str]:
    """Build a random proxy URL from environment config, or None."""
    if not BS_PROXIES or not BS_PROXY_USER:
        return None
    hosts = [h.strip() for h in BS_PROXIES.split(",") if h.strip()]
    if not hosts:
        return None
    host = random.choice(hosts)
    return f"http://{BS_PROXY_USER}:{BS_PROXY_PASS}@{host}"


def _normalise_column(raw: str) -> Optional[str]:
    """Map a raw CSV header to a DB column name."""
    stripped = raw.strip()
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


def parse_csv(csv_bytes: bytes, added_date: str) -> list[dict]:
    """Parse a BrokerSnapshot CSV export into a list of record dicts."""
    text = csv_bytes.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        return []

    # Build column mapping for this specific CSV
    col_map: dict[str, str] = {}
    for raw_col in reader.fieldnames:
        db_col = _normalise_column(raw_col)
        if db_col and db_col in _VALID_COLUMNS:
            col_map[raw_col] = db_col

    records: list[dict] = []
    for row in reader:
        record: dict[str, str] = {}
        for csv_col, db_col in col_map.items():
            record[db_col] = _clean_val(row.get(csv_col, ""))

        # Ensure date_added is set
        if not record.get("date_added"):
            record["date_added"] = added_date

        # Skip rows without a DOT number (invalid)
        if not record.get("dot_number"):
            continue

        records.append(record)

    return records


async def scrape_broker_snapshot(
    added_date: str,
    on_progress: Optional[callable] = None,
) -> dict:
    """Scrape BrokerSnapshot for carriers added on a specific date.

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

    proxy_url = _get_proxy_url()
    proxy_kwarg: dict = {}
    if proxy_url:
        proxy_kwarg["proxy"] = proxy_url

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    try:
        async with httpx.AsyncClient(
            timeout=120.0,
            follow_redirects=True,
            headers=headers,
            **proxy_kwarg,
        ) as client:
            if on_progress:
                on_progress(5, "Connecting to BrokerSnapshot...")

            # 1. Get login page
            await client.get(f"{BASE_URL}/LogIn")

            if on_progress:
                on_progress(10, "Logging in...")

            # 2. Login
            login_data = {
                "email": BS_EMAIL,
                "password": BS_PASSWORD,
                "ReturnUrl": "",
            }
            resp = await client.post(f"{BASE_URL}/LogIn", data=login_data)
            if "LogOff" not in resp.text:
                return {
                    "success": False,
                    "error": "BrokerSnapshot login failed. Check credentials.",
                    "count": 0,
                    "records": [],
                }

            if on_progress:
                on_progress(20, "Login successful. Triggering export...")

            # 3. Trigger export
            resp = await client.get(
                f"{BASE_URL}/SearchCompanies/GenerateExport",
                params={"added-date": added_date},
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
                resp = await client.get(
                    f"{BASE_URL}/SearchCompanies/GetStatusExport",
                    params={"added-date": added_date},
                )
                d = resp.json().get("Data")
                if d and d.get("FileName"):
                    key = d["FileName"]
                    break
                pct = d.get("Percent", 0) if d else 0
                if on_progress:
                    on_progress(30 + int(pct * 0.4), f"Generating export... {pct}%")
                await asyncio.sleep(3)

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
            resp = await client.get(
                f"{BASE_URL}/SearchCompanies/DownloadExport",
                params={"Key": key},
            )
            csv_bytes = resp.content
            if not csv_bytes or len(csv_bytes) < 50:
                return {
                    "success": False,
                    "error": "Downloaded file is empty or too small.",
                    "count": 0,
                    "records": [],
                }

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
        return {
            "success": False,
            "error": f"Scrape failed: {str(e)}",
            "count": 0,
            "records": [],
        }
