import asyncio
from urllib.parse import urlparse

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

from app.scraper import scrape_carrier, fetch_safety_data, fetch_insurance_data, close_clients
from app.task_manager import task_manager
from app.fmcsa_register import scrape_fmcsa_register
from app.auth import create_token, verify_token, require_auth
from app.database import (
    connect_db, close_db,
    upsert_carrier, fetch_carriers, delete_carrier as db_delete_carrier,
    get_carrier_count, update_carrier_insurance as db_update_carrier_insurance,
    update_carrier_safety as db_update_carrier_safety, get_carriers_by_mc_range,
    fetch_users, fetch_user_by_email, create_user, update_user, delete_user,
    get_user_password_hash,
    fetch_blocked_ips, block_ip, unblock_ip, is_ip_blocked,
    save_fmcsa_register_entries, fetch_fmcsa_register_by_date,
    get_fmcsa_extracted_dates, get_fmcsa_categories, delete_fmcsa_entries_before_date,
)


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Connect to PostgreSQL on startup, disconnect on shutdown."""
    await connect_db()
    yield
    await close_clients()
    await close_db()


app = FastAPI(lifespan=lifespan)

_PUBLIC_PATHS: set[str] = {
    "/health",
    "/healthz",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/api/get-ip",
    "/api/auth/login",
    "/api/auth/register",
    "/api/users/verify-password",
    "/api/blocked-ips/check",
}

_PUBLIC_PREFIXES: tuple[str, ...] = (
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/blocked-ips/check/",
)


class AuthMiddleware(BaseHTTPMiddleware):
    """Reject requests to protected paths that lack a valid JWT Bearer token."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        if request.method == "OPTIONS":
            return await call_next(request)

        if path in _PUBLIC_PATHS or any(path.startswith(p) for p in _PUBLIC_PREFIXES):
            return await call_next(request)

        user_payload = await require_auth(request)
        if user_payload is None:
            return JSONResponse(
                status_code=401,
                content={"error": "Authentication required. Please log in."},
            )

        request.state.user = user_payload
        return await call_next(request)


app.add_middleware(AuthMiddleware)

_cors_origins = os.getenv("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _cors_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_DOMAINS = {
    "safer.fmcsa.dot.gov",
    "ai.fmcsa.dot.gov",
    "searchcarriers.com",
    "www.searchcarriers.com",
    "li-public.fmcsa.dot.gov",
}

@app.get("/healthz")
async def healthz():
    try:
        from app.database import get_pool
        pool = get_pool()
        await pool.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": "database unreachable"})

@app.get("/health")
async def health():
    return {"status": "ok", "message": "FMCSA Scraper Backend is running"}

@app.get("/api/get-ip")
async def get_ip(request: Request):
    """Return the client's IP address (replaces Vercel serverless function)."""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        ip = forwarded.split(",")[0].strip()
    else:
        ip = request.headers.get("x-real-ip", "") or (request.client.host if request.client else "")
    return {"ip": ip}

@app.get("/api/proxy")
async def proxy(url: str = Query(...)):
    parsed = urlparse(url)
    if not parsed.hostname or parsed.hostname not in ALLOWED_DOMAINS:
        return JSONResponse(status_code=403, content={"error": "Domain not allowed"})
    try:
        if "searchcarriers.com" in url:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": "https://searchcarriers.com/",
                "Origin": "https://searchcarriers.com",
            }
        else:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
        content_type = resp.headers.get("content-type", "text/html; charset=utf-8")
        return PlainTextResponse(content=resp.text, status_code=resp.status_code, headers={
            "Content-Type": content_type,
            "Access-Control-Allow-Origin": "*",
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/fmcsa-register")
async def fmcsa_register(request: Request):
    body = await request.json()
    date_str = body.get("date", None)
    save_to_db = body.get("saveToDb", False)
    result = await scrape_fmcsa_register(date_str)
    if result.get("success") and save_to_db:
        entries = result.get("entries", [])
        extracted_date = result.get("date", "")
        db_result = await save_fmcsa_register_entries(entries, extracted_date)
        result["dbSaved"] = db_result.get("saved", 0)
        result["dbSkipped"] = db_result.get("skipped", 0)
    if result.get("success"):
        return result
    return JSONResponse(status_code=500 if "error" in result else 400, content=result)

@app.get("/api/fmcsa-register/entries")
async def get_fmcsa_entries(
    extracted_date: str = Query(...),
    category: str = Query(None),
    search: str = Query(None),
):
    """Fetch FMCSA register entries from database by date."""
    entries = await fetch_fmcsa_register_by_date(extracted_date, category, search)
    return {"success": True, "count": len(entries), "entries": entries}

@app.get("/api/fmcsa-register/dates")
async def get_fmcsa_dates():
    """Return all distinct date_fetched values stored in database."""
    dates = await get_fmcsa_extracted_dates()
    return {"success": True, "dates": dates}

@app.get("/api/scrape/carrier/{mc_number}")
async def scrape_single_carrier(mc_number: str):
    data = await scrape_carrier(mc_number)
    if data:
        return data
    return JSONResponse(status_code=404, content={"error": "No data found"})

@app.get("/api/scrape/safety/{dot_number}")
async def scrape_safety(dot_number: str):
    data = await fetch_safety_data(dot_number)
    return data

@app.get("/api/scrape/insurance/{dot_number}")
async def scrape_insurance(dot_number: str):
    data = await fetch_insurance_data(dot_number)
    return data

@app.post("/api/tasks/scraper/start")
async def start_scraper_task(request: Request):
    body = await request.json()
    config = body.get("config", {})
    task_id = await task_manager.start_scraper_task(config)
    return {"task_id": task_id, "status": "started"}

@app.post("/api/tasks/scraper/stop")
async def stop_scraper_task(request: Request):
    body = await request.json()
    task_id = body.get("task_id")
    if not task_id:
        return JSONResponse(status_code=400, content={"error": "task_id required"})
    task_manager.stop_task(task_id)
    return {"task_id": task_id, "status": "stopping"}

@app.get("/api/tasks/scraper/status")
async def get_scraper_status(task_id: str = Query(...)):
    status = task_manager.get_task_status(task_id)
    if not status:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    return status

@app.post("/api/tasks/insurance/start")
async def start_insurance_task(request: Request):
    body = await request.json()
    config = body.get("config", {})
    task_id = await task_manager.start_insurance_task(config)
    return {"task_id": task_id, "status": "started"}

@app.post("/api/tasks/insurance/stop")
async def stop_insurance_task(request: Request):
    body = await request.json()
    task_id = body.get("task_id")
    if not task_id:
        return JSONResponse(status_code=400, content={"error": "task_id required"})
    task_manager.stop_task(task_id)
    return {"task_id": task_id, "status": "stopping"}

@app.get("/api/tasks/insurance/status")
async def get_insurance_status(task_id: str = Query(...)):
    status = task_manager.get_task_status(task_id)
    if not status:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    return status

@app.get("/api/tasks/scraper/data")
async def get_scraper_data(task_id: str = Query(...)):
    data = task_manager.get_task_data(task_id)
    if data is None:
        return JSONResponse(status_code=404, content={"error": "Task not found"})
    return data

@app.get("/api/tasks/active")
async def get_active_task(task_type: str = Query("scraper")):
    task_id = task_manager.get_active_task_id(task_type)
    if not task_id:
        return {"task_id": None}
    status = task_manager.get_task_status(task_id)
    return {"task_id": task_id, "task": status}

@app.get("/api/tasks")
async def list_tasks():
    return task_manager.list_tasks()

@app.get("/api/carriers")
async def api_fetch_carriers(
    mc_number: str = Query(None),
    dot_number: str = Query(None),
    legal_name: str = Query(None),
    active: str = Query(None),
    state: str = Query(None),
    has_email: str = Query(None),
    has_boc3: str = Query(None),
    has_company_rep: str = Query(None),
    classification: str = Query(None),
    carrier_operation: str = Query(None),
    cargo: str = Query(None),
    hazmat: str = Query(None),
    power_units_min: str = Query(None),
    power_units_max: str = Query(None),
    drivers_min: str = Query(None),
    drivers_max: str = Query(None),
    insurance_required: str = Query(None),
    bipd_min: str = Query(None),
    bipd_max: str = Query(None),
    bipd_on_file: str = Query(None),
    cargo_on_file: str = Query(None),
    bond_on_file: str = Query(None),
    years_in_business_min: str = Query(None),
    years_in_business_max: str = Query(None),
    oos_min: str = Query(None),
    oos_max: str = Query(None),
    crashes_min: str = Query(None),
    crashes_max: str = Query(None),
    injuries_min: str = Query(None),
    injuries_max: str = Query(None),
    fatalities_min: str = Query(None),
    fatalities_max: str = Query(None),
    toway_min: str = Query(None),
    toway_max: str = Query(None),
    inspections_min: str = Query(None),
    inspections_max: str = Query(None),
    limit: int = Query(None),
):
    """Fetch carriers with optional filters."""
    filters = {}
    if mc_number: filters["mc_number"] = mc_number
    if dot_number: filters["dot_number"] = dot_number
    if legal_name: filters["legal_name"] = legal_name
    if active: filters["active"] = active
    if state: filters["state"] = state
    if has_email: filters["has_email"] = has_email
    if has_boc3: filters["has_boc3"] = has_boc3
    if has_company_rep: filters["has_company_rep"] = has_company_rep
    if classification: filters["classification"] = classification.split(",")
    if carrier_operation: filters["carrier_operation"] = carrier_operation.split(",")
    if cargo: filters["cargo"] = cargo.split(",")
    if hazmat: filters["hazmat"] = hazmat
    if power_units_min: filters["power_units_min"] = power_units_min
    if power_units_max: filters["power_units_max"] = power_units_max
    if drivers_min: filters["drivers_min"] = drivers_min
    if drivers_max: filters["drivers_max"] = drivers_max
    if insurance_required: filters["insurance_required"] = insurance_required.split(",")
    if bipd_min: filters["bipd_min"] = bipd_min
    if bipd_max: filters["bipd_max"] = bipd_max
    if bipd_on_file: filters["bipd_on_file"] = bipd_on_file
    if cargo_on_file: filters["cargo_on_file"] = cargo_on_file
    if bond_on_file: filters["bond_on_file"] = bond_on_file
    if years_in_business_min: filters["years_in_business_min"] = years_in_business_min
    if years_in_business_max: filters["years_in_business_max"] = years_in_business_max
    if oos_min: filters["oos_min"] = oos_min
    if oos_max: filters["oos_max"] = oos_max
    if crashes_min: filters["crashes_min"] = crashes_min
    if crashes_max: filters["crashes_max"] = crashes_max
    if injuries_min: filters["injuries_min"] = injuries_min
    if injuries_max: filters["injuries_max"] = injuries_max
    if fatalities_min: filters["fatalities_min"] = fatalities_min
    if fatalities_max: filters["fatalities_max"] = fatalities_max
    if toway_min: filters["toway_min"] = toway_min
    if toway_max: filters["toway_max"] = toway_max
    if inspections_min: filters["inspections_min"] = inspections_min
    if inspections_max: filters["inspections_max"] = inspections_max
    if limit is not None:
        filters["limit"] = limit
    else:
        has_filters = any(v for k, v in filters.items() if k != "limit")
        if not has_filters:
            filters["limit"] = 200
    data = await fetch_carriers(filters)
    return data


@app.post("/api/carriers")
async def api_upsert_carrier(request: Request):
    """Upsert a single carrier record."""
    body = await request.json()
    ok = await upsert_carrier(body)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=400, content={"success": False, "error": "Failed to upsert carrier"})


@app.post("/api/carriers/batch")
async def api_upsert_carriers_batch(request: Request):
    """Upsert multiple carrier records."""
    body = await request.json()
    carriers_list = body.get("carriers", [])
    saved = 0
    failed = 0
    batch_size = 10
    for i in range(0, len(carriers_list), batch_size):
        batch = carriers_list[i:i + batch_size]
        results = await asyncio.gather(
            *(upsert_carrier(c) for c in batch),
            return_exceptions=True,
        )
        for ok in results:
            if ok is True:
                saved += 1
            else:
                failed += 1
    return {"success": failed == 0, "saved": saved, "failed": failed}


@app.delete("/api/carriers/{mc_number}")
async def api_delete_carrier(mc_number: str):
    """Delete a carrier by MC number."""
    ok = await db_delete_carrier(mc_number)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "Carrier not found"})


@app.get("/api/carriers/count")
async def api_get_carrier_count():
    """Get the total carrier count."""
    count = await get_carrier_count()
    return {"count": count}


@app.put("/api/carriers/{dot_number}/insurance")
async def api_update_carrier_insurance(dot_number: str, request: Request):
    """Update insurance data for a carrier by DOT number."""
    body = await request.json()
    policies = body.get("policies", [])
    ok = await db_update_carrier_insurance(dot_number, policies)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "Carrier not found or update failed"})


@app.put("/api/carriers/{dot_number}/safety")
async def api_update_carrier_safety(dot_number: str, request: Request):
    """Update safety data for a carrier by DOT number."""
    body = await request.json()
    ok = await db_update_carrier_safety(dot_number, body)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "Carrier not found or update failed"})


@app.get("/api/carriers/range")
async def api_get_carriers_by_range(
    start: str = Query(...),
    end: str = Query(...),
):
    """Fetch carriers within a specific MC Number range."""
    data = await get_carriers_by_mc_range(start, end)
    return data


@app.post("/api/auth/login")
async def api_auth_login(request: Request):
    """Authenticate with email + password, return a JWT token."""
    body = await request.json()
    email = body.get("email", "").strip().lower()
    password = body.get("password", "")
    if not email or not password:
        return JSONResponse(status_code=400, content={"error": "Email and password are required"})

    stored_hash = await get_user_password_hash(email)
    if not stored_hash:
        return JSONResponse(status_code=401, content={"error": "Invalid email or password"})

    import bcrypt
    if stored_hash.startswith("$2b$") or stored_hash.startswith("$2a$"):
        if not bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8")):
            return JSONResponse(status_code=401, content={"error": "Invalid email or password"})
    else:
        return JSONResponse(status_code=401, content={"error": "Invalid email or password"})

    user = await fetch_user_by_email(email)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Invalid email or password"})

    token = create_token(
        user_id=user["user_id"],
        email=user["email"],
        role=user["role"],
    )
    return {
        "token": token,
        "user": {
            "user_id": user["user_id"],
            "name": user["name"],
            "email": user["email"],
            "role": user["role"],
            "plan": user["plan"],
            "daily_limit": user["daily_limit"],
            "records_extracted_today": user["records_extracted_today"],
            "last_active": user.get("last_active", "Never"),
            "ip_address": user.get("ip_address", ""),
            "is_online": user.get("is_online", False),
            "is_blocked": user.get("is_blocked", False),
        },
    }


@app.post("/api/auth/register")
async def api_auth_register(request: Request):
    """Register a new user and return a JWT token."""
    body = await request.json()
    email = body.get("email", "").strip().lower()
    password = body.get("password", "")
    name = body.get("name", "")
    if not email or not password or not name:
        return JSONResponse(status_code=400, content={"error": "Name, email, and password are required"})

    existing = await fetch_user_by_email(email)
    if existing:
        return JSONResponse(status_code=409, content={"error": "User with this email already exists"})

    import bcrypt
    import time as _time
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    user_data = {
        "user_id": body.get("user_id", f"user-{int(_time.time() * 1000)}"),
        "name": name,
        "email": email,
        "password_hash": password_hash,
        "role": "user",
        "plan": "Free",
        "daily_limit": 50,
        "records_extracted_today": 0,
        "last_active": "Now",
        "ip_address": body.get("ip_address", ""),
        "is_online": True,
        "is_blocked": False,
    }
    user = await create_user(user_data)
    if not user:
        return JSONResponse(status_code=500, content={"error": "Failed to create user"})

    token = create_token(
        user_id=user["user_id"],
        email=user["email"],
        role=user["role"],
    )
    user.pop("password_hash", None)
    return {"token": token, "user": user}


@app.get("/api/users")
async def api_fetch_users():
    """Fetch all users. Requires authentication."""
    users = await fetch_users()
    return users


@app.get("/api/users/by-email/{email:path}")
async def api_fetch_user_by_email(email: str):
    """Fetch a single user by email."""
    user = await fetch_user_by_email(email)
    if user:
        return user
    return JSONResponse(status_code=404, content={"error": "User not found"})


@app.post("/api/users")
async def api_create_user(request: Request):
    """Create a new user. Hashes password with bcrypt if provided."""
    body = await request.json()
    if body.get("password"):
        import bcrypt
        body["password_hash"] = bcrypt.hashpw(
            body.pop("password").encode("utf-8"),
            bcrypt.gensalt(),
        ).decode("utf-8")
    user = await create_user(body)
    if user:
        user.pop("password_hash", None)
        return user
    return JSONResponse(status_code=400, content={"error": "Failed to create user"})


@app.put("/api/users/{user_id}")
async def api_update_user(user_id: str, request: Request):
    """Update a user by user_id."""
    body = await request.json()
    ok = await update_user(user_id, body)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "User not found"})


@app.delete("/api/users/{user_id}")
async def api_delete_user(user_id: str):
    """Delete a user by user_id."""
    ok = await delete_user(user_id)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "User not found"})


@app.post("/api/users/verify-password")
async def api_verify_password(request: Request):
    """Verify a user password. Accepts plaintext password, verifies against bcrypt hash."""
    body = await request.json()
    email = body.get("email", "")
    password = body.get("password", "")
    password_hash_legacy = body.get("passwordHash", "")
    stored_hash = await get_user_password_hash(email)
    if not stored_hash:
        return {"valid": False}
    import bcrypt
    if stored_hash.startswith("$2b$") or stored_hash.startswith("$2a$"):
        if password:
            is_valid = bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
            return {"valid": is_valid}
        return {"valid": False}
    if password_hash_legacy and stored_hash == password_hash_legacy:
        return {"valid": True}
    return {"valid": False}


@app.get("/api/blocked-ips")
async def api_fetch_blocked_ips():
    """Fetch all blocked IPs."""
    ips = await fetch_blocked_ips()
    return ips


@app.post("/api/blocked-ips")
async def api_block_ip(request: Request):
    """Block an IP address."""
    body = await request.json()
    ip = body.get("ip_address", "")
    reason = body.get("reason", "No reason provided")
    ok = await block_ip(ip, reason)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=400, content={"success": False, "error": "Failed to block IP"})


@app.delete("/api/blocked-ips/{ip_address}")
async def api_unblock_ip(ip_address: str):
    """Unblock an IP address."""
    ok = await unblock_ip(ip_address)
    if ok:
        return {"success": True}
    return JSONResponse(status_code=404, content={"success": False, "error": "IP not found"})


@app.get("/api/blocked-ips/check/{ip_address}")
async def api_check_ip_blocked(ip_address: str):
    """Check if an IP is blocked."""
    blocked = await is_ip_blocked(ip_address)
    return {"blocked": blocked}


@app.post("/api/fmcsa-register/save")
async def api_save_fmcsa_entries(request: Request):
    """Save FMCSA register entries to database."""
    body = await request.json()
    entries = body.get("entries", [])
    extracted_date = body.get("extractedDate", "")
    result = await save_fmcsa_register_entries(entries, extracted_date)
    return result


@app.get("/api/fmcsa-register/categories")
async def api_get_fmcsa_categories():
    """Get all unique FMCSA register categories."""
    categories = await get_fmcsa_categories()
    return {"categories": categories}


@app.delete("/api/fmcsa-register/before/{date}")
async def api_delete_fmcsa_before_date(date: str):
    """Delete FMCSA register entries before a date."""
    deleted = await delete_fmcsa_entries_before_date(date)
    return {"success": True, "deleted": deleted}
