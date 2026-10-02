import asyncio
import contextlib
import json
import os
import zoneinfo
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import asyncpg
from fastapi import FastAPI, Header, HTTPException, Request, Response, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from passlib.context import CryptContext
from pydantic import BaseModel

# --- КОНФИГУРАЦИЯ ---
SECRET_KEY = os.getenv("SECRET_KEY", "usefguIHSFUSDFGUjhjfk88448")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://slet_db_user:password@host/slet_db")
DATA_FILE = "server_data.json"
MSK_TZ = zoneinfo.ZoneInfo("Europe/Moscow")
SESSION_EXPIRE_DAYS = 30  # Срок действия авторизации

BASE_WEEK_START = datetime(2026, 9, 21, 5, 0, 0, tzinfo=MSK_TZ)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
db_pool: Optional[asyncpg.Pool] = None

active_sessions: Dict[str, str] = {}
action_logs_memory: List[dict] = []

ALL_SERVERS = [
    "Phoenix", "Tucson", "Scottdale", "Chandler", "Brainburg",
    "Saint-Rose", "Mesa", "Red-Rock", "Yuma", "Surprise",
    "Prescott", "Glendale", "Kingman", "Winslow", "Payson",
    "Gilbert", "Show-Low", "Casa-Grande", "Page", "Sun-City",
    "Queen-Creek", "Sedona", "Holiday", "Wednesday", "Yava",
    "Faraway", "Bumble-Bee", "Christmas", "Love", "Mirage",
    "Drake", "Space", "Home"
]

server_taxes_data: Dict[str, dict] = {srv: {"house_tax": 0, "biz_tax": 0} for srv in ALL_SERVERS}

SEASONS_MAP = {
    1: "📱 По инфе",
    2: "⌨ Скорострелы",
    3: "🏎️ Автогонки",
    4: "✈️ По новому",
    5: "🏍 Мотогонки"
}

BASE_SERVER_SEASONS = {
    "Phoenix": 4, "Tucson": 1, "Scottdale": 2, "Chandler": 1,
    "Brainburg": 4, "Saint-Rose": 4, "Mesa": 3, "Red-Rock": 2,
    "Yuma": 4, "Surprise": 2, "Prescott": 1, "Glendale": 2,
    "Kingman": 5, "Winslow": 3, "Payson": 3, "Gilbert": 5,
    "Show-Low": 5, "Casa-Grande": 5, "Page": 1, "Sun-City": 3,
    "Queen-Creek": 5, "Sedona": 1, "Holiday": 4, "Wednesday": 2,
    "Yava": 2, "Faraway": 2, "Bumble-Bee": 5, "Christmas": 2,
    "Love": 2, "Mirage": 2, "Drake": 2, "Space": 5, "Home": 1
}

SERVER_DROP_RULES = {
    "Phoenix":     {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Tucson":      {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Scottdale":   {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Chandler":    {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Brainburg":   {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Saint-Rose":  {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Mesa":        {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Red-Rock":    {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Yuma":        {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Surprise":    {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Prescott":    {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Glendale":    {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Kingman":     {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Winslow":     {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Payson":      {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Gilbert":     {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Show-Low":    {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Casa-Grande": {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Page":        {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Sun-City":    {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Queen-Creek": {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Sedona":      {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Holiday":     {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Wednesday":   {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Yava":        {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Faraway":     {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Bumble-Bee":  {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Christmas":   {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Mirage":      {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Love":        {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}},
    "Drake":       {"house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}, "biz": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}},
    "Space":       {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}},
    "Home":        {"house": {"insured": 1, "uninsured_min": 1, "uninsured_max": 2}, "biz": {"insured": 2, "uninsured_min": 1, "uninsured_max": 2}}
}

def get_drop_limit(server: str, prop_type: str, status: str) -> int:
    srv_rules = SERVER_DROP_RULES.get(server, {
        "house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3},
        "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}
    })
    rules = srv_rules.get(prop_type, {"insured": 2, "uninsured_min": 2, "uninsured_max": 3})
    if status in ["uninsured", "no_activity", "frozen"]:
        return rules.get("uninsured_min", 2)
    return rules.get("insured", 2)

data_lock = asyncio.Lock()

# --- СХЕМЫ PYDANTIC ---
class PropertyEntry(BaseModel):
    propType: str
    pd: int
    propId: Optional[int] = None
    pos: int

class Payload(BaseModel):
    server: str
    scanner: Optional[str] = "unknown"
    entries: List[PropertyEntry]

class UpdateStatusModel(BaseModel):
    server: str
    scanId: str
    propType: str
    pos: int
    status: str

class DeleteItemModel(BaseModel):
    server: str
    scanId: str
    propType: str
    pos: int

class DeleteScanModel(BaseModel):
    server: str
    scanId: str

class AddItemModel(BaseModel):
    server: str
    scanId: str
    propType: str
    pd: int
    propId: Optional[int] = None

class LoginModel(BaseModel):
    username: str
    password: str

class ToggleAccessModel(BaseModel):
    user_id: int
    is_allowed: bool

class CreateUserModel(BaseModel):
    username: str
    password: str
    role: Optional[str] = "user"

class DeleteUserModel(BaseModel):
    user_id: int

class UpdateRoleModel(BaseModel):
    user_id: int
    role: str

class ChangePasswordModel(BaseModel):
    user_id: int
    new_password: str

class UpdateTaxModel(BaseModel):
    server: str
    house_tax: int
    biz_tax: int

# --- ЛОГИРОВАНИЕ ДЕЙСТВИЙ ---
async def record_action_log(server: str, username: str, action: str):
    now_msk = datetime.now(MSK_TZ)
    time_str = now_msk.strftime("%H:%M:%S")
    entry = {
        "time": time_str,
        "server": server,
        "username": username,
        "action": action
    }
    action_logs_memory.insert(0, entry)
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO action_logs (server, username, action, created_at) VALUES ($1, $2, $3, $4)",
                    server, username, action, now_msk.replace(tzinfo=None)
                )
        except Exception as e:
            print(f"Ошибка логирования действия: {e}")

# --- РАБОТА С БД POSTGRESQL И АВТОРИЗАЦИЕЙ ---
async def init_db():
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(20) DEFAULT 'user',
                is_allowed BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS scan_logs (
                id SERIAL PRIMARY KEY,
                server VARCHAR(50) NOT NULL,
                scanner VARCHAR(100) NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS action_logs (
                id SERIAL PRIMARY KEY,
                server VARCHAR(50) NOT NULL,
                username VARCHAR(50) NOT NULL,
                action VARCHAR(255) NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS server_taxes (
                server VARCHAR(50) PRIMARY KEY,
                house_tax INT DEFAULT 0,
                biz_tax INT DEFAULT 0
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS server_scans_data (
                server VARCHAR(50) PRIMARY KEY,
                data JSONB NOT NULL
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                token VARCHAR(64) PRIMARY KEY,
                username VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                expires_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        hashed_pw = pwd_context.hash("hpsdjfk123safl!")
        await conn.execute("""
            INSERT INTO users (username, password_hash, role, is_allowed)
            VALUES ('admin', $1, 'admin', TRUE)
            ON CONFLICT (username) DO NOTHING;
        """, hashed_pw)

        tax_rows = await conn.fetch("SELECT server, house_tax, biz_tax FROM server_taxes;")
        for r in tax_rows:
            if r["server"] in server_taxes_data:
                server_taxes_data[r["server"]] = {
                    "house_tax": r["house_tax"],
                    "biz_tax": r["biz_tax"]
                }

async def get_user_by_username(username: str):
    if not db_pool:
        return None
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE username = $1", username)

async def check_user_access(username: str) -> bool:
    user = await get_user_by_username(username)
    if not user or not user["is_allowed"]:
        return False
    return True

async def get_username_by_token(token: str) -> Optional[str]:
    if token in active_sessions:
        return active_sessions[token]
    
    if not db_pool:
        return None

    async with db_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT username, expires_at FROM user_sessions WHERE token = $1", token
        )
        if session:
            now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
            if session["expires_at"] > now_naive:
                active_sessions[token] = session["username"]
                return session["username"]
            else:
                await conn.execute("DELETE FROM user_sessions WHERE token = $1", token)
    return None

async def verify_auth(request: Request) -> str:
    token = request.cookies.get("session_token")
    if not token:
        raise HTTPException(status_code=401, detail="Необходима авторизация")
    
    username = await get_username_by_token(token)
    if not username:
        raise HTTPException(status_code=401, detail="Сессия истекла или недействительна")
    
    is_allowed = await check_user_access(username)
    if not is_allowed:
        raise HTTPException(status_code=403, detail="Доступ заблокирован администратором")
    
    return username

async def verify_admin(username: str = Depends(verify_auth)) -> str:
    user = await get_user_by_username(username)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Отказано в доступе: требуется роль администратора")
    return username

async def verify_editor(username: str = Depends(verify_auth)) -> str:
    user = await get_user_by_username(username)
    if not user or user["role"] not in ["admin", "support"]:
        raise HTTPException(status_code=403, detail="Отказано в доступе: требуется роль admin или support")
    return username

# --- РАБОТА С ФАЙЛАМИ И ДАННЫМИ ---
async def load_data_from_db() -> Dict[str, dict]:
    data_store = {srv: {"scans": []} for srv in ALL_SERVERS}
    
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT server, data FROM server_scans_data;")
                if rows:
                    for row in rows:
                        srv = row["server"]
                        if srv in data_store:
                            raw_data = row["data"]
                            if isinstance(raw_data, str):
                                data_store[srv] = json.loads(raw_data)
                            elif isinstance(raw_data, dict):
                                data_store[srv] = raw_data
                    print("Данные сканирований успешно загружены из PostgreSQL.")
                    return data_store
        except Exception as e:
            print(f"Ошибка загрузки сканов из PostgreSQL: {e}")

    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for srv in ALL_SERVERS:
                    if srv in loaded and "scans" in loaded[srv]:
                        data_store[srv] = loaded[srv]
            print("Данные успешно загружены из локального JSON-файла.")
        except Exception as e:
            print(f"Ошибка чтения JSON-файла: {e}")

    return data_store

server_data = {srv: {"scans": []} for srv in ALL_SERVERS}

async def save_data_to_file_async():
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                for srv, data in server_data.items():
                    data_json = json.dumps(data, ensure_ascii=False)
                    await conn.execute("""
                        INSERT INTO server_scans_data (server, data)
                        VALUES ($1, $2::jsonb)
                        ON CONFLICT (server) DO UPDATE SET data = EXCLUDED.data;
                    """, srv, data_json)
        except Exception as e:
            print(f"Ошибка сохранения данных в PostgreSQL: {e}")

    def _save():
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(server_data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"Ошибка сохранения данных в файл: {e}")

    await asyncio.to_thread(_save)

def check_and_update_outdated_scans() -> bool:
    now_msk = datetime.now(MSK_TZ)
    now_hour_start = now_msk.replace(minute=0, second=0, microsecond=0)
    changed = False

    for srv, data in server_data.items():
        scans = data.get("scans", [])
        for s in scans:
            if not s.get("hasPair", False) and not s.get("isOutdated", False):
                scan_time_str = s.get("scanTime")
                if scan_time_str:
                    try:
                        scan_dt = datetime.strptime(scan_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)
                        scan_hour_start = scan_dt.replace(minute=0, second=0, microsecond=0)
                        hours_diff = (now_hour_start - scan_hour_start).total_seconds() / 3600
                        if hours_diff >= 2:
                            s["isOutdated"] = True
                            changed = True
                    except Exception:
                        pass
    return changed

def get_current_season_id(server: str) -> int:
    base_id = BASE_SERVER_SEASONS.get(server, 1)
    now_msk = datetime.now(MSK_TZ)
    diff = now_msk - BASE_WEEK_START
    weeks_passed = max(0, diff.days // 7)
    return ((base_id - 1 + weeks_passed) % 5) + 1

def get_server_season_info(server: str) -> dict:
    season_id = get_current_season_id(server)
    season_name = SEASONS_MAP[season_id]
    return {
        "id": season_id,
        "name": season_name,
        "display": f"{season_name} ({season_id})"
    }

def get_latest_confirmed_scan(scans: List[dict]) -> Optional[dict]:
    if not scans:
        return None
    current_pair_scans = [s for s in scans if s.get("isCurrentPair", False) and s.get("hasPair", False)]
    if current_pair_scans:
        return current_pair_scans[-1]
    
    confirmed_scans = [s for s in scans if s.get("hasPair", False) or s.get("isConfirmed", False)]
    if confirmed_scans:
        return confirmed_scans[-1]
        
    return None

def find_pairs_with_offset(prev_items: List[dict], curr_entries: List[PropertyEntry], prop_type: str):
    valid_diffs = {0, 1, 2} if prop_type == "house" else {0, 1, 2, 4}
    matches = {}
    used_prev_indices = set()

    def determine_status(diff: int, p_type: str) -> str:
        if diff == 0:
            return "frozen"
        if p_type == "house":
            return "uninsured" if diff >= 2 else "insured"
        return "no_activity" if diff >= 4 else ("uninsured" if diff >= 2 else "insured")

    for c_idx, curr in enumerate(curr_entries):
        if curr.propId is not None:
            for p_idx, prev in enumerate(prev_items):
                if p_idx in used_prev_indices:
                    continue
                if prev.get("propId") == curr.propId:
                    base_pd = prev.get("basePd", prev["pd"])
                    diff = base_pd - curr.pd
                    if diff in valid_diffs:
                        auto_status = determine_status(diff, prop_type)
                        matches[c_idx] = (prev, auto_status)
                        used_prev_indices.add(p_idx)
                        break

    for c_idx, curr in enumerate(curr_entries):
        if c_idx in matches:
            continue

        for p_idx, prev in enumerate(prev_items):
            if p_idx in used_prev_indices:
                continue

            base_pd = prev.get("basePd", prev["pd"])
            diff = base_pd - curr.pd

            if diff in valid_diffs:
                auto_status = determine_status(diff, prop_type)
                matches[c_idx] = (prev, auto_status)
                used_prev_indices.add(p_idx)
                break

    return matches

async def process_hourly_payday():
    now_msk = datetime.now(MSK_TZ)
    if now_msk.hour == 0:
        action_logs_memory.clear()
        if db_pool:
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute("TRUNCATE TABLE scan_logs;")
                    await conn.execute("TRUNCATE TABLE action_logs;")
            except Exception as e:
                print(f"Ошибка очистки логов: {e}")

    async with data_lock:
        check_and_update_outdated_scans()
        await save_data_to_file_async()

async def hourly_loop():
    while True:
        now = datetime.now(MSK_TZ)
        seconds_until_next_hour = (60 - now.minute - 1) * 60 + (60 - now.second)
        if seconds_until_next_hour <= 0:
            seconds_until_next_hour = 3600
        
        await asyncio.sleep(seconds_until_next_hour)
        await process_hourly_payday()

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, server_data
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        await init_db()
        loaded_db_data = await load_data_from_db()
        server_data.update(loaded_db_data)
        check_and_update_outdated_scans()
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")

    task = asyncio.create_task(hourly_loop())
    yield
    task.cancel()
    if db_pool:
        await db_pool.close()
    async with data_lock:
        await save_data_to_file_async()

app = FastAPI(title="Arizona Property Tracker API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ЭНДПОИНТЫ ---

@app.post("/api/auth/login")
async def login(data: LoginModel, response: Response):
    if not db_pool:
        raise HTTPException(status_code=533, detail="БД недоступна")

    user = await get_user_by_username(data.username)
    if not user or not pwd_context.verify(data.password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Неверный логин или пароль")
    
    if not user["is_allowed"]:
        raise HTTPException(status_code=403, detail="Доступ заблокирован")

    token = os.urandom(24).hex()
    active_sessions[token] = user["username"]

    expires_at_utc = datetime.now(timezone.utc) + timedelta(days=SESSION_EXPIRE_DAYS)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_sessions (token, username, expires_at) VALUES ($1, $2, $3)",
            token, user["username"], expires_at_utc.replace(tzinfo=None)
        )
    
    response.set_cookie(
        key="session_token",
        value=token,
        max_age=SESSION_EXPIRE_DAYS * 24 * 3600,
        expires=expires_at_utc,
        httponly=True,
        samesite="lax"
    )
    return {"status": "ok", "role": user["role"], "username": user["username"]}

@app.post("/api/auth/logout")
async def logout(request: Request, response: Response):
    token = request.cookies.get("session_token")
    if token:
        if token in active_sessions:
            del active_sessions[token]
        if db_pool:
            async with db_pool.acquire() as conn:
                await conn.execute("DELETE FROM user_sessions WHERE token = $1", token)

    response.delete_cookie("session_token")
    return {"status": "ok"}

@app.get("/api/auth/me")
async def get_me(request: Request):
    token = request.cookies.get("session_token")
    if not token:
        return {"authenticated": False}
    
    username = await get_username_by_token(token)
    if not username:
        return {"authenticated": False}

    user = await get_user_by_username(username)
    if not user or not user["is_allowed"]:
        return {"authenticated": False}

    return {"authenticated": True, "username": user["username"], "role": user["role"]}

@app.get("/api/admin/users")
async def get_users(username: str = Depends(verify_admin)):
    if not db_pool:
        return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, username, role, is_allowed, created_at FROM users ORDER BY id ASC")
        return [dict(row) for row in rows]

@app.get("/api/admin/scan_logs")
async def get_scan_logs(username: str = Depends(verify_admin)):
    if not db_pool:
        return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, server, scanner, created_at FROM scan_logs ORDER BY created_at DESC LIMIT 200")
        res = []
        for r in rows:
            d = dict(r)
            if d["created_at"]:
                d["created_at"] = d["created_at"].strftime("%H:%M:%S")
            res.append(d)
        return res

@app.get("/api/action_logs")
async def get_action_logs(username: str = Depends(verify_auth)):
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT server, username, action, created_at FROM action_logs ORDER BY created_at DESC LIMIT 200")
                return [{
                    "time": row["created_at"].strftime("%H:%M:%S") if row["created_at"] else "—",
                    "server": row["server"],
                    "username": row["username"],
                    "action": row["action"]
                } for row in rows]
        except Exception:
            pass
    return action_logs_memory

@app.post("/api/admin/toggle_access")
async def toggle_access(data: ToggleAccessModel, username: str = Depends(verify_admin)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="БД недоступна")
    async with db_pool.acquire() as conn:
        target = await conn.fetchrow("SELECT username FROM users WHERE id = $1", data.user_id)
        if target and target["username"] == "admin":
            raise HTTPException(status_code=400, detail="Нельзя изменять доступ администратора admin")
        await conn.execute("UPDATE users SET is_allowed = $1 WHERE id = $2", data.is_allowed, data.user_id)
    return {"status": "success"}

@app.post("/api/admin/create_user")
async def create_user(data: CreateUserModel, username: str = Depends(verify_admin)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="БД недоступна")
    role = data.role if data.role in ["user", "support", "admin"] else "user"
    hashed_pw = pwd_context.hash(data.password)
    async with db_pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO users (username, password_hash, role, is_allowed) VALUES ($1, $2, $3, TRUE)",
                data.username, hashed_pw, role
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=400, detail="Пользователь уже существует")
    return {"status": "success"}

@app.post("/api/admin/delete_user")
async def delete_user(data: DeleteUserModel, current_admin: str = Depends(verify_admin)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="БД недоступна")
    async with db_pool.acquire() as conn:
        target = await conn.fetchrow("SELECT username FROM users WHERE id = $1", data.user_id)
        if not target:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        if target["username"] in [current_admin, "admin"]:
            raise HTTPException(status_code=400, detail="Нельзя удалить главный аккаунт admin")
        
        await conn.execute("DELETE FROM users WHERE id = $1", data.user_id)
        await conn.execute("DELETE FROM user_sessions WHERE username = $1", target["username"])
        
        tokens_to_remove = [t for t, u in active_sessions.items() if u == target["username"]]
        for t in tokens_to_remove:
            del active_sessions[t]

    return {"status": "success"}

@app.post("/api/admin/update_role")
async def update_role(data: UpdateRoleModel, current_admin: str = Depends(verify_admin)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="БД недоступна")
    if data.role not in ["user", "support", "admin"]:
        raise HTTPException(status_code=400, detail="Недопустимая роль")
        
    async with db_pool.acquire() as conn:
        target = await conn.fetchrow("SELECT username FROM users WHERE id = $1", data.user_id)
        if not target:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        if target["username"] == "admin":
            raise HTTPException(status_code=400, detail="Нельзя менять роль супер-администратору")

        await conn.execute("UPDATE users SET role = $1 WHERE id = $2", data.role, data.user_id)

    return {"status": "success"}

@app.post("/api/admin/change_password")
async def change_password(data: ChangePasswordModel, current_admin: str = Depends(verify_admin)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="БД недоступна")
    if not data.new_password or len(data.new_password.strip()) < 4:
        raise HTTPException(status_code=400, detail="Пароль слишком короткий")

    hashed_pw = pwd_context.hash(data.new_password)
    async with db_pool.acquire() as conn:
        target = await conn.fetchrow("SELECT username FROM users WHERE id = $1", data.user_id)
        if not target:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        
        await conn.execute("UPDATE users SET password_hash = $1 WHERE id = $2", hashed_pw, data.user_id)

    return {"status": "success"}

@app.get("/api/taxes")
async def get_taxes(username: str = Depends(verify_auth)):
    return server_taxes_data

@app.post("/api/taxes/update")
async def update_tax(data: UpdateTaxModel, username: str = Depends(verify_admin)):
    if data.server not in ALL_SERVERS:
        raise HTTPException(status_code=400, detail="Некорректный сервер")
    
    server_taxes_data[data.server] = {
        "house_tax": max(0, data.house_tax),
        "biz_tax": max(0, data.biz_tax)
    }

    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO server_taxes (server, house_tax, biz_tax)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (server) DO UPDATE SET house_tax = EXCLUDED.house_tax, biz_tax = EXCLUDED.biz_tax;
                """, data.server, max(0, data.house_tax), max(0, data.biz_tax))
        except Exception as e:
            print(f"Ошибка сохранения налогов: {e}")

    await record_action_log(data.server, username, f"Обновление налогов: Дом={data.house_tax}$, Бизнес={data.biz_tax}$")
    return {"status": "success"}

# --- ЭНДПОИНТЫ МОНИТОРИНГА ---

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)

@app.post("/api/paydays")
async def receive_paydays(payload: Payload, x_secret_key: Optional[str] = Header(None)):
    if x_secret_key != SECRET_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Secret Key")

    srv = payload.server
    scanner_name = payload.scanner or "unknown"
    now_msk = datetime.now(MSK_TZ)
    hour_label = now_msk.strftime("%H:00")
    scan_id = f"{now_msk.strftime('%Y-%m-%d')} {hour_label}"

    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO scan_logs (server, scanner, created_at) VALUES ($1, $2, $3)",
                    srv, scanner_name, now_msk.replace(tzinfo=None)
                )
        except Exception as e:
            print(f"Ошибка сохранения лога сканирования в БД: {e}")

    async with data_lock:
        check_and_update_outdated_scans()

        if srv not in server_data:
            server_data[srv] = {"scans": []}
        if "scans" not in server_data[srv]:
            server_data[srv]["scans"] = []

        scans = server_data[srv]["scans"]
        
        existing_scan = next((s for s in scans if s["scanId"] == scan_id), None)
        other_scans = [s for s in scans if s["scanId"] != scan_id]
        
        prev_scan = None
        if other_scans:
            last_s = other_scans[-1]
            if existing_scan and (existing_scan.get("hasPair", False) or existing_scan.get("isConfirmed", False)):
                prev_scan = last_s
            elif not last_s.get("hasPair", False) and not last_s.get("isOutdated", False):
                prev_scan = last_s

        house_entries = [e for e in payload.entries if e.propType == "house"]
        biz_entries = [e for e in payload.entries if e.propType != "house"]

        prev_houses = prev_scan.get("houses", []) if prev_scan else []
        prev_biz = prev_scan.get("businesses", []) if prev_scan else []

        existing_houses = existing_scan.get("houses", []) if existing_scan else []
        existing_biz = existing_scan.get("businesses", []) if existing_scan else []

        house_matches = find_pairs_with_offset(prev_houses, house_entries, "house") if prev_scan else {}
        biz_matches = find_pairs_with_offset(prev_biz, biz_entries, "biz") if prev_scan else {}

        houses = []
        for idx, item in enumerate(house_entries):
            matched_existing = None
            if existing_houses:
                if item.propId is not None:
                    matched_existing = next((eh for eh in existing_houses if eh.get("propId") == item.propId), None)
                if not matched_existing:
                    matched_existing = next((eh for eh in existing_houses if eh.get("pos") == item.pos), None)

            if idx in house_matches:
                prev_item, auto_status = house_matches[idx]
                prev_item["isPendingPair"] = False
                prev_item["status"] = auto_status

                final_status = auto_status
                if matched_existing and not matched_existing.get("isPendingPair", True):
                    final_status = matched_existing.get("status", auto_status)

                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": final_status,
                    "isPendingPair": False,
                    "createdAt": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            elif matched_existing and not matched_existing.get("isPendingPair", True):
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": matched_existing.get("status", "new"),
                    "isPendingPair": False,
                    "createdAt": matched_existing.get("createdAt", now_msk.strftime("%Y-%m-%d %H:%M:%S")),
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            else:
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": "new",
                    "isPendingPair": True,
                    "createdAt": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            houses.append(record)

        businesses = []
        for idx, item in enumerate(biz_entries):
            matched_existing = None
            if existing_biz:
                if item.propId is not None:
                    matched_existing = next((eb for eb in existing_biz if eb.get("propId") == item.propId), None)
                if not matched_existing:
                    matched_existing = next((eb for eb in existing_biz if eb.get("pos") == item.pos), None)

            if idx in biz_matches:
                prev_item, auto_status = biz_matches[idx]
                prev_item["isPendingPair"] = False
                prev_item["status"] = auto_status

                final_status = auto_status
                if matched_existing and not matched_existing.get("isPendingPair", True):
                    final_status = matched_existing.get("status", auto_status)

                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": final_status,
                    "isPendingPair": False,
                    "createdAt": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            elif matched_existing and not matched_existing.get("isPendingPair", True):
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": matched_existing.get("status", "new"),
                    "isPendingPair": False,
                    "createdAt": matched_existing.get("createdAt", now_msk.strftime("%Y-%m-%d %H:%M:%S")),
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            else:
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": "new",
                    "isPendingPair": True,
                    "createdAt": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            businesses.append(record)

        houses = sorted(houses, key=lambda x: x["pos"])
        businesses = sorted(businesses, key=lambda x: x["pos"])

        is_pair_found = (prev_scan is not None)

        if is_pair_found and prev_scan:
            for s in scans:
                s["isCurrentPair"] = False
                s["isOutdated"] = True

            prev_scan["hasPair"] = True
            prev_scan["isConfirmed"] = True
            prev_scan["isCurrentPair"] = True
            prev_scan["isOutdated"] = False

        has_houses_in_payload = len(house_entries) > 0
        has_biz_in_payload = len(biz_entries) > 0

        if existing_scan:
            if has_houses_in_payload:
                existing_scan["houses"] = houses
            if has_biz_in_payload:
                existing_scan["businesses"] = businesses
            existing_scan["scanTime"] = now_msk.strftime("%Y-%m-%d %H:%M:%S")
            if is_pair_found and prev_scan:
                existing_scan["isConfirmed"] = True
                existing_scan["hasPair"] = True
                existing_scan["isCurrentPair"] = True
                existing_scan["isOutdated"] = False
        else:
            new_scan = {
                "scanId": scan_id,
                "hourLabel": hour_label,
                "scanTime": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                "houses": houses,
                "businesses": businesses,
                "isConfirmed": is_pair_found and (prev_scan is not None),
                "hasPair": is_pair_found and (prev_scan is not None),
                "isCurrentPair": is_pair_found and (prev_scan is not None),
                "isOutdated": False
            }
            scans.append(new_scan)

        if len(server_data[srv]["scans"]) > 4:
            server_data[srv]["scans"] = server_data[srv]["scans"][-4:]

        await save_data_to_file_async()

    return {"status": "ok", "scanId": scan_id, "count": len(payload.entries)}

@app.post("/api/update_status")
async def update_status(data: UpdateStatusModel, username: str = Depends(verify_editor)):
    srv = data.server
    now_msk = datetime.now(MSK_TZ)
    async with data_lock:
        if srv in server_data:
            for scan in server_data[srv].get("scans", []):
                if scan["scanId"] == data.scanId:
                    target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
                    for item in scan[target_key]:
                        if item["pos"] == data.pos:
                            prev_status = item.get("status", "new")
                            
                            # Ручная сменa статуса: списание за прошедшие часы в статусе 'new'
                            if prev_status in ["new", "новый"]:
                                created_str = item.get("createdAt") or scan.get("scanTime")
                                if created_str:
                                    try:
                                        created_dt = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)
                                        c_hour_start = created_dt.replace(minute=0, second=0, microsecond=0)
                                        n_hour_start = now_msk.replace(minute=0, second=0, microsecond=0)
                                        hours_passed = max(0, int((n_hour_start - c_hour_start).total_seconds() / 3600))
                                        
                                        if hours_passed >= 1:
                                            rate = 1
                                            if data.status == "uninsured": rate = 2
                                            elif data.status == "no_activity": rate = 4
                                            elif data.status == "frozen": rate = 0
                                            
                                            item["pd"] = max(0, item["pd"] - (hours_passed * rate))
                                    except Exception as e:
                                        print(f"Ошибка вычисления разницы часов: {e}")

                            item["status"] = data.status
                            item["isPendingPair"] = False
                            await save_data_to_file_async()
                            
                            p_title = "Дом" if target_key == "houses" else "Бизнес"
                            prop_id_str = f" #{item.get('propId')}" if item.get("propId") else ""
                            await record_action_log(
                                srv, username,
                                f"Смена статуса на '{data.status}' для {p_title}{prop_id_str} (№{data.pos})"
                            )
                            return {"status": "success"}
    raise HTTPException(status_code=404, detail="Scan or Item not found")

@app.post("/api/delete_item")
async def delete_item(data: DeleteItemModel, username: str = Depends(verify_editor)):
    srv = data.server
    async with data_lock:
        if srv in server_data:
            for scan in server_data[srv].get("scans", []):
                if scan["scanId"] == data.scanId:
                    target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
                    p_title = "дом" if target_key == "houses" else "бизнес"
                    scan[target_key] = [item for item in scan[target_key] if item["pos"] != data.pos]
                    await save_data_to_file_async()
                    
                    await record_action_log(srv, username, f"Удаление {p_title}а (№{data.pos})")
                    return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server or Scan not found")

@app.post("/api/delete_scan")
async def delete_scan(data: DeleteScanModel, username: str = Depends(verify_editor)):
    srv = data.server
    async with data_lock:
        if srv in server_data and "scans" in server_data[srv]:
            server_data[srv]["scans"] = [
                s for s in server_data[srv]["scans"] if s["scanId"] != data.scanId
            ]
            await save_data_to_file_async()
            await record_action_log(srv, username, f"Удаление скана ({data.scanId})")
            return {"status": "success"}
    raise HTTPException(status_code=404, detail="Scan not found")

@app.post("/api/add_item")
async def add_item(data: AddItemModel, username: str = Depends(verify_editor)):
    srv = data.server
    now_msk = datetime.now(MSK_TZ)
    async with data_lock:
        if srv in server_data:
            for scan in server_data[srv].get("scans", []):
                if scan["scanId"] == data.scanId:
                    target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
                    existing_positions = [item["pos"] for item in scan[target_key]]
                    new_pos = max(existing_positions, default=0) + 1
                    
                    new_record = {
                        "basePd": data.pd,
                        "pd": data.pd,
                        "propId": data.propId,
                        "pos": new_pos,
                        "status": "insured",
                        "isPendingPair": False,
                        "createdAt": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                        "updatedAt": now_msk.strftime("%H:%M:%S")
                    }
                    
                    scan[target_key].append(new_record)
                    scan[target_key] = sorted(scan[target_key], key=lambda x: x["pos"])
                    await save_data_to_file_async()
                    
                    p_title = "дома" if target_key == "houses" else "бизнеса"
                    prop_str = f" #{data.propId}" if data.propId else ""
                    await record_action_log(
                        srv, username,
                        f"Добавление {p_title}{prop_str} (PD: {data.pd})"
                    )
                    return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server or Scan not found")

@app.get("/api/paydays")
async def get_paydays(username: str = Depends(verify_auth)):
    async with data_lock:
        if check_and_update_outdated_scans():
            await save_data_to_file_async()
        res = {}
        for srv, data in server_data.items():
            scans = data.get("scans", [])
            latest_confirmed = get_latest_confirmed_scan(scans)
            res[srv] = {
                "scans": scans,
                "latestConfirmedScan": latest_confirmed,
                "season": get_server_season_info(srv),
                "dropRules": SERVER_DROP_RULES.get(srv, {
                    "house": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3},
                    "biz": {"insured": 2, "uninsured_min": 2, "uninsured_max": 3}
                })
            }
        return res

# --- ДАШБОРД (FRONTEND) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Arizona RP — Мониторинг Слётов</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #07090e;
            --card-bg: rgba(18, 22, 33, 0.85);
            --card-border: rgba(255, 255, 255, 0.08);
            --accent-orange: #ff9800;
            --accent-orange-glow: rgba(255, 152, 0, 0.35);
            --text-main: #f3f6fc;
            --text-muted: #8e9bb0;
            --table-header: rgba(255, 255, 255, 0.03);
            --table-row-hover: rgba(255, 255, 255, 0.05);
        }

        * { box-sizing: border-box; }

        body { 
            font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            background-color: var(--bg-color); 
            background-image: 
                radial-gradient(circle at 50% 0%, rgba(255, 152, 0, 0.08), transparent 45%),
                radial-gradient(circle at 10% 90%, rgba(0, 176, 255, 0.05), transparent 35%);
            background-attachment: fixed;
            color: var(--text-main); 
            margin: 0; 
            padding: 20px 15px; 
            min-height: 100vh;
        }

        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: var(--bg-color); }
        ::-webkit-scrollbar-thumb { background: #202838; border-radius: 4px; }

        h1 { 
            text-align: center; 
            color: #ffffff; 
            font-size: 1.9rem;
            font-weight: 800;
            letter-spacing: 0.8px;
            margin: 10px 0 25px 0;
            text-transform: uppercase;
            background: linear-gradient(135deg, #ffffff 20%, #ffb74d 70%, var(--accent-orange) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 0 4px 20px rgba(255, 152, 0, 0.2);
        }

        .lottery-banner {
            background: linear-gradient(135deg, rgba(255, 152, 0, 0.12), rgba(156, 39, 176, 0.18));
            border: 1px solid rgba(255, 152, 0, 0.35);
            backdrop-filter: blur(12px);
            border-radius: 14px;
            padding: 14px 22px;
            margin: 0 auto 20px auto;
            max-width: 1600px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 15px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.35);
        }
        .lottery-info { display: flex; align-items: center; gap: 14px; }
        .lottery-icon { font-size: 2.2rem; line-height: 1; }
        .lottery-title { font-size: 1.05rem; font-weight: 800; color: #ffca28; text-transform: uppercase; }
        .lottery-sub { font-size: 0.85rem; color: var(--text-muted); margin-top: 2px; }
        .lottery-sub b { color: #ffffff; }
        .lottery-countdown {
            display: flex;
            align-items: center;
            gap: 10px;
            background: rgba(10, 13, 20, 0.7);
            padding: 8px 18px;
            border-radius: 10px;
            border: 1px solid rgba(0, 230, 118, 0.3);
        }
        .lottery-label { font-size: 0.85rem; font-weight: 700; color: var(--text-muted); }
        .lottery-timer { font-size: 1.25rem; font-weight: 800; color: #00e676; font-family: monospace; }

        .login-box { 
            max-width: 400px; 
            margin: 80px auto; 
            background: var(--card-bg); 
            backdrop-filter: blur(16px);
            padding: 38px 32px; 
            border-radius: 16px; 
            border: 1px solid var(--card-border); 
            text-align: center; 
            box-shadow: 0 12px 40px rgba(0,0,0,0.6);
        }
        .login-box h2 { margin-top: 0; margin-bottom: 22px; color: var(--text-main); font-size: 1.45rem; font-weight: 800; }
        .login-box input { 
            width: 100%; 
            padding: 13px; 
            margin: 8px 0; 
            background: rgba(10, 13, 20, 0.8); 
            border: 1px solid rgba(255,255,255,0.12); 
            color: #fff; 
            border-radius: 8px; 
            font-size: 0.95rem;
            outline: none;
        }
        .login-box input:focus { border-color: var(--accent-orange); }
        .login-box button { 
            width: 100%; 
            padding: 13px; 
            margin-top: 18px;
            background: linear-gradient(135deg, #ff9800, #f57c00); 
            border: none; 
            font-weight: 800; 
            cursor: pointer; 
            border-radius: 8px; 
            color: #0b0d14; 
            font-size: 1rem;
        }

        .user-nav { 
            display: flex; 
            justify-content: space-between; 
            align-items: center; 
            max-width: 1600px; 
            margin: 0 auto 20px auto; 
            background: var(--card-bg);
            padding: 12px 22px;
            border-radius: 12px;
            border: 1px solid var(--card-border);
        }
        #user-info { font-weight: 700; font-size: 0.92rem; color: var(--text-muted); }
        .btn-logout { 
            background: rgba(255, 45, 85, 0.12); 
            color: #ff5252; 
            border: 1px solid rgba(255, 45, 85, 0.3); 
            padding: 8px 18px; 
            border-radius: 8px; 
            cursor: pointer; 
            font-weight: 700;
        }
        .btn-logout:hover { background: #ff2d55; color: #fff; }

        .tabs { display: flex; justify-content: center; gap: 10px; margin-bottom: 25px; flex-wrap: wrap; }
        .tab-btn { 
            background-color: var(--card-bg); 
            color: var(--text-muted); 
            border: 1px solid var(--card-border); 
            padding: 11px 24px; 
            font-size: 0.92rem; 
            font-weight: 800; 
            border-radius: 10px; 
            cursor: pointer; 
            transition: all 0.2s; 
        }
        .tab-btn.active { 
            background: linear-gradient(135deg, #ff9800, #f57c00); 
            color: #0b0d14; 
            border-color: #ff9800; 
            box-shadow: 0 4px 18px var(--accent-orange-glow);
        }

        .tab-content { display: none; }
        .tab-content.active { display: block; }

        .upcoming-filters { display: flex; justify-content: center; gap: 10px; margin-bottom: 20px; }
        .time-filter-btn { 
            background-color: var(--card-bg); 
            color: var(--text-muted); 
            border: 1px solid var(--card-border); 
            padding: 8px 20px; 
            font-size: 0.85rem; 
            font-weight: 800; 
            border-radius: 20px; 
            cursor: pointer; 
            transition: all 0.2s; 
        }
        .time-filter-btn.active { 
            background: linear-gradient(135deg, #e53935, #d32f2f); 
            color: #ffffff; 
            border-color: #ef5350; 
            box-shadow: 0 0 14px rgba(229, 57, 53, 0.5); 
        }

        .filter-panel { 
            background: var(--card-bg); 
            border: 1px solid var(--card-border); 
            border-radius: 12px; 
            padding: 14px 22px; 
            margin: 0 auto 20px auto; 
            max-width: 1600px;
            display: flex; 
            flex-wrap: wrap; 
            gap: 22px; 
            align-items: center; 
            justify-content: center; 
        }
        .filter-group { display: flex; align-items: center; gap: 10px; }
        .filter-group label { font-size: 0.85rem; color: var(--accent-orange); font-weight: 800; text-transform: uppercase; }
        .filter-select { 
            background: rgba(10, 13, 20, 0.8); 
            color: #fff; 
            border: 1px solid rgba(255,255,255,0.12); 
            padding: 8px 14px; 
            border-radius: 8px; 
            font-size: 0.85rem; 
            font-weight: 700;
            outline: none;
        }

        /* Выравнивание столбиков серверов */
        .servers-container { 
            display: grid; 
            grid-template-columns: repeat(3, 1fr); 
            gap: 18px; 
            max-width: 1600px; 
            margin: 0 auto; 
            align-items: start;
        }

        @media (max-width: 1100px) { .servers-container { grid-template-columns: repeat(2, 1fr); } }
        @media (max-width: 650px) { .servers-container { grid-template-columns: repeat(1, 1fr); } }

        .server-card { 
            background-color: var(--card-bg); 
            border: 1px solid var(--card-border); 
            border-radius: 14px; 
            padding: 16px; 
            box-shadow: 0 6px 20px rgba(0,0,0,0.3); 
            display: flex;
            flex-direction: column;
            transition: all 0.25s ease;
        }
        
        .server-header { 
            border-bottom: 1px solid rgba(255, 255, 255, 0.08); 
            padding-bottom: 10px; 
            margin-bottom: 12px; 
        }
        .server-title { 
            font-size: 1.08rem; 
            font-weight: 800; 
            color: #ffffff; 
            display: flex; 
            justify-content: space-between; 
            align-items: center; 
        }
        .season-badge { 
            font-size: 0.72rem; 
            background: rgba(255, 152, 0, 0.12); 
            color: #ffca28; 
            border: 1px solid rgba(255, 152, 0, 0.3); 
            padding: 3px 10px; 
            border-radius: 12px; 
            font-weight: 700; 
        }

        .scan-info-time {
            font-size: 0.78rem;
            color: var(--text-muted);
            margin-bottom: 10px;
            font-weight: 600;
        }
        .scan-info-time b { color: #00e676; }
        
        .scan-tabs-bar { 
            display: flex; 
            justify-content: space-between; 
            align-items: center; 
            margin-bottom: 12px; 
            border-bottom: 1px solid rgba(255, 255, 255, 0.08); 
            padding-bottom: 8px; 
            gap: 8px;
        }
        .scan-tabs-container { display: flex; gap: 6px; overflow-x: auto; padding-bottom: 2px; }
        .scan-subtab { 
            background-color: rgba(10, 13, 20, 0.8); 
            color: var(--text-muted); 
            border: 1px solid rgba(255,255,255,0.08); 
            padding: 5px 12px; 
            font-size: 0.78rem; 
            border-radius: 6px; 
            cursor: pointer; 
            transition: all 0.2s; 
            font-weight: 700;
        }

        .scan-subtab.current-pair { background-color: rgba(46, 125, 50, 0.2); color: #a5d6a7; border-color: #4caf50; }
        .scan-subtab.current-pair.active { background-color: #2e7d32; color: #ffffff; border-color: #81c784; }
        .scan-subtab.old-pair { background-color: rgba(55, 71, 79, 0.25); color: #90a4ae; border-color: #37474f; }
        .scan-subtab.old-pair.active { background-color: #37474f; color: #cfd8dc; }
        .scan-subtab.single { background-color: rgba(255, 152, 0, 0.15); color: #ffe082; border-color: #ff9800; }
        .scan-subtab.single.active { background-color: #f57c00; color: #ffffff; }

        .btn-delete-scan { 
            background-color: rgba(255, 45, 85, 0.15); 
            color: #ff5252; 
            border: 1px solid rgba(255, 45, 85, 0.35); 
            padding: 4px 8px; 
            border-radius: 6px; 
            font-size: 0.7rem; 
            font-weight: 800; 
            cursor: pointer; 
        }

        .tables-grid { 
            display: grid; 
            grid-template-columns: 1fr 1fr; 
            gap: 12px; 
            flex-grow: 1;
        }
        @media (max-width: 480px) { .tables-grid { grid-template-columns: 1fr; } }
        
        .section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
        .section-title { font-size: 0.8rem; font-weight: 800; color: #00e5ff; text-transform: uppercase; }
        
        .btn-add { 
            background: linear-gradient(135deg, #0288d1, #0277bd); 
            color: white; 
            border: none; 
            border-radius: 5px; 
            padding: 3px 8px; 
            font-size: 0.75rem; 
            font-weight: 800; 
            cursor: pointer; 
        }

        table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
        th, td { padding: 6px 8px; text-align: left; border-bottom: 1px solid rgba(255, 255, 255, 0.05); }
        th { background-color: var(--table-header); color: var(--text-muted); font-weight: 700; font-size: 0.72rem; text-transform: uppercase; }
        tr:hover td { background-color: var(--table-row-hover); }

        .pd-badge { background-color: #263043; color: #e2e8f0; padding: 2px 6px; border-radius: 5px; font-weight: 800; font-size: 0.75rem; }
        .time-left-badge { font-weight: 800; color: #ffca28; font-size: 0.75rem; background-color: rgba(255, 152, 0, 0.12); padding: 3px 7px; border-radius: 6px; border: 1px solid rgba(255, 152, 0, 0.35); }

        .btn-group { display: flex; gap: 4px; align-items: center; }
        .btn-opt { 
            width: 24px;
            height: 24px;
            border-radius: 50%;
            border: 1px solid rgba(255, 255, 255, 0.12); 
            background: rgba(255, 255, 255, 0.05); 
            color: #a0aec0; 
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 0.78rem;
            cursor: pointer; 
            padding: 0;
            flex-shrink: 0;
        }
        .btn-opt.active-insured { background: linear-gradient(135deg, #2e7d32, #4caf50); color: #fff; border-color: #66bb6a; }
        .btn-opt.active-uninsured { background: linear-gradient(135deg, #c62828, #ef5350); color: #fff; border-color: #e57373; }
        .btn-opt.active-noact { background: linear-gradient(135deg, #d32f2f, #ff1744); color: #fff; border-color: #ff5252; }
        .btn-opt.active-frozen { background: linear-gradient(135deg, #1565c0, #00b0ff); color: #fff; border-color: #40c4ff; }
        
        .status-text { font-weight: 800; font-size: 0.75rem; padding: 2px 6px; border-radius: 4px; display: inline-block; }
        .status-insured { color: #81c784; }
        .status-uninsured { color: #e57373; }
        .status-noact { color: #ff5252; }
        .status-frozen { color: #64b5f6; }
        .status-new { color: #ffca28; font-style: italic; }

        .btn-del { background-color: transparent; color: #ff5252; border: 1px solid rgba(255, 82, 82, 0.3); padding: 1px 6px; font-size: 0.75rem; border-radius: 4px; cursor: pointer; }
        .btn-user-del { background-color: #c62828; color: #fff; border: none; padding: 5px 10px; border-radius: 6px; cursor: pointer; font-size: 0.8em; font-weight: 700; }
        
        .empty { color: #546e7a; font-style: italic; font-size: 0.78rem; display: block; padding: 4px 0; }
        .empty-center { text-align: center; color: var(--text-muted); font-style: italic; padding: 35px; background-color: var(--card-bg); border-radius: 14px; border: 1px solid var(--card-border); max-width: 600px; margin: 0 auto; grid-column: 1 / -1; }

        .action-log-panel {
            background: var(--card-bg);
            border: 1px solid var(--card-border);
            border-radius: 14px;
            padding: 16px 20px;
            max-width: 1600px;
            margin: 0 auto 22px auto;
        }
        .action-log-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            user-select: none;
        }
        .action-log-title { font-size: 1rem; font-weight: 800; color: var(--accent-orange); }
        .toggle-icon { font-size: 0.9rem; color: var(--text-muted); }
        .action-log-content { margin-top: 15px; max-height: 280px; overflow-y: auto; border-top: 1px solid rgba(255, 255, 255, 0.08); padding-top: 10px; }
        .action-log-content.collapsed { display: none; }

        .taxes-table-wrapper {
            max-width: 1000px;
            margin: 0 auto;
            background: var(--card-bg);
            border: 1px solid var(--card-border);
            border-radius: 14px;
            padding: 18px;
            backdrop-filter: blur(14px);
        }
        .taxes-table { width: 100%; border-collapse: collapse; }
        .taxes-table th { background-color: var(--table-header); color: var(--accent-orange); font-weight: 800; font-size: 0.85rem; padding: 10px 14px; text-align: left; }
        .taxes-table td { padding: 8px 14px; border-bottom: 1px solid rgba(255, 255, 255, 0.05); font-size: 0.9rem; }
        .tax-input-compact { width: 110px; background: rgba(10, 13, 20, 0.8); border: 1px solid rgba(255, 255, 255, 0.15); color: #00e676; font-weight: 800; border-radius: 6px; padding: 5px 8px; font-size: 0.88rem; text-align: right; outline: none; }
        .btn-save-tax-compact { background: linear-gradient(135deg, #ff9800, #f57c00); color: #0b0d14; border: none; font-weight: 800; padding: 6px 14px; border-radius: 6px; cursor: pointer; }
    </style>
</head>
<body>
    <div id="login-screen" style="display: none;">
        <div class="login-box">
            <h2>🔐 Авторизация</h2>
            <input type="text" id="login-username" placeholder="Логин">
            <input type="password" id="login-password" placeholder="Пароль">
            <button onclick="handleLogin()">Войти в систему</button>
        </div>
    </div>

    <div id="main-dashboard" style="display: none;">
        <h1>Arizona RP — Мониторинг Слётов</h1>

        <div class="lottery-banner">
            <div class="lottery-info">
                <div class="lottery-icon">🎰</div>
                <div>
                    <div class="lottery-title">Лотерейный билет</div>
                    <div class="lottery-sub">Сброс лотерейных билетов каждый день в <b>21:10 МСК</b></div>
                </div>
            </div>
            <div class="lottery-countdown">
                <span class="lottery-label">До сброса:</span>
                <span class="lottery-timer" id="lottery-timer">00:00:00</span>
            </div>
        </div>

        <div class="user-nav">
            <span id="user-info">Вы вошли как: ...</span>
            <button class="btn-logout" onclick="handleLogout()">Выйти</button>
        </div>

        <div class="tabs">
            <button class="tab-btn active" id="btn-tab-view" onclick="switchTab('view')">📊 Обзор слётов</button>
            <button class="tab-btn" id="btn-tab-upcoming" onclick="switchTab('upcoming')">🚨 Ожидаются к слёту</button>
            <button class="tab-btn" id="btn-tab-manage" onclick="switchTab('manage')" style="display:none;">⚙️ Управление</button>
            <button class="tab-btn" id="btn-tab-taxes" onclick="switchTab('taxes')">💰 Налоги</button>
            <button class="tab-btn" id="btn-tab-admin" onclick="switchTab('admin')" style="display:none;">👑 Админ-панель</button>
        </div>

        <!-- Таб Обзор -->
        <div id="tab-view" class="tab-content active">
            <div class="filter-panel">
                <div class="filter-group">
                    <label for="filter-season">Сезон:</label>
                    <select id="filter-season" class="filter-select" onchange="renderViewTab()">
                        <option value="all">Все сезоны</option>
                        <option value="1">📱 По инфе (1)</option>
                        <option value="2">⌨ Скорострелы (2)</option>
                        <option value="3">🏎️️ Автогонки (3)</option>
                        <option value="4">✈️ По новому (4)</option>
                        <option value="5">🏍 Мотогонки (5)</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label for="filter-fav">Избранное:</label>
                    <select id="filter-fav" class="filter-select" onchange="renderViewTab()">
                        <option value="all">Все сервера</option>
                        <option value="fav_only">⭐ Только избранные</option>
                    </select>
                </div>
            </div>
            <div class="servers-container" id="servers-view"></div>
        </div>

        <!-- Таб Ожидаются к слёту -->
        <div id="tab-upcoming" class="tab-content">
            <div class="upcoming-filters">
                <button class="time-filter-btn active" id="btn-upcoming-1h" onclick="setUpcomingHoursFilter(1)">1 час</button>
                <button class="time-filter-btn" id="btn-upcoming-2h" onclick="setUpcomingHoursFilter(2)">2 часа</button>
                <button class="time-filter-btn" id="btn-upcoming-3h" onclick="setUpcomingHoursFilter(3)">3 часа</button>
            </div>
            <div class="servers-container" id="servers-upcoming"></div>
        </div>

        <!-- Таб Управление -->
        <div id="tab-manage" class="tab-content">
            <div class="action-log-panel">
                <div class="action-log-header" onclick="toggleActionLog()">
                    <div class="action-log-title">
                        <span>📝 Лог действий (За сегодня)</span>
                    </div>
                    <span class="toggle-icon" id="action-log-arrow">▼ Свернуть</span>
                </div>
                <div class="action-log-content" id="action-log-content">
                    <div id="action-logs-table-container">Загрузка логов...</div>
                </div>
            </div>

            <div class="servers-container" id="servers-manage"></div>
        </div>

        <!-- Таб Налоги -->
        <div id="tab-taxes" class="tab-content">
            <div class="taxes-table-wrapper" id="taxes-container">Загрузка данных налогов...</div>
        </div>

        <!-- Таб Админка -->
        <div id="tab-admin" class="tab-content">
            <div class="admin-panel" style="max-width: 1200px; margin: 0 auto;">
                <div class="admin-card" style="background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 14px; padding: 22px; margin-bottom: 20px;">
                    <h3 style="margin-top: 0; color: var(--accent-orange);">➕ Создать пользователя</h3>
                    <div style="display: flex; gap: 12px; flex-wrap: wrap; align-items: center;">
                        <input type="text" id="new-username" placeholder="Логин" class="filter-select" style="padding: 9px 14px;">
                        <input type="password" id="new-password" placeholder="Пароль" class="filter-select" style="padding: 9px 14px;">
                        <select id="new-role" class="filter-select" style="padding: 9px 14px;">
                            <option value="user">User</option>
                            <option value="support">Support</option>
                            <option value="admin">Admin</option>
                        </select>
                        <button class="btn-add" style="padding: 10px 18px; background: linear-gradient(135deg, #ff9800, #f57c00); color: #0b0d14;" onclick="handleCreateUser()">Создать</button>
                    </div>
                </div>

                <div class="admin-card" style="background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 14px; padding: 22px; margin-bottom: 20px;">
                    <h3 style="margin-top: 0; color: var(--accent-orange);">👥 Список пользователей</h3>
                    <div id="admin-users-table"></div>
                </div>

                <div class="admin-card" style="background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 14px; padding: 22px;">
                    <h3 style="margin-top: 0; color: var(--accent-orange);">📜 Логи сканирований (За сегодня)</h3>
                    <div id="admin-scan-logs-table"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        const ALL_SERVERS = [
            "Phoenix", "Tucson", "Scottdale", "Chandler", "Brainburg",
            "Saint-Rose", "Mesa", "Red-Rock", "Yuma", "Surprise",
            "Prescott", "Glendale", "Kingman", "Winslow", "Payson",
            "Gilbert", "Show-Low", "Casa-Grande", "Page", "Sun-City",
            "Queen-Creek", "Sedona", "Holiday", "Wednesday", "Yava",
            "Faraway", "Bumble-Bee", "Christmas", "Love", "Mirage",
            "Drake", "Space", "Home"
        ];

        const SERVER_NUMBERS = {
            "Phoenix": 1, "Tucson": 2, "Scottdale": 3, "Chandler": 4, "Brainburg": 5,
            "Saint-Rose": 6, "Mesa": 7, "Red-Rock": 8, "Yuma": 9, "Surprise": 10,
            "Prescott": 11, "Glendale": 12, "Kingman": 13, "Winslow": 14, "Payson": 15,
            "Gilbert": 16, "Show-Low": 17, "Casa-Grande": 18, "Page": 19, "Sun-City": 20,
            "Queen-Creek": 21, "Sedona": 22, "Holiday": 23, "Wednesday": 24, "Yava": 25,
            "Faraway": 26, "Bumble-Bee": 27, "Christmas": 28, "Love": 30, "Mirage": 29,
            "Drake": 31, "Space": 32, "Home": 33
        };

        function getServerDisplayName(srv) {
            const num = SERVER_NUMBERS[srv];
            return num ? `${num}. ${srv}` : srv;
        }

        const activeServerScans = {};
        let currentUser = null;
        let favoriteServers = JSON.parse(localStorage.getItem('fav_servers') || '[]');
        let globalServerData = {};
        let selectedUpcomingHours = 1;

        function updateLotteryTimer() {
            const now = new Date();
            const utcMs = now.getTime() + (now.getTimezoneOffset() * 60000);
            const mskNow = new Date(utcMs + (3 * 3600000));

            let target = new Date(mskNow);
            target.setHours(21, 10, 0, 0);

            if (mskNow >= target) {
                target.setDate(target.getDate() + 1);
            }

            const diffMs = target - mskNow;
            const hours = Math.floor(diffMs / (1000 * 60 * 60));
            const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
            const seconds = Math.floor((diffMs % (1000 * 60)) / 1000);

            const pad = n => String(n).padStart(2, '0');
            const timerElem = document.getElementById('lottery-timer');
            if (timerElem) {
                timerElem.innerText = `${pad(hours)}:${pad(minutes)}:${pad(seconds)}`;
            }
        }

        function getStatusRate(status) {
            if (status === 'insured') return 1;
            if (status === 'uninsured') return 2;
            if (status === 'no_activity') return 4;
            if (status === 'frozen' || status === 'new' || status === 'новый') return 0;
            return 1;
        }

        function calculateCurrentPd(scannedPd, status, scanTimeStr) {
            if (!scanTimeStr || status === 'frozen' || status === 'new' || status === 'новый') return scannedPd;
            const rate = getStatusRate(status);
            if (rate === 0) return scannedPd;

            const now = new Date();
            const utcMs = now.getTime() + (now.getTimezoneOffset() * 60000);
            const mskNow = new Date(utcMs + (3 * 3600000));

            const parts = scanTimeStr.split(' ');
            const dateParts = parts[0].split('-');
            const timeParts = parts[1].split(':');
            const scanDt = new Date(Date.UTC(dateParts[0], dateParts[1] - 1, dateParts[2], timeParts[0], timeParts[1], timeParts[2]));
            const scanMsk = new Date(scanDt.getTime() - (3 * 3600000));

            const scanHourStart = new Date(scanMsk.getFullYear(), scanMsk.getMonth(), scanMsk.getDate(), scanMsk.getHours());
            const nowHourStart = new Date(mskNow.getFullYear(), mskNow.getMonth(), mskNow.getDate(), mskNow.getHours());

            const hoursPassed = Math.max(0, Math.floor((nowHourStart - scanHourStart) / 3600000));
            return Math.max(0, scannedPd - (hoursPassed * rate));
        }

        function getDropLimit(server, type, status) {
            const info = globalServerData[server];
            const defaultRules = { insured: 2, uninsured_min: 2, uninsured_max: 3 };
            const rules = (info && info.dropRules) ? (info.dropRules[type] || defaultRules) : defaultRules;
            if (['uninsured', 'no_activity', 'frozen'].includes(status)) {
                return rules.uninsured_min || 2;
            }
            return rules.insured || 2;
        }

        function getHoursUntilDrop(pd, status, dropLimit) {
            if (status === 'frozen') return null;
            const diff = pd - dropLimit;
            if (diff < 0) return 0;
            let dec = getStatusRate(status);
            if (dec === 0) dec = 1;
            return Math.floor(diff / dec) + 1;
        }

        function getTimeUntilDropText(pd, status, dropLimit) {
            const hours = getHoursUntilDrop(pd, status, dropLimit);
            if (hours === null) return 'Заморожен';
            if (hours <= 0) return 'Слёт прямо сейчас';

            const now = new Date();
            const utcMs = now.getTime() + (now.getTimezoneOffset() * 60000);
            const mskNow = new Date(utcMs + (3 * 3600000));

            const targetTime = new Date(mskNow.getTime() + hours * 3600000);
            targetTime.setMinutes(0, 0, 0);

            const diffMs = targetTime.getTime() - mskNow.getTime();
            if (diffMs <= 0) return 'Слёт прямо сейчас';

            const totalMin = Math.floor(diffMs / 60000);
            const h = Math.floor(totalMin / 60);
            const m = totalMin % 60;

            if (h > 0 && m > 0) return `${h} ч. ${m} мин.`;
            if (h > 0) return `${h} ч.`;
            return `${m} мин.`;
        }

        function toggleFavorite(srv) {
            if (favoriteServers.includes(srv)) {
                favoriteServers = favoriteServers.filter(s => s !== srv);
            } else {
                favoriteServers.push(srv);
            }
            localStorage.setItem('fav_servers', JSON.stringify(favoriteServers));
            renderViewTab();
        }

        function toggleActionLog() {
            const content = document.getElementById('action-log-content');
            const arrow = document.getElementById('action-log-arrow');
            if (content.classList.contains('collapsed')) {
                content.classList.remove('collapsed');
                arrow.innerText = '▼ Свернуть';
            } else {
                content.classList.add('collapsed');
                arrow.innerText = '▶ Развернуть';
            }
        }

        async function checkAuth() {
            try {
                const res = await fetch('/api/auth/me');
                if (!res.ok) {
                    document.getElementById('login-screen').style.display = 'block';
                    document.getElementById('main-dashboard').style.display = 'none';
                    return;
                }
                const data = await res.json();
                if (data.authenticated) {
                    currentUser = data;
                    document.getElementById('login-screen').style.display = 'none';
                    document.getElementById('main-dashboard').style.display = 'block';
                    document.getElementById('user-info').innerText = `Вы вошли как: ${data.username} (${data.role})`;
                    
                    const canManage = data.role === 'admin' || data.role === 'support';
                    const isAdmin = data.role === 'admin';

                    document.getElementById('btn-tab-manage').style.display = canManage ? 'inline-block' : 'none';
                    document.getElementById('btn-tab-admin').style.display = isAdmin ? 'inline-block' : 'none';
                    
                    initDashboard();
                } else {
                    document.getElementById('login-screen').style.display = 'block';
                    document.getElementById('main-dashboard').style.display = 'none';
                }
            } catch (e) {
                document.getElementById('login-screen').style.display = 'block';
                document.getElementById('main-dashboard').style.display = 'none';
            }
        }

        async function handleLogin() {
            const username = document.getElementById('login-username').value.trim();
            const password = document.getElementById('login-password').value;

            if (!username || !password) {
                alert('Введите логин и пароль.');
                return;
            }

            try {
                const res = await fetch('/api/auth/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username, password })
                });

                if (res.ok) {
                    await checkAuth();
                } else {
                    const err = await res.json();
                    alert(err.detail || 'Ошибка авторизации');
                }
            } catch (err) {
                alert('Ошибка соединения с сервером');
            }
        }

        async function handleLogout() {
            await fetch('/api/auth/logout', { method: 'POST' });
            location.reload();
        }

        function switchTab(tabName) {
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            
            if (tabName === 'manage') {
                document.getElementById('btn-tab-manage').classList.add('active');
                document.getElementById('tab-manage').classList.add('active');
                loadActionLogs();
                renderManageTab();
            } else if (tabName === 'upcoming') {
                document.getElementById('btn-tab-upcoming').classList.add('active');
                document.getElementById('tab-upcoming').classList.add('active');
                renderUpcomingTab();
            } else if (tabName === 'taxes') {
                document.getElementById('btn-tab-taxes').classList.add('active');
                document.getElementById('tab-taxes').classList.add('active');
                loadTaxesTab();
            } else if (tabName === 'admin') {
                document.getElementById('btn-tab-admin').classList.add('active');
                document.getElementById('tab-admin').classList.add('active');
                loadAdminUsers();
                loadScanLogs();
            } else {
                document.getElementById('btn-tab-view').classList.add('active');
                document.getElementById('tab-view').classList.add('active');
                renderViewTab();
            }
        }

        function setUpcomingHoursFilter(hours) {
            selectedUpcomingHours = hours;
            document.querySelectorAll('.time-filter-btn').forEach(b => b.classList.remove('active'));
            document.getElementById(`btn-upcoming-${hours}h`).classList.add('active');
            renderUpcomingTab();
        }

        async function initDashboard() {
            await fetchPaydays();
            setInterval(fetchPaydays, 20000);
            setInterval(updateLotteryTimer, 1000);
            updateLotteryTimer();
        }

        async function fetchPaydays() {
            try {
                const res = await fetch('/api/paydays');
                if (res.ok) {
                    globalServerData = await res.json();
                    
                    ALL_SERVERS.forEach(srv => {
                        const scans = globalServerData[srv]?.scans || [];
                        if (scans.length > 0) {
                            if (!activeServerScans[srv] || !scans.find(s => s.scanId === activeServerScans[srv])) {
                                activeServerScans[srv] = scans[scans.length - 1].scanId;
                            }
                        }
                    });

                    if (document.getElementById('tab-view').classList.contains('active')) renderViewTab();
                    if (document.getElementById('tab-manage').classList.contains('active')) renderManageTab();
                    if (document.getElementById('tab-upcoming').classList.contains('active')) renderUpcomingTab();
                }
            } catch (e) {
                console.error("Ошибка обновления данных:", e);
            }
        }

        // --- ТАБ: ОБЩИЙ ВИД ---
        function renderViewTab() {
            const seasonFilter = document.getElementById('filter-season').value;
            const favFilter = document.getElementById('filter-fav').value;
            const container = document.getElementById('servers-view');

            let html = '';
            let count = 0;

            ALL_SERVERS.forEach(srv => {
                const info = globalServerData[srv];
                if (!info) return;

                if (favFilter === 'fav_only' && !favoriteServers.includes(srv)) return;
                if (seasonFilter !== 'all' && String(info.season.id) !== seasonFilter) return;

                const scan = info.latestConfirmedScan;
                if (!scan) return;

                count++;
                const isFav = favoriteServers.includes(srv);
                const favStar = isFav ? '⭐' : '☆';

                html += `
                    <div class="server-card">
                        <div class="server-header">
                            <div class="server-title">
                                <span><span class="fav-btn" onclick="toggleFavorite('${srv}')">${favStar}</span>${getServerDisplayName(srv)}</span>
                                <span class="season-badge">${info.season.name}</span>
                            </div>
                        </div>
                        <div class="scan-info-time">Скан: <b>${scan.scanTime}</b></div>
                        <div class="tables-grid">
                            ${renderPropertyTable(srv, scan, 'house', true)}
                            ${renderPropertyTable(srv, scan, 'biz', true)}
                        </div>
                    </div>
                `;
            });

            container.innerHTML = count > 0 ? html : `<div class="empty-center">Нет подтвержденных сканов для отображения.</div>`;
        }

        // --- ТАБ: БЛИЖАЙШИЕ СЛЁТЫ ---
        function renderUpcomingTab() {
            const container = document.getElementById('servers-upcoming');
            let html = '';
            let count = 0;

            ALL_SERVERS.forEach(srv => {
                const info = globalServerData[srv];
                if (!info) return;
                const scan = info.latestConfirmedScan;
                if (!scan) return;

                const scanTime = scan.scanTime;
                
                const filteredHouses = (scan.houses || []).map(h => {
                    const curPd = calculateCurrentPd(h.pd, h.status, scanTime);
                    const dropLimit = getDropLimit(srv, 'house', h.status);
                    const hLeft = getHoursUntilDrop(curPd, h.status, dropLimit);
                    return { ...h, curPd, hLeft, dropLimit };
                }).filter(h => h.hLeft === selectedUpcomingHours);

                const filteredBiz = (scan.businesses || []).map(b => {
                    const curPd = calculateCurrentPd(b.pd, b.status, scanTime);
                    const dropLimit = getDropLimit(srv, 'biz', b.status);
                    const hLeft = getHoursUntilDrop(curPd, b.status, dropLimit);
                    return { ...b, curPd, hLeft, dropLimit };
                }).filter(b => b.hLeft === selectedUpcomingHours);

                if (filteredHouses.length > 0 || filteredBiz.length > 0) {
                    count++;
                    html += `
                        <div class="server-card">
                            <div class="server-header">
                                <div class="server-title">
                                    <span>${getServerDisplayName(srv)}</span>
                                    <span class="season-badge">${info.season.name}</span>
                                </div>
                            </div>
                            <div class="scan-info-time">Скан: <b>${scan.scanTime}</b></div>
                            <div class="tables-grid">
                                ${renderCustomList(srv, filteredHouses, 'Домы')}
                                ${renderCustomList(srv, filteredBiz, 'Бизнесы')}
                            </div>
                        </div>
                    `;
                }
            });

            container.innerHTML = count > 0 ? html : `<div class="empty-center">Нет слетов через ${selectedUpcomingHours} ч.</div>`;
        }

        function renderCustomList(srv, items, title) {
            let html = `<div>
                <div class="section-header"><span class="section-title">${title}</span></div>
                <table>
                    <thead><tr><th>#</th><th>Ид</th><th>Осталось</th></tr></thead>
                    <tbody>`;
            
            if (items.length === 0) {
                html += `<tr><td colspan="3"><span class="empty">Нет объектов</span></td></tr>`;
            } else {
                items.forEach(item => {
                    const timeText = getTimeUntilDropText(item.curPd, item.status, item.dropLimit);
                    html += `<tr>
                        <td><b>${item.pos}</b></td>
                        <td>${item.propId ? '#' + item.propId : '—'}</td>
                        <td><span class="pd-badge">${item.curPd}</span> <span class="time-left-badge">${timeText}</span></td>
                    </tr>`;
                });
            }
            html += `</tbody></table></div>`;
            return html;
        }

        // --- ТАБ: УПРАВЛЕНИЕ ---
        function renderManageTab() {
            const container = document.getElementById('servers-manage');
            let html = '';

            ALL_SERVERS.forEach(srv => {
                const info = globalServerData[srv];
                if (!info) return;

                const scans = info.scans || [];
                let activeScanId = activeServerScans[srv];
                
                if (scans.length > 0 && (!activeScanId || !scans.find(s => s.scanId === activeScanId))) {
                    activeScanId = scans[scans.length - 1].scanId;
                    activeServerScans[srv] = activeScanId;
                }

                const currentScan = scans.find(s => s.scanId === activeScanId);

                let tabsHtml = '';
                scans.forEach(s => {
                    const isActive = (s.scanId === activeScanId) ? 'active' : '';
                    let scanClass = 'single';
                    let typeText = 'Одиночный скан';
                    
                    if (s.isCurrentPair) { scanClass = 'current-pair'; typeText = 'Текущая пара'; }
                    else if (s.hasPair || s.isConfirmed) { scanClass = 'old-pair'; typeText = 'Подтвержденная пара'; }

                    const timeOnly = s.hourLabel || s.scanTime.split(' ')[1].substring(0, 5);
                    const tooltip = `Дата: ${s.scanTime} | Статус: ${typeText}`;

                    tabsHtml += `<button class="scan-subtab ${scanClass} ${isActive}" title="${tooltip}" onclick="selectScan('${srv}', '${s.scanId}')">${timeOnly}</button>`;
                });

                html += `
                    <div class="server-card">
                        <div class="server-header">
                            <div class="server-title">
                                <span>${getServerDisplayName(srv)}</span>
                                <span class="season-badge">${info.season.name}</span>
                            </div>
                        </div>
                        <div class="scan-tabs-bar">
                            <div class="scan-tabs-container">${tabsHtml}</div>
                            ${currentScan ? `<button class="btn-delete-scan" onclick="deleteScan('${srv}', '${currentScan.scanId}')">Удалить</button>` : ''}
                        </div>
                        <div class="tables-grid">
                            ${renderPropertyTable(srv, currentScan, 'house', false)}
                            ${renderPropertyTable(srv, currentScan, 'biz', false)}
                        </div>
                    </div>
                `;
            });

            container.innerHTML = html;
        }

        function renderPropertyTable(srv, scan, propType, isViewTab) {
            const isHouse = propType === 'house';
            const title = isHouse ? 'Дома' : 'Бизнесы';
            const items = scan ? (isHouse ? scan.houses : scan.businesses) : [];

            let html = `<div>
                <div class="section-header">
                    <span class="section-title">${title}</span>
                    ${(!isViewTab && scan) ? `<button class="btn-add" onclick="addItem('${srv}', '${scan.scanId}', '${propType}')">+ Добавить</button>` : ''}
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Ид</th>
                            <th>Число</th>
                            ${isViewTab ? '<th>Слёт</th>' : '<th>Статус</th>'}
                            ${!isViewTab ? '<th></th>' : ''}
                        </tr>
                    </thead>
                    <tbody>`;

            if (!items || items.length === 0) {
                html += `<tr><td colspan="5"><span class="empty">Нет данных</span></td></tr>`;
            } else {
                items.forEach(item => {
                    const displayPd = isViewTab ? calculateCurrentPd(item.pd, item.status, scan.scanTime) : item.pd;
                    const dropLimit = getDropLimit(srv, propType, item.status);
                    
                    if (isViewTab) {
                        const timeText = getTimeUntilDropText(displayPd, item.status, dropLimit);
                        html += `<tr>
                            <td><b>${item.pos}</b></td>
                            <td>${item.propId ? '#' + item.propId : '—'}</td>
                            <td><span class="pd-badge">${displayPd}</span></td>
                            <td><span class="time-left-badge">${timeText}</span></td>
                        </tr>`;
                    } else {
                        html += `<tr>
                            <td><b>${item.pos}</b></td>
                            <td>${item.propId ? '#' + item.propId : '—'}</td>
                            <td><span class="pd-badge">${displayPd}</span></td>
                            <td>${renderStatusControls(srv, scan.scanId, propType, item)}</td>
                            <td><button class="btn-del" onclick="deleteItem('${srv}', '${scan.scanId}', '${propType}', ${item.pos})">×</button></td>
                        </tr>`;
                    }
                });
            }

            html += `</tbody></table></div>`;
            return html;
        }

        function renderStatusControls(srv, scanId, propType, item) {
            const st = item.status;
            if (st === 'new' || st === 'новый') {
                return `<span class="status-text status-new">Новый</span>`;
            }

            const btnInsured = `<button class="btn-opt ${st === 'insured' ? 'active-insured' : ''}" title="Страхованный" onclick="updateStatus('${srv}', '${scanId}', '${propType}', ${item.pos}, 'insured')">С</button>`;
            const btnUninsured = `<button class="btn-opt ${st === 'uninsured' ? 'active-uninsured' : ''}" title="Нестрахованный" onclick="updateStatus('${srv}', '${scanId}', '${propType}', ${item.pos}, 'uninsured')">Н</button>`;
            const btnNoAct = (propType === 'biz') ? `<button class="btn-opt ${st === 'no_activity' ? 'active-noact' : ''}" title="Без занятости" onclick="updateStatus('${srv}', '${scanId}', '${propType}', ${item.pos}, 'no_activity')">Б</button>` : '';
            const btnFrozen = `<button class="btn-opt ${st === 'frozen' ? 'active-frozen' : ''}" title="Заморожен" onclick="updateStatus('${srv}', '${scanId}', '${propType}', ${item.pos}, 'frozen')">З</button>`;

            return `<div class="btn-group">${btnInsured}${btnUninsured}${btnNoAct}${btnFrozen}</div>`;
        }

        function selectScan(srv, scanId) {
            activeServerScans[srv] = scanId;
            renderManageTab();
        }

        async function updateStatus(server, scanId, propType, pos, status) {
            await fetch('/api/update_status', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ server, scanId, propType, pos, status })
            });
            await fetchPaydays();
        }

        async function deleteItem(server, scanId, propType, pos) {
            if (!confirm('Удалить эту запись?')) return;
            await fetch('/api/delete_item', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ server, scanId, propType, pos })
            });
            await fetchPaydays();
        }

        async function deleteScan(server, scanId) {
            if (!confirm('Удалить весь скан?')) return;
            await fetch('/api/delete_scan', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ server, scanId })
            });
            await fetchPaydays();
        }

        async function addItem(server, scanId, propType) {
            const pdStr = prompt("Введите количество ПейДев (PD):");
            if (!pdStr) return;
            const pd = parseInt(pdStr);
            if (isNaN(pd)) return alert("Некорректное число");

            const propIdStr = prompt("Введите ID дома/бизнеса (необязательно):");
            const propId = propIdStr ? parseInt(propIdStr) : null;

            await fetch('/api/add_item', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ server, scanId, propType, pd, propId })
            });
            await fetchPaydays();
        }

        async function loadActionLogs() {
            try {
                const res = await fetch('/api/action_logs');
                if (res.ok) {
                    const logs = await res.json();
                    let html = `<table class="taxes-table"><thead><tr><th>Время</th><th>Сервер</th><th>Пользователь</th><th>Действие</th></tr></thead><tbody>`;
                    if (logs.length === 0) {
                        html += `<tr><td colspan="4"><span class="empty">Логи отсутствуют</span></td></tr>`;
                    } else {
                        logs.forEach(l => {
                            html += `<tr><td>${l.time}</td><td><b>${l.server}</b></td><td>${l.username}</td><td>${l.action}</td></tr>`;
                        });
                    }
                    html += `</tbody></table>`;
                    document.getElementById('action-logs-table-container').innerHTML = html;
                }
            } catch (e) {}
        }

        async function loadTaxesTab() {
            try {
                const res = await fetch('/api/taxes');
                if (res.ok) {
                    const taxes = await res.json();
                    const isAdmin = currentUser && currentUser.role === 'admin';

                    let html = `<table class="taxes-table"><thead><tr><th># Сервер</th><th>Налог на дом ($)</th><th>Налог на бизнес ($)</th>${isAdmin ? '<th>Действия</th>' : ''}</tr></thead><tbody>`;

                    ALL_SERVERS.forEach(srv => {
                        const t = taxes[srv] || { house_tax: 0, biz_tax: 0 };
                        html += `<tr>
                            <td><b>${getServerDisplayName(srv)}</b></td>
                            <td><input type="number" id="tax-house-${srv}" value="${t.house_tax}" class="tax-input-compact" ${!isAdmin ? 'disabled' : ''}></td>
                            <td><input type="number" id="tax-biz-${srv}" value="${t.biz_tax}" class="tax-input-compact" ${!isAdmin ? 'disabled' : ''}></td>
                            ${isAdmin ? `<td><button class="btn-save-tax-compact" onclick="saveTax('${srv}')">Сохранить</button></td>` : ''}
                        </tr>`;
                    });

                    html += `</tbody></table>`;
                    document.getElementById('taxes-container').innerHTML = html;
                }
            } catch (e) {}
        }

        async function saveTax(server) {
            const houseTax = parseInt(document.getElementById(`tax-house-${server}`).value) || 0;
            const bizTax = parseInt(document.getElementById(`tax-biz-${server}`).value) || 0;

            const res = await fetch('/api/taxes/update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ server, house_tax: houseTax, biz_tax: bizTax })
            });

            if (res.ok) alert(`Налоги для ${server} успешно обновлены!`);
            else alert("Ошибка при сохранении налогов");
        }

        async function loadAdminUsers() {
            try {
                const res = await fetch('/api/admin/users');
                if (res.ok) {
                    const users = await res.json();
                    let html = `<table class="taxes-table"><thead><tr><th>ID</th><th>Логин</th><th>Роль</th><th>Доступ</th><th>Действия</th></tr></thead><tbody>`;

                    users.forEach(u => {
                        let actions = '';
                        if (u.username !== 'admin') {
                            actions = `
                                <button onclick="toggleUserAccess(${u.id}, ${!u.is_allowed})" class="${u.is_allowed ? 'btn-del' : 'btn-add'}">
                                    ${u.is_allowed ? 'Заблокировать' : 'Разблокировать'}
                                </button>
                                <button onclick="deleteUser(${u.id})" class="btn-user-del">Удалить</button>
                            `;
                        } else {
                            actions = `<span style="color:var(--text-muted);">—</span>`;
                        }

                        html += `<tr>
                            <td>${u.id}</td>
                            <td><b>${u.username}</b></td>
                            <td>${u.username === 'admin' ? 'admin' : `
                                <select class="filter-select" onchange="updateUserRole(${u.id}, this.value)">
                                    <option value="user" ${u.role==='user'?'selected':''}>user</option>
                                    <option value="support" ${u.role==='support'?'selected':''}>support</option>
                                    <option value="admin" ${u.role==='admin'?'selected':''}>admin</option>
                                </select>`}
                            </td>
                            <td>${u.is_allowed ? '<span style="color:#00e676">Разрешен</span>' : '<span style="color:#ff2d55">Заблокирован</span>'}</td>
                            <td>${actions}</td>
                        </tr>`;
                    });

                    html += `</tbody></table>`;
                    document.getElementById('admin-users-table').innerHTML = html;
                }
            } catch (e) {}
        }

        async function handleCreateUser() {
            const username = document.getElementById('new-username').value.trim();
            const password = document.getElementById('new-password').value;
            const role = document.getElementById('new-role').value;

            if (!username || !password) return alert("Заполните логин и пароль");

            const res = await fetch('/api/admin/create_user', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password, role })
            });

            if (res.ok) {
                alert("Пользователь создан");
                document.getElementById('new-username').value = '';
                document.getElementById('new-password').value = '';
                loadAdminUsers();
            } else {
                const err = await res.json();
                alert(err.detail || "Ошибка создания");
            }
        }

        async function toggleUserAccess(userId, isAllowed) {
            await fetch('/api/admin/toggle_access', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, is_allowed: isAllowed })
            });
            loadAdminUsers();
        }

        async function deleteUser(userId) {
            if (!confirm('Удалить пользователя?')) return;
            const res = await fetch('/api/admin/delete_user', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId })
            });
            if (res.ok) loadAdminUsers();
            else {
                const err = await res.json();
                alert(err.detail || "Ошибка удаления");
            }
        }

        async function updateUserRole(userId, role) {
            await fetch('/api/admin/update_role', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, role })
            });
            loadAdminUsers();
        }

        async function loadScanLogs() {
            try {
                const res = await fetch('/api/admin/scan_logs');
                if (res.ok) {
                    const logs = await res.json();
                    let html = `<table class="taxes-table"><thead><tr><th>Время</th><th>Сервер</th><th>Сканнер</th></tr></thead><tbody>`;
                    logs.forEach(l => {
                        html += `<tr><td>${l.created_at}</td><td><b>${l.server}</b></td><td>${l.scanner}</td></tr>`;
                    });
                    html += `</tbody></table>`;
                    document.getElementById('admin-scan-logs-table').innerHTML = html;
                }
            } catch (e) {}
        }

        checkAuth();
    </script>
</body>
</html>
"""
