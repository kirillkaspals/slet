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
from fastapi.responses import HTMLResponse
from passlib.context import CryptContext
from pydantic import BaseModel

# --- КОНФИГУРАЦИЯ ---
SECRET_KEY = os.getenv("SECRET_KEY", "usefguIHSFUSDFGUjhjfk88448")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://slet_db_user:password@host/slet_db")
DATA_FILE = "server_data.json"
MSK_TZ = zoneinfo.ZoneInfo("Europe/Moscow")
SESSION_EXPIRE_DAYS = 30  # Срок действия авторизации ("Запомнить устройство")

BASE_WEEK_START = datetime(2026, 9, 21, 5, 0, 0, tzinfo=MSK_TZ)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
db_pool: Optional[asyncpg.Pool] = None

# Активные сессии в памяти: token -> username
active_sessions: Dict[str, str] = {}

ALL_SERVERS = [
    "Phoenix", "Tucson", "Scottdale", "Chandler", "Brainburg",
    "Saint-Rose", "Mesa", "Red-Rock", "Yuma", "Surprise",
    "Prescott", "Glendale", "Kingman", "Winslow", "Payson",
    "Gilbert", "Show-Low", "Casa-Grande", "Page", "Sun-City",
    "Queen-Creek", "Sedona", "Holiday", "Wednesday", "Yava",
    "Faraway", "Bumble-Bee", "Christmas", "Love", "Mirage",
    "Drake", "Space", "Home"
]

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
    if status in ["uninsured", "no_activity", "frozen", "pending"]:
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
            CREATE TABLE IF NOT EXISTS manual_action_logs (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                action_type VARCHAR(50) NOT NULL,
                server VARCHAR(50) NOT NULL,
                details TEXT NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
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

async def log_manual_action(username: str, action_type: str, server: str, details: str):
    now_msk = datetime.now(MSK_TZ).replace(tzinfo=None)
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO manual_action_logs (username, action_type, server, details, created_at)
                    VALUES ($1, $2, $3, $4, $5);
                """, username, action_type, server, details, now_msk)
        except Exception as e:
            print(f"Ошибка сохранения лога ручных действий в БД: {e}")

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
    # Приоритет 1: Свежая подтвержденная пара (2-й скан из пары)
    current_pair_scans = [s for s in scans if s.get("isCurrentPair", False) and s.get("hasPair", False)]
    if current_pair_scans:
        return current_pair_scans[-1]
    
    # Приоритет 2: Любой подтвержденный скан
    confirmed_scans = [s for s in scans if s.get("hasPair", False) or s.get("isConfirmed", False)]
    if confirmed_scans:
        return confirmed_scans[-1]
        
    return None

# --- АЛГОРИТМ УМНОГО СОПОСТАВЛЕНИЯ ---
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

# --- ЛОГИКА PAYDAY ---
async def process_hourly_payday():
    now_msk = datetime.now(MSK_TZ)

    if now_msk.hour == 0 and db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("TRUNCATE TABLE scan_logs;")
                await conn.execute("TRUNCATE TABLE manual_action_logs;")
                print(f"[{now_msk.strftime('%Y-%m-%d %H:%M:%S')}] Логи сканов и ручных действий очищены (00:00 МСК).")
        except Exception as e:
            print(f"Ошибка очистки логов: {e}")

    async with data_lock:
        check_and_update_outdated_scans()
        print(f"[{now_msk.strftime('%Y-%m-%d %H:%M:%S')}] Выполнен почасовой PayDay...")
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
        print(f"Ошибка подключения к PostgreSQL: {e}")

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

# --- ЭНДПОИНТЫ АВТОРИЗАЦИИ И АДМИНКИ ---

@app.post("/api/auth/login")
async def login(data: LoginModel, response: Response):
    if not db_pool:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="База данных недоступна. Попробуйте позже."
        )

    user = await get_user_by_username(data.username)
    if not user or not pwd_context.verify(data.password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Неверный логин или пароль")
    
    if not user["is_allowed"]:
        raise HTTPException(status_code=403, detail="Ваш доступ к сайту заблокирован")

    token = os.urandom(24).hex()
    active_sessions[token] = user["username"]

    expires_at_utc = datetime.now(timezone.utc) + timedelta(days=SESSION_EXPIRE_DAYS)
    expires_at_db = expires_at_utc.replace(tzinfo=None)

    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_sessions (token, username, expires_at) VALUES ($1, $2, $3)",
            token, user["username"], expires_at_db
        )
    
    max_age = SESSION_EXPIRE_DAYS * 24 * 3600
    response.set_cookie(
        key="session_token",
        value=token,
        max_age=max_age,
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

    return {
        "authenticated": True,
        "username": user["username"],
        "role": user["role"]
    }

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
        rows = await conn.fetch("""
            SELECT id, server, scanner, created_at 
            FROM scan_logs 
            ORDER BY created_at DESC 
            LIMIT 200
        """)
        result = []
        for row in rows:
            r = dict(row)
            if r["created_at"]:
                r["created_at"] = r["created_at"].strftime("%H:%M:%S")
            result.append(r)
        return result

@app.get("/api/admin/manual_action_logs")
async def get_manual_action_logs(username: str = Depends(verify_editor)):
    if not db_pool:
        return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, username, action_type, server, details, created_at 
            FROM manual_action_logs 
            ORDER BY created_at DESC 
            LIMIT 200
        """)
        result = []
        for row in rows:
            r = dict(row)
            if r["created_at"]:
                r["created_at"] = r["created_at"].strftime("%d.%m.%Y %H:%M:%S")
            result.append(r)
        return result

@app.post("/api/admin/toggle_access")
async def toggle_access(data: ToggleAccessModel, username: str = Depends(verify_admin)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="БД недоступна")
    async with db_pool.acquire() as conn:
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
        if target["username"] == current_admin:
            raise HTTPException(status_code=400, detail="Нельзя удалить собственный аккаунт")
        
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
        if target["username"] == current_admin:
            raise HTTPException(status_code=400, detail="Нельзя изменить роль самому себе")

        await conn.execute("UPDATE users SET role = $1 WHERE id = $2", data.role, data.user_id)

    return {"status": "success"}

@app.post("/api/admin/change_password")
async def change_password(data: ChangePasswordModel, current_admin: str = Depends(verify_admin)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="БД недоступна")
    if not data.new_password or len(data.new_password.strip()) < 4:
        raise HTTPException(status_code=400, detail="Пароль слишком короткий (минимум 4 символа)")

    hashed_pw = pwd_context.hash(data.new_password)
    
    async with db_pool.acquire() as conn:
        target = await conn.fetchrow("SELECT username FROM users WHERE id = $1", data.user_id)
        if not target:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        
        await conn.execute("UPDATE users SET password_hash = $1 WHERE id = $2", hashed_pw, data.user_id)

    return {"status": "success"}

# --- ЭНДПОИНТЫ API МОНИТОРИНГА ---

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)

@app.post("/api/paydays")
async def receive_paydays(payload: Payload, x_secret_key: Optional[str] = Header(None)):
    if x_secret_key != SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Invalid Secret Key"
        )

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
                if matched_existing and not matched_existing.get("isPendingPair", True) and matched_existing.get("status") != "pending":
                    final_status = matched_existing.get("status", auto_status)

                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": final_status,
                    "isPendingPair": False,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            elif matched_existing and not matched_existing.get("isPendingPair", True) and matched_existing.get("status") != "pending":
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": matched_existing.get("status", "insured"),
                    "isPendingPair": False,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            else:
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": "pending",
                    "isPendingPair": True,
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
                if matched_existing and not matched_existing.get("isPendingPair", True) and matched_existing.get("status") != "pending":
                    final_status = matched_existing.get("status", auto_status)

                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": final_status,
                    "isPendingPair": False,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            elif matched_existing and not matched_existing.get("isPendingPair", True) and matched_existing.get("status") != "pending":
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": matched_existing.get("status", "insured"),
                    "isPendingPair": False,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            else:
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": "pending",
                    "isPendingPair": True,
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
            prev_scan["isCurrentPair"] = False
            prev_scan["isOutdated"] = True

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
                existing_scan["isConfirmed"] = existing_scan.get("isConfirmed", False)
                existing_scan["hasPair"] = existing_scan.get("hasPair", False)
                existing_scan["isCurrentPair"] = existing_scan.get("isCurrentPair", False)
                existing_scan["isOutdated"] = existing_scan.get("isOutdated", False)
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
    if data.propType in ["house", "houses"] and data.status == "no_activity":
        raise HTTPException(
            status_code=400,
            detail="У домов не может быть статуса 'без активности'"
        )

    async with data_lock:
        if srv in server_data:
            for scan in server_data[srv].get("scans", []):
                if scan["scanId"] == data.scanId:
                    target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
                    for item in scan[target_key]:
                        if item["pos"] == data.pos:
                            old_status = item.get("status", "pending")
                            item["status"] = data.status
                            item["isPendingPair"] = (data.status == "pending")
                            await save_data_to_file_async()

                            prop_label = "Дом" if data.propType in ["house", "houses"] else "Бизнес"
                            prop_id_str = f"№{item['propId']}" if item.get('propId') else f"pos {data.pos}"
                            details = f"Изменен статус ({prop_label} {prop_id_str}): '{old_status}' ➔ '{data.status}' [Скан: {data.scanId}]"
                            await log_manual_action(username, "update_status", srv, details)

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
                    target_item = next((item for item in scan[target_key] if item["pos"] == data.pos), None)
                    if target_item:
                        scan[target_key] = [item for item in scan[target_key] if item["pos"] != data.pos]
                        await save_data_to_file_async()

                        prop_label = "Дом" if data.propType in ["house", "houses"] else "Бизнес"
                        prop_id_str = f"№{target_item['propId']}" if target_item.get('propId') else f"pos {data.pos}"
                        details = f"Удален {prop_label} {prop_id_str} [Скан: {data.scanId}]"
                        await log_manual_action(username, "delete_item", srv, details)

                        return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server or Scan not found")

@app.post("/api/delete_scan")
async def delete_scan(data: DeleteScanModel, username: str = Depends(verify_editor)):
    srv = data.server
    async with data_lock:
        if srv in server_data and "scans" in server_data[srv]:
            initial_count = len(server_data[srv]["scans"])
            server_data[srv]["scans"] = [
                s for s in server_data[srv]["scans"] if s["scanId"] != data.scanId
            ]
            if len(server_data[srv]["scans"]) < initial_count:
                await save_data_to_file_async()

                details = f"Удален весь скан [{data.scanId}]"
                await log_manual_action(username, "delete_scan", srv, details)

                return {"status": "success"}
    raise HTTPException(status_code=404, detail="Scan not found")

@app.post("/api/add_item")
async def add_item(data: AddItemModel, username: str = Depends(verify_editor)):
    srv = data.server
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
                        "updatedAt": datetime.now(MSK_TZ).strftime("%H:%M:%S")
                    }
                    
                    scan[target_key].append(new_record)
                    scan[target_key] = sorted(scan[target_key], key=lambda x: x["pos"])
                    await save_data_to_file_async()

                    prop_label = "Дом" if data.propType in ["house", "houses"] else "Бизнес"
                    prop_id_str = f"№{data.propId}" if data.propId else f"pos {new_pos}"
                    details = f"Добавлен {prop_label} {prop_id_str} (PD: {data.pd}) [Скан: {data.scanId}]"
                    await log_manual_action(username, "add_item", srv, details)

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

# --- ДАШБОРД ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Arizona RP — Мониторинг Слётов</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0b0e14;
            --card-bg: #141822;
            --card-border: #202636;
            --accent-orange: #ff9800;
            --accent-orange-glow: rgba(255, 152, 0, 0.25);
            --accent-blue: #00b0ff;
            --accent-green: #00e676;
            --accent-red: #ff3d00;
            --text-main: #f0f4f8;
            --text-muted: #8a99ad;
            --table-header: #1a202c;
            --table-row-hover: #1c2333;
        }

        * { box-sizing: border-box; }

        body { 
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            background-color: var(--bg-color); 
            color: var(--text-main); 
            margin: 0; 
            padding: 20px 15px; 
            min-height: 100vh;
        }

        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: var(--bg-color); }
        ::-webkit-scrollbar-thumb { background: #263043; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #3b4962; }

        h1 { 
            text-align: center; 
            color: #ffffff; 
            font-size: 1.8rem;
            font-weight: 800;
            letter-spacing: 0.5px;
            margin: 10px 0 25px 0;
            text-transform: uppercase;
            background: linear-gradient(135deg, #fff 30%, var(--accent-orange));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .lottery-banner {
            background: linear-gradient(135deg, rgba(255, 152, 0, 0.15), rgba(156, 39, 176, 0.2));
            border: 1px solid rgba(255, 152, 0, 0.4);
            border-radius: 12px;
            padding: 14px 20px;
            margin: 0 auto 20px auto;
            max-width: 1600px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 15px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        }
        .lottery-info { display: flex; align-items: center; gap: 14px; }
        .lottery-icon { font-size: 2.2rem; line-height: 1; color: #ffd700; filter: drop-shadow(0 0 8px rgba(255, 215, 0, 0.8)); }
        .lottery-title { font-size: 1.05rem; font-weight: 800; color: #ffb74d; text-transform: uppercase; letter-spacing: 0.5px; }
        .lottery-sub { font-size: 0.85rem; color: var(--text-muted); margin-top: 2px; }
        .lottery-sub b { color: #ffffff; }
        .lottery-countdown { display: flex; align-items: center; gap: 10px; background: rgba(15, 18, 26, 0.7); padding: 8px 16px; border-radius: 8px; border: 1px solid var(--card-border); }
        .lottery-label { font-size: 0.85rem; font-weight: 600; color: var(--text-muted); }
        .lottery-timer { font-size: 1.25rem; font-weight: 800; color: #00e676; font-family: monospace; letter-spacing: 1px; }

        .login-box { 
            max-width: 380px; 
            margin: 80px auto; 
            background: var(--card-bg); 
            padding: 35px 30px; 
            border-radius: 12px; 
            border: 1px solid var(--card-border); 
            text-align: center; 
            box-shadow: 0 10px 30px rgba(0,0,0,0.5);
        }
        .login-box h2 { margin-top: 0; margin-bottom: 20px; color: var(--text-main); font-size: 1.4rem; }
        .login-box input { 
            width: 100%; padding: 12px; margin: 8px 0; background: #0f121a; border: 1px solid #263043; color: #fff; border-radius: 6px; font-size: 0.95rem; outline: none; transition: 0.2s;
        }
        .login-box input:focus { border-color: var(--accent-orange); box-shadow: 0 0 8px var(--accent-orange-glow); }
        .login-box button { 
            width: 100%; padding: 12px; margin-top: 15px; background: linear-gradient(135deg, #ff9800, #f57c00); border: none; font-weight: 700; cursor: pointer; border-radius: 6px; color: #121212; font-size: 1rem; transition: transform 0.1s, box-shadow 0.2s;
        }
        .login-box button:hover { transform: translateY(-1px); box-shadow: 0 4px 15px var(--accent-orange-glow); }

        .user-nav { 
            display: flex; justify-content: space-between; align-items: center; max-width: 1600px; margin: 0 auto 20px auto; background: var(--card-bg); padding: 12px 20px; border-radius: 10px; border: 1px solid var(--card-border);
        }
        #user-info { font-weight: 600; font-size: 0.95rem; color: var(--text-muted); }
        .btn-logout { background: rgba(229, 57, 53, 0.15); color: #ef5350; border: 1px solid rgba(229, 57, 53, 0.3); padding: 7px 16px; border-radius: 6px; cursor: pointer; font-weight: 600; transition: 0.2s; }
        .btn-logout:hover { background: #e53935; color: #fff; }

        .tabs { display: flex; justify-content: center; gap: 10px; margin-bottom: 25px; flex-wrap: wrap; }
        .tab-btn { background-color: var(--card-bg); color: var(--text-muted); border: 1px solid var(--card-border); padding: 10px 22px; font-size: 0.95rem; font-weight: 700; border-radius: 8px; cursor: pointer; transition: all 0.2s ease; }
        .tab-btn.active { background: linear-gradient(135deg, #ff9800, #f57c00); color: #121212; border-color: #ff9800; box-shadow: 0 4px 15px var(--accent-orange-glow); }
        .tab-btn:hover:not(.active) { background-color: #1c2333; color: #fff; border-color: #2e384e; }

        .tab-content { display: none; }
        .tab-content.active { display: block; }

        .upcoming-filters { display: flex; justify-content: center; gap: 10px; margin-bottom: 20px; }
        .time-filter-btn { background-color: var(--card-bg); color: var(--text-muted); border: 1px solid var(--card-border); padding: 8px 18px; font-size: 0.85rem; font-weight: 700; border-radius: 20px; cursor: pointer; transition: 0.2s; }
        .time-filter-btn.active { background-color: #e53935; color: #ffffff; border-color: #ef5350; box-shadow: 0 0 12px rgba(229, 57, 53, 0.4); }

        .filter-panel { background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 10px; padding: 14px 20px; margin: 0 auto 20px auto; max-width: 1600px; display: flex; flex-wrap: wrap; gap: 20px; align-items: center; justify-content: center; }
        .filter-group { display: flex; align-items: center; gap: 10px; }
        .filter-group label { font-size: 0.85rem; color: var(--accent-orange); font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }
        .filter-select { background: #0f121a; color: #fff; border: 1px solid #263043; padding: 7px 12px; border-radius: 6px; font-size: 0.85rem; font-weight: 600; outline: none; cursor: pointer; }
        .fav-btn { cursor: pointer; font-size: 1.1rem; user-select: none; margin-right: 6px; transition: transform 0.15s ease; display: inline-block; }
        .fav-btn:hover { transform: scale(1.3); }

        .servers-container { display: grid; grid-template-columns: repeat(1, 1fr); gap: 16px; max-width: 1600px; margin: 0 auto; }
        @media (min-width: 650px) { .servers-container { grid-template-columns: repeat(2, 1fr); } }
        @media (min-width: 1100px) { .servers-container { grid-template-columns: repeat(3, 1fr); } }

        #servers-manage {
            display: grid;
            grid-template-columns: repeat(1, 1fr);
            gap: 16px;
            max-width: 1600px;
            margin: 0 auto;
        }
        @media (min-width: 650px) { #servers-manage { grid-template-columns: repeat(2, 1fr); } }
        @media (min-width: 1100px) { #servers-manage { grid-template-columns: repeat(3, 1fr); } }

        .server-card { background-color: var(--card-bg); border: 1px solid var(--card-border); border-radius: 10px; padding: 14px; box-shadow: 0 4px 12px rgba(0,0,0,0.25); display: flex; flex-direction: column; transition: transform 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease; }
        .server-card:hover { border-color: #2e384e; box-shadow: 0 6px 18px rgba(0,0,0,0.4); }
        
        .server-header { border-bottom: 1px solid #1f2736; padding-bottom: 8px; margin-bottom: 10px; }
        .server-title { font-size: 1.05rem; font-weight: 700; color: #ffffff; display: flex; justify-content: space-between; align-items: center; }
        .season-badge { font-size: 0.7rem; background: rgba(255, 152, 0, 0.12); color: #ffb74d; border: 1px solid rgba(255, 152, 0, 0.3); padding: 2px 8px; border-radius: 12px; font-weight: 600; white-space: nowrap; }
        .scan-info-tag { font-size: 0.78rem; color: #81c784; font-weight: 600; margin-top: 6px; display: block; }
        
        .scan-tabs-bar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; border-bottom: 1px solid #1f2736; padding-bottom: 6px; gap: 8px; }
        .scan-tabs-container { display: flex; gap: 6px; overflow-x: auto; padding-bottom: 2px; }
        .scan-subtab { 
            background-color: #0f121a; color: var(--text-muted); border: 1px solid #232b3c; padding: 4px 8px; font-size: 0.75rem; border-radius: 4px; cursor: pointer; white-space: nowrap; transition: 0.2s; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;
        }

        .scan-subtab.pair-current { background-color: rgba(46, 125, 50, 0.25); color: #81c784; border: 1px solid #4caf50; }
        .scan-subtab.pair-current.active { background-color: #2e7d32; color: #ffffff; border-color: #66bb6a; box-shadow: 0 0 10px rgba(76, 175, 80, 0.5); }

        .scan-subtab.pair-outdated { background-color: rgba(55, 71, 79, 0.3); color: #b0bec5; border: 1px solid #546e7a; }
        .scan-subtab.pair-outdated.active { background-color: #455a64; color: #ffffff; border-color: #78909c; }

        .scan-subtab.single-pending { background-color: rgba(255, 152, 0, 0.2); color: #ffb74d; border: 1px solid #ff9800; }
        .scan-subtab.single-pending.active { background-color: #f57c00; color: #ffffff; border-color: #ffe082; box-shadow: 0 0 10px rgba(255, 152, 0, 0.5); }

        .scan-subtab.single-failed { background-color: rgba(211, 47, 47, 0.25); color: #ef5350; border: 1px solid #ef5350; }
        .scan-subtab.single-failed.active { background-color: #c62828; color: #ffffff; border-color: #ff8a80; box-shadow: 0 0 10px rgba(239, 83, 80, 0.5); }

        .scan-subtab:hover:not(.active) { opacity: 1; filter: brightness(1.2); }

        .btn-delete-scan { background-color: rgba(183, 28, 28, 0.2); color: #ef5350; border: 1px solid rgba(239, 83, 80, 0.4); padding: 3px 6px; border-radius: 4px; font-size: 0.7rem; font-weight: 700; cursor: pointer; transition: 0.2s; white-space: nowrap; }
        .btn-delete-scan:hover { background-color: #d32f2f; color: #fff; }

        .tables-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; flex-grow: 1; }
        @media (max-width: 480px) { .tables-grid { grid-template-columns: 1fr; } }
        
        .section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
        .section-title { font-size: 0.8rem; font-weight: 700; color: #00e5ff; text-transform: uppercase; letter-spacing: 0.5px; }
        
        .btn-add { background-color: #0288d1; color: white; border: none; border-radius: 4px; padding: 2px 6px; font-size: 0.75rem; font-weight: 700; cursor: pointer; transition: 0.2s; }
        .btn-add:hover { background-color: #0277bd; }

        table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }
        th, td { padding: 5px 4px; text-align: left; border-bottom: 1px solid #1a202c; vertical-align: middle; }
        th { background-color: var(--table-header); color: var(--text-muted); font-weight: 600; font-size: 0.72rem; }
        tr:hover td { background-color: var(--table-row-hover); }

        .pd-badge { background-color: #37474f; color: #eceff1; padding: 2px 5px; border-radius: 4px; font-weight: 700; font-size: 0.72rem; }
        .pd-badge-drop { background-color: #b71c1c; color: #ffee58; padding: 2px 5px; border-radius: 4px; font-weight: 800; font-size: 0.72rem; animation: pulse 1.5s infinite; }
        
        .time-left-badge { font-weight: 700; color: #ffb74d; font-size: 0.75rem; background-color: rgba(255, 152, 0, 0.1); padding: 2px 5px; border-radius: 4px; border: 1px solid rgba(255, 152, 0, 0.3); white-space: nowrap; }
        .time-left-frozen { font-weight: 700; color: #64b5f6; font-size: 0.75rem; }

        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
        }

        .btn-circle-group { 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            gap: 3px; 
            flex-wrap: nowrap;
        }

        .btn-circle { 
            width: 22px; 
            height: 22px; 
            border-radius: 50%; 
            border: 1px solid #2e384e; 
            background-color: #0f121a; 
            color: #8a99ad; 
            display: inline-flex; 
            align-items: center; 
            justify-content: center; 
            font-size: 0.65rem; 
            cursor: pointer; 
            transition: all 0.2s ease; 
            padding: 0; 
            line-height: 1;
            user-select: none;
            flex-shrink: 0;
        }

        .btn-circle:hover { 
            transform: scale(1.18); 
            border-color: #4a5a7a;
        }

        .btn-circle.active-insured { 
            background: #2e7d32; 
            border-color: #4caf50; 
            color: #ffffff; 
            box-shadow: 0 0 8px rgba(76, 175, 80, 0.6); 
        }

        .btn-circle.active-uninsured { 
            background: #c62828; 
            border-color: #ef5350; 
            color: #ffffff; 
            box-shadow: 0 0 8px rgba(239, 83, 80, 0.6); 
        }

        .btn-circle.active-noact { 
            background: #e65100; 
            border-color: #ff9800; 
            color: #ffffff; 
            box-shadow: 0 0 8px rgba(255, 152, 0, 0.6); 
        }

        .btn-circle.active-frozen { 
            background: #1565c0; 
            border-color: #42a5f5; 
            color: #ffffff; 
            box-shadow: 0 0 8px rgba(66, 165, 245, 0.6); 
        }

        .btn-circle.active-pending {
            background: #f57c00;
            border-color: #ffe082;
            color: #ffffff;
            box-shadow: 0 0 8px rgba(255, 152, 0, 0.6);
        }

        .btn-circle-del { 
            background: rgba(239, 83, 80, 0.12); 
            border-color: rgba(239, 83, 80, 0.35); 
            color: #ef5350; 
        }

        .btn-circle-del:hover { 
            background: #ef5350; 
            border-color: #ff5252; 
            color: #ffffff; 
            transform: scale(1.18); 
        }
        
        .status-text { font-weight: 700; font-size: 0.75rem; padding: 2px 4px; border-radius: 3px; display: inline-block; }
        .status-insured { color: #81c784; }
        .status-uninsured { color: #e57373; }
        .status-noact { color: #ff5252; }
        .status-frozen { color: #64b5f6; }
        .status-pending { color: #ffb74d; font-weight: 700; font-size: 0.75rem; }

        .btn-user-del { background-color: #c62828; color: #fff; border: none; padding: 4px 8px; border-radius: 4px; cursor: pointer; font-size: 0.8em; }
        .btn-user-del:hover { background-color: #e53935; }
        
        .select-role { background: #0f121a; color: #fff; border: 1px solid #263043; padding: 4px; border-radius: 4px; font-size: 0.85em; }
        .empty { color: #546e7a; font-style: italic; font-size: 0.78rem; display: block; padding: 4px 0; }
        .empty-center { text-align: center; color: var(--text-muted); font-style: italic; padding: 30px; background-color: var(--card-bg); border-radius: 10px; border: 1px solid var(--card-border); max-width: 600px; margin: 0 auto; grid-column: 1 / -1; }
    </style>
</head>
<body>

    <!-- ЭКРАН ВХОДА -->
    <div id="login-screen" class="login-box" style="display: none;">
        <h2>Авторизация</h2>
        <input type="text" id="login-username" placeholder="Логин">
        <input type="password" id="login-password" placeholder="Пароль">
        <button onclick="handleLogin()">Войти</button>
    </div>

    <!-- ОСНОВНОЙ ДАШБОРД -->
    <div id="main-dashboard" style="display: none;">
        <div class="user-nav">
            <span id="user-info">Загрузка...</span>
            <button class="btn-logout" onclick="handleLogout()">Выйти</button>
        </div>

        <div class="lottery-banner">
            <div class="lottery-info">
                <div class="lottery-icon">🎫</div>
                <div>
                    <div class="lottery-title">Лотерейные билеты</div>
                    <div class="lottery-sub">Слет билетов происходит каждый день в <b>21:10 МСК</b></div>
                </div>
            </div>
            <div class="lottery-countdown">
                <div class="lottery-label">До слета:</div>
                <div class="lottery-timer" id="lottery-timer">00:00:00</div>
            </div>
        </div>

        <h1>Arizona RP — Мониторинг Слётов</h1>

        <div class="tabs">
            <button id="btn-tab-view" class="tab-btn active" onclick="switchTab('view')">📊 Слёты</button>
            <button id="btn-tab-upcoming" class="tab-btn" onclick="switchTab('upcoming')">🔥 Ближайшие слеты</button>
            <button id="btn-tab-manage" class="tab-btn" onclick="switchTab('manage')" style="display: none;">⚙️ Управление</button>
            <button id="btn-tab-admin" class="tab-btn" onclick="switchTab('admin')" style="display: none;">👑 Админ-панель</button>
        </div>

        <!-- Вкладка: Все слёты (Общий вид) -->
        <div id="tab-view" class="tab-content active">
            <div class="filter-panel">
                <div class="filter-group">
                    <label for="filter-season">Сезон:</label>
                    <select id="filter-season" class="filter-select" onchange="renderViewTab()">
                        <option value="all">Все сезоны</option>
                        <option value="1">📱 По инфе (1)</option>
                        <option value="2">⌨ Скорострелы (2)</option>
                        <option value="3">🏎️ Автогонки (3)</option>
                        <option value="4">✈️ По новому (4)</option>
                        <option value="5">🏍 Мотогонки (5)</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label for="filter-fav">Избранное:</label>
                    <select id="filter-fav" class="filter-select" onchange="renderViewTab()">
                        <option value="all">Все серверы</option>
                        <option value="fav_only">Только ⭐ Избранное</option>
                    </select>
                </div>
            </div>
            <div id="servers-view" class="servers-container"></div>
        </div>

        <!-- Вкладка: Ближайшие слеты -->
        <div id="tab-upcoming" class="tab-content">
            <div class="upcoming-filters">
                <button id="btn-upcoming-1h" class="time-filter-btn active" onclick="setUpcomingHoursFilter(1)">В этот час</button>
                <button id="btn-upcoming-2h" class="time-filter-btn" onclick="setUpcomingHoursFilter(2)">Через 2 часа</button>
                <button id="btn-upcoming-3h" class="time-filter-btn" onclick="setUpcomingHoursFilter(3)">Через 3 часа</button>
            </div>
            <div id="servers-upcoming" class="servers-container"></div>
        </div>

        <!-- Вкладка: Управление -->
        <div id="tab-manage" class="tab-content">
            <div id="admin-manual-logs-card" class="server-card" style="display: none; max-width: 1600px; margin: 0 auto 20px auto;">
                <div style="display: flex; justify-content: space-between; align-items: center; cursor: pointer;" onclick="toggleManualLogs()">
                    <h3 style="margin: 0; color: var(--accent-orange); font-size: 1.1rem; display: flex; align-items: center; gap: 8px;">
                        📋 Лог ручных действий (Управление)
                    </h3>
                    <button id="btn-toggle-manual-logs" style="background: rgba(255,152,0,0.15); color: #ffb74d; border: 1px solid rgba(255,152,0,0.4); border-radius: 4px; padding: 4px 10px; font-size: 0.8rem; cursor: pointer; font-weight: 600;">
                        ▲ Свернуть
                    </button>
                </div>
                <div id="admin-manual-logs-table" style="max-height: 250px; overflow-y: auto; margin-top: 12px; transition: all 0.3s ease;"></div>
            </div>
            <div id="servers-manage"></div>
        </div>

        <!-- Вкладка: Админка -->
        <div id="tab-admin" class="tab-content">
            <div style="max-width: 1000px; margin: 0 auto; display: flex; flex-direction: column; gap: 20px;">
                <div class="server-card">
                    <h3>Создание пользователя</h3>
                    <div style="display: flex; gap: 10px; flex-wrap: wrap;">
                        <input type="text" id="new-username" placeholder="Логин" style="background: #0f121a; color: #fff; border: 1px solid #263043; padding: 8px; border-radius: 4px;">
                        <input type="password" id="new-password" placeholder="Пароль" style="background: #0f121a; color: #fff; border: 1px solid #263043; padding: 8px; border-radius: 4px;">
                        <select id="new-role" class="select-role">
                            <option value="user">User</option>
                            <option value="support">Support</option>
                            <option value="admin">Admin</option>
                        </select>
                        <button class="btn-add" style="padding: 8px 16px;" onclick="handleCreateUser()">Создать</button>
                    </div>
                </div>

                <div class="server-card">
                    <h3>Список пользователей</h3>
                    <div id="admin-users-table"></div>
                </div>

                <div class="server-card">
                    <h3>Логи сканирований (за сегодня)</h3>
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

        function formatScanTime(scanTimeStr) {
            if (!scanTimeStr) return '';
            const parts = scanTimeStr.split(' ');
            if (parts.length === 2) {
                const dateParts = parts[0].split('-');
                if (dateParts.length === 3) {
                    return `${dateParts[2]}.${dateParts[1]}.${dateParts[0]} ${parts[1]}`;
                }
            }
            return scanTimeStr;
        }

        function getScanTooltip(scan) {
            if (!scan) return '';
            const formatted = formatScanTime(scan.scanTime);
            const parts = formatted.split(' ');
            const dateStr = parts[0] || '';
            const timeStr = parts[1] || '';
            const hasPair = !!(scan.hasPair || scan.isConfirmed);
            const isOutdated = !!scan.isOutdated;
            let statusText = '';
            if (hasPair && !isOutdated) {
                statusText = 'подтвержденная пара';
            } else if (hasPair && isOutdated) {
                statusText = 'устаревшая пара';
            } else if (!hasPair && !isOutdated) {
                statusText = 'ждет пару';
            } else {
                statusText = 'не подтвержденный';
            }
            return `Дата: ${dateStr}, Время: ${timeStr}\nСтатус: ${statusText}`;
        }

        function getScanBadgeInfo(scan) {
            if (!scan) return { emoji: '', cssClass: '', label: 'Нет данных' };

            const hasPair = !!(scan.hasPair || scan.isConfirmed);
            const isOutdated = !!scan.isOutdated;

            if (hasPair && !isOutdated) {
                return { emoji: '📎', cssClass: 'scan-subtab pair-current', label: 'Подтверждённая пара' };
            } else if (hasPair && isOutdated) {
                return { emoji: '✔', cssClass: 'scan-subtab pair-outdated', label: 'Устаревшая пара' };
            } else if (!hasPair && !isOutdated) {
                return { emoji: '⏳', cssClass: 'scan-subtab single-pending', label: 'Новый (в ожидании пары)' };
            } else {
                return { emoji: '❌', cssClass: 'scan-subtab single-failed', label: 'Не нашел пару' };
            }
        }

        function getStatusHTML(status, isPending) {
            if (isPending || status === 'pending') {
                return '<span class="status-pending">🔍 Новый</span>';
            }
            switch (status) {
                case 'insured':
                    return '<span class="status-text status-insured">🛡️ Страховка</span>';
                case 'uninsured':
                    return '<span class="status-text status-uninsured">❌ Без страховки</span>';
                case 'no_activity':
                    return '<span class="status-text status-noact">🚫 Без актива</span>';
                case 'frozen':
                    return '<span class="status-text status-frozen">❄️ Заморожен</span>';
                default:
                    return `<span class="status-text">${status}</span>`;
            }
        }

        function getMskNowDate() {
            const now = new Date();
            const utcMs = now.getTime() + (now.getTimezoneOffset() * 60000);
            return new Date(utcMs + (3 * 3600000));
        }

        function parseScanTimeMSK(scanTimeStr) {
            if (!scanTimeStr) return getMskNowDate();
            try {
                const parts = scanTimeStr.split(' ');
                const d = parts[0].split('-');
                const t = parts[1].split(':');
                return new Date(Date.UTC(parseInt(d[0]), parseInt(d[1]) - 1, parseInt(d[2]), parseInt(t[0]), parseInt(t[1] || 0), parseInt(t[2] || 0)));
            } catch (e) {
                return getMskNowDate();
            }
        }

        function calculateElapsedPaydays(scanTimeStr) {
            if (!scanTimeStr) return 0;
            try {
                const scanDate = parseScanTimeMSK(scanTimeStr);
                const mskNow = getMskNowDate();
                
                const scanHourStart = new Date(Date.UTC(scanDate.getUTCFullYear(), scanDate.getUTCMonth(), scanDate.getUTCDate(), scanDate.getUTCHours(), 0, 0));
                const nowHourStart = new Date(Date.UTC(mskNow.getFullYear(), mskNow.getMonth(), mskNow.getDate(), mskNow.getHours(), 0, 0));
                
                const diffHours = Math.floor((nowHourStart.getTime() - scanHourStart.getTime()) / (3600 * 1000));
                return Math.max(0, diffHours);
            } catch (e) {
                return 0;
            }
        }

        function calculateItemDropInfo(item, scanTimeStr, propType, dropRules) {
            const basePd = item.basePd !== undefined ? item.basePd : item.pd;
            const status = item.status || 'insured';

            let decrement = 1;
            if (status === 'uninsured') decrement = 2;
            else if (status === 'no_activity') decrement = 4;
            else if (status === 'frozen' || status === 'pending') decrement = 0;

            const elapsedPaydays = calculateElapsedPaydays(scanTimeStr);
            const currentPd = Math.max(0, basePd - (elapsedPaydays * decrement));

            if (status === 'frozen') {
                return {
                    currentPd: currentPd,
                    timeLeftStr: '❄️ Заморожен',
                    isFrozen: true,
                    isPending: false,
                    hasDropped: false,
                    dropDate: null,
                    paydaysLeftToDrop: 999
                };
            }

            if (status === 'pending' || item.isPendingPair) {
                return {
                    currentPd: currentPd,
                    timeLeftStr: '🔍 Новый',
                    isFrozen: false,
                    isPending: true,
                    hasDropped: false,
                    dropDate: null,
                    paydaysLeftToDrop: 999
                };
            }

            const rules = dropRules?.[propType] || { insured: 2, uninsured_min: 2, uninsured_max: 3 };
            let dropLimit = rules.insured;
            if (status === 'uninsured' || status === 'no_activity') {
                dropLimit = rules.uninsured_min;
            }

            const paydaysLeftToDrop = Math.max(0, Math.ceil((currentPd - (dropLimit - 1)) / (decrement || 1)));

            const mskNow = getMskNowDate();
            
            // Вычисление точной даты/времени слета
            let dropDate = new Date(mskNow.getTime());
            if (paydaysLeftToDrop === 0) {
                const nextHourDate = new Date(Date.UTC(mskNow.getFullYear(), mskNow.getMonth(), mskNow.getDate(), mskNow.getHours() + 1, 0, 0));
                if (nextHourDate.getUTCHours() === 5) {
                    nextHourDate.setUTCHours(6); // Рестарт в 5:00 МСК -> слёт переносится на 6:00 МСК
                }
                dropDate = nextHourDate;
            } else {
                let currentCalcHour = mskNow.getHours();
                let hoursToAdd = 0;
                for (let i = 1; i <= paydaysLeftToDrop; i++) {
                    hoursToAdd++;
                    let targetHour = (currentCalcHour + i) % 24;
                    if (targetHour === 5) {
                        hoursToAdd++; // Пропускаем слеты в 5:00 МСК, слёт происходит в 6:00 МСК
                    }
                }
                const nextPayday = new Date(Date.UTC(mskNow.getFullYear(), mskNow.getMonth(), mskNow.getDate(), mskNow.getHours() + hoursToAdd, 0, 0));
                dropDate = nextPayday;
            }

            // Объекты НЕ пропадают до 6:00 МСК и пропадают ТОЛЬКО после 6:00 МСК
            const hasDropped = (mskNow.getTime() >= dropDate.getTime());

            const diffMs = dropDate.getTime() - mskNow.getTime();
            let timeLeftStr = '0 ч 0 мин';
            if (diffMs > 0) {
                const totalMin = Math.floor(diffMs / 60000);
                const hours = Math.floor(totalMin / 60);
                const minutes = totalMin % 60;
                timeLeftStr = `${hours} ч ${minutes} мин`;
            }

            return {
                currentPd: currentPd,
                timeLeftStr: timeLeftStr,
                isFrozen: false,
                isPending: false,
                hasDropped: hasDropped,
                dropDate: dropDate,
                paydaysLeftToDrop: paydaysLeftToDrop,
                diffMs: diffMs
            };
        }

        function toggleManualLogs() {
            const tableDiv = document.getElementById('admin-manual-logs-table');
            const btn = document.getElementById('btn-toggle-manual-logs');
            if (tableDiv.style.display === 'none') {
                tableDiv.style.display = 'block';
                btn.innerText = '▲ Свернуть';
            } else {
                tableDiv.style.display = 'none';
                btn.innerText = '▼ Развернуть';
            }
        }

        function updateLotteryTimer() {
            const mskNow = getMskNowDate();
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

        function toggleFavorite(srv) {
            if (favoriteServers.includes(srv)) {
                favoriteServers = favoriteServers.filter(s => s !== srv);
            } else {
                favoriteServers.push(srv);
            }
            localStorage.setItem('fav_servers', JSON.stringify(favoriteServers));
            renderViewTab();
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
            const u = document.getElementById('login-username').value.trim();
            const p = document.getElementById('login-password').value.trim();
            if (!u || !p) return alert('Заполните логин и пароль');

            try {
                const res = await fetch('/api/auth/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username: u, password: p })
                });
                if (!res.ok) {
                    const err = await res.json();
                    return alert(err.detail || 'Ошибка авторизации');
                }
                checkAuth();
            } catch (e) {
                alert('Ошибка соединения с сервером');
            }
        }

        async function handleLogout() {
            await fetch('/api/auth/logout', { method: 'POST' });
            checkAuth();
        }

        function switchTab(tab) {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

            document.getElementById(`btn-tab-${tab}`).classList.add('active');
            document.getElementById(`tab-${tab}`).classList.add('active');

            if (tab === 'view') renderViewTab();
            else if (tab === 'upcoming') renderUpcomingTab();
            else if (tab === 'manage') renderManageTab();
            else if (tab === 'admin') renderAdminTab();
        }

        function setUpcomingHoursFilter(hours) {
            selectedUpcomingHours = hours;
            document.querySelectorAll('.time-filter-btn').forEach(btn => btn.classList.remove('active'));
            document.getElementById(`btn-upcoming-${hours}h`).classList.add('active');
            renderUpcomingTab();
        }

        async function initDashboard() {
            updateLotteryTimer();
            setInterval(updateLotteryTimer, 1000);
            await loadPaydaysData();
            setInterval(loadPaydaysData, 15000);
        }

        async function loadPaydaysData() {
            try {
                const res = await fetch('/api/paydays');
                if (!res.ok) return;
                globalServerData = await res.json();

                const activeTab = document.querySelector('.tab-content.active')?.id;
                if (activeTab === 'tab-view') renderViewTab();
                else if (activeTab === 'tab-upcoming') renderUpcomingTab();
                else if (activeTab === 'tab-manage') renderManageTab();
            } catch (e) {
                console.error("Ошибка загрузки данных paydays:", e);
            }
        }

        function renderViewTab() {
            const container = document.getElementById('servers-view');
            if (!container) return;
            container.innerHTML = '';

            const seasonFilter = document.getElementById('filter-season').value;
            const favFilter = document.getElementById('filter-fav').value;

            ALL_SERVERS.forEach(srv => {
                const sData = globalServerData[srv] || { scans: [], latestConfirmedScan: null, season: { id: 1, display: '📱 По инфе (1)' }, dropRules: {} };
                
                if (seasonFilter !== 'all' && String(sData.season?.id) !== String(seasonFilter)) return;
                
                const isFav = favoriteServers.includes(srv);
                if (favFilter === 'fav_only' && !isFav) return;

                const scan = sData.latestConfirmedScan;
                const dropRules = sData.dropRules || {};

                const card = document.createElement('div');
                card.className = 'server-card';

                const starIcon = isFav ? '⭐' : '☆';
                const headerHTML = `
                    <div class="server-header">
                        <div class="server-title">
                            <span>
                                <span class="fav-btn" onclick="toggleFavorite('${srv}')">${starIcon}</span>
                                ${getServerDisplayName(srv)}
                            </span>
                            <span class="season-badge">${sData.season?.display || ''}</span>
                        </div>
                        ${scan ? `<span class="scan-info-tag">Последняя пара: ${formatScanTime(scan.scanTime)}</span>` : '<span class="scan-info-tag" style="color: var(--text-muted)">Нет подтвержденных пар</span>'}
                    </div>
                `;

                let housesRows = '';
                if (scan && scan.houses && scan.houses.length > 0) {
                    scan.houses.forEach(h => {
                        const dropInfo = calculateItemDropInfo(h, scan.scanTime, 'house', dropRules);
                        if (dropInfo.hasDropped) return; // Не показываем слетевшие после 6:00 МСК

                        const pdClass = dropInfo.currentPd <= (dropRules?.house?.insured || 2) ? 'pd-badge-drop' : 'pd-badge';
                        const timeBadge = dropInfo.isFrozen ? '<span class="time-left-frozen">❄️ Заморожен</span>' : `<span class="time-left-badge">${dropInfo.timeLeftStr}</span>`;

                        housesRows += `
                            <tr>
                                <td>${h.propId ? `№${h.propId}` : `поз. ${h.pos}`}</td>
                                <td><span class="${pdClass}">${dropInfo.currentPd} PD</span></td>
                                <td>${timeBadge}</td>
                                <td>${getStatusHTML(h.status, h.isPendingPair)}</td>
                            </tr>
                        `;
                    });
                }

                let bizRows = '';
                if (scan && scan.businesses && scan.businesses.length > 0) {
                    scan.businesses.forEach(b => {
                        const dropInfo = calculateItemDropInfo(b, scan.scanTime, 'biz', dropRules);
                        if (dropInfo.hasDropped) return;

                        const pdClass = dropInfo.currentPd <= (dropRules?.biz?.insured || 2) ? 'pd-badge-drop' : 'pd-badge';
                        const timeBadge = dropInfo.isFrozen ? '<span class="time-left-frozen">❄️ Заморожен</span>' : `<span class="time-left-badge">${dropInfo.timeLeftStr}</span>`;

                        bizRows += `
                            <tr>
                                <td>${b.propId ? `№${b.propId}` : `поз. ${b.pos}`}</td>
                                <td><span class="${pdClass}">${dropInfo.currentPd} PD</span></td>
                                <td>${timeBadge}</td>
                                <td>${getStatusHTML(b.status, b.isPendingPair)}</td>
                            </tr>
                        `;
                    });
                }

                const tablesHTML = `
                    <div class="tables-grid">
                        <div>
                            <div class="section-header"><span class="section-title">🏠 Дома</span></div>
                            ${housesRows ? `<table><thead><tr><th>Объект</th><th>PD</th><th>Слёт</th><th>Статус</th></tr></thead><tbody>${housesRows}</tbody></table>` : '<span class="empty">Нет данных</span>'}
                        </div>
                        <div>
                            <div class="section-header"><span class="section-title">🏢 Бизнесы</span></div>
                            ${bizRows ? `<table><thead><tr><th>Объект</th><th>PD</th><th>Слёт</th><th>Статус</th></tr></thead><tbody>${bizRows}</tbody></table>` : '<span class="empty">Нет данных</span>'}
                        </div>
                    </div>
                `;

                card.innerHTML = headerHTML + tablesHTML;
                container.appendChild(card);
            });

            if (container.children.length === 0) {
                container.innerHTML = '<div class="empty-center">Нет серверов, соответствующих выбранным фильтрам</div>';
            }
        }

        function renderUpcomingTab() {
            const container = document.getElementById('servers-upcoming');
            if (!container) return;
            container.innerHTML = '';

            let hasItems = false;

            ALL_SERVERS.forEach(srv => {
                const sData = globalServerData[srv] || { scans: [], latestConfirmedScan: null, dropRules: {} };
                const scan = sData.latestConfirmedScan;
                if (!scan) return;

                const dropRules = sData.dropRules || {};
                let matchingHouses = [];
                let matchingBiz = [];

                if (scan.houses) {
                    scan.houses.forEach(h => {
                        const dropInfo = calculateItemDropInfo(h, scan.scanTime, 'house', dropRules);
                        if (!dropInfo.hasDropped && !dropInfo.isFrozen && !dropInfo.isPending) {
                            if (dropInfo.paydaysLeftToDrop < selectedUpcomingHours) {
                                matchingHouses.push({ item: h, dropInfo });
                            }
                        }
                    });
                }

                if (scan.businesses) {
                    scan.businesses.forEach(b => {
                        const dropInfo = calculateItemDropInfo(b, scan.scanTime, 'biz', dropRules);
                        if (!dropInfo.hasDropped && !dropInfo.isFrozen && !dropInfo.isPending) {
                            if (dropInfo.paydaysLeftToDrop < selectedUpcomingHours) {
                                matchingBiz.push({ item: b, dropInfo });
                            }
                        }
                    });
                }

                if (matchingHouses.length === 0 && matchingBiz.length === 0) return;
                hasItems = true;

                const card = document.createElement('div');
                card.className = 'server-card';

                const headerHTML = `
                    <div class="server-header">
                        <div class="server-title">
                            <span>${getServerDisplayName(srv)}</span>
                            <span class="season-badge">${sData.season?.display || ''}</span>
                        </div>
                        <span class="scan-info-tag">Ожидается слёт в ближайшее время</span>
                    </div>
                `;

                let housesRows = '';
                matchingHouses.forEach(({ item, dropInfo }) => {
                    housesRows += `
                        <tr>
                            <td>${item.propId ? `№${item.propId}` : `поз. ${item.pos}`}</td>
                            <td><span class="pd-badge-drop">${dropInfo.currentPd} PD</span></td>
                            <td><span class="time-left-badge">${dropInfo.timeLeftStr}</span></td>
                            <td>${getStatusHTML(item.status, item.isPendingPair)}</td>
                        </tr>
                    `;
                });

                let bizRows = '';
                matchingBiz.forEach(({ item, dropInfo }) => {
                    bizRows += `
                        <tr>
                            <td>${item.propId ? `№${item.propId}` : `поз. ${item.pos}`}</td>
                            <td><span class="pd-badge-drop">${dropInfo.currentPd} PD</span></td>
                            <td><span class="time-left-badge">${dropInfo.timeLeftStr}</span></td>
                            <td>${getStatusHTML(item.status, item.isPendingPair)}</td>
                        </tr>
                    `;
                });

                const tablesHTML = `
                    <div class="tables-grid">
                        <div>
                            <div class="section-header"><span class="section-title">🏠 Дома</span></div>
                            ${housesRows ? `<table><thead><tr><th>Объект</th><th>PD</th><th>Слёт</th><th>Статус</th></tr></thead><tbody>${housesRows}</tbody></table>` : '<span class="empty">Нет</span>'}
                        </div>
                        <div>
                            <div class="section-header"><span class="section-title">🏢 Бизнесы</span></div>
                            ${bizRows ? `<table><thead><tr><th>Объект</th><th>PD</th><th>Слёт</th><th>Статус</th></tr></thead><tbody>${bizRows}</tbody></table>` : '<span class="empty">Нет</span>'}
                        </div>
                    </div>
                `;

                card.innerHTML = headerHTML + tablesHTML;
                container.appendChild(card);
            });

            if (!hasItems) {
                container.innerHTML = '<div class="empty-center">В ближайшее время слётов не ожидается</div>';
            }
        }

        function renderManageTab() {
            if (currentUser && (currentUser.role === 'admin' || currentUser.role === 'support')) {
                document.getElementById('admin-manual-logs-card').style.display = 'block';
                loadManualLogs();
            } else {
                document.getElementById('admin-manual-logs-card').style.display = 'none';
            }

            const container = document.getElementById('servers-manage');
            if (!container) return;
            container.innerHTML = '';

            ALL_SERVERS.forEach(srv => {
                const sData = globalServerData[srv] || { scans: [], season: {} };
                const scans = sData.scans || [];

                if (!activeServerScans[srv] && scans.length > 0) {
                    activeServerScans[srv] = scans[scans.length - 1].scanId;
                }

                const activeScanId = activeServerScans[srv];
                const activeScan = scans.find(s => s.scanId === activeScanId) || scans[scans.length - 1];

                const card = document.createElement('div');
                card.className = 'server-card';

                let tabsHTML = '<div class="scan-tabs-container">';
                if (scans.length > 0) {
                    scans.forEach(s => {
                        const badgeInfo = getScanBadgeInfo(s);
                        const isActive = (activeScan && s.scanId === activeScan.scanId) ? 'active' : '';
                        const tooltip = getScanTooltip(s);
                        tabsHTML += `
                            <button class="${badgeInfo.cssClass} ${isActive}" title="${tooltip}" onclick="setActiveScan('${srv}', '${s.scanId}')">
                                ${badgeInfo.emoji} ${s.hourLabel || s.scanId}
                            </button>
                        `;
                    });
                } else {
                    tabsHTML += '<span class="empty">Нет сканов</span>';
                }
                tabsHTML += '</div>';

                const deleteScanBtnHTML = (activeScan && (currentUser.role === 'admin' || currentUser.role === 'support')) ? 
                    `<button class="btn-delete-scan" onclick="handleDeleteScan('${srv}', '${activeScan.scanId}')">Удалить скан</button>` : '';

                const scanBarHTML = `
                    <div class="scan-tabs-bar">
                        ${tabsHTML}
                        ${deleteScanBtnHTML}
                    </div>
                `;

                const canEdit = currentUser && (currentUser.role === 'admin' || currentUser.role === 'support');

                let housesRows = '';
                if (activeScan && activeScan.houses) {
                    activeScan.houses.forEach(h => {
                        const isPending = h.isPendingPair || h.status === 'pending';
                        
                        let actionsHTML = '';
                        if (canEdit) {
                            actionsHTML = `
                                <div class="btn-circle-group">
                                    <button class="btn-circle ${h.status === 'insured' && !isPending ? 'active-insured' : ''}" title="Страхованный" onclick="updateItemStatus('${srv}', '${activeScan.scanId}', 'house', ${h.pos}, 'insured')">🛡️</button>
                                    <button class="btn-circle ${h.status === 'uninsured' && !isPending ? 'active-uninsured' : ''}" title="Не страхованный" onclick="updateItemStatus('${srv}', '${activeScan.scanId}', 'house', ${h.pos}, 'uninsured')">❌</button>
                                    <button class="btn-circle ${h.status === 'frozen' && !isPending ? 'active-frozen' : ''}" title="Заморожен" onclick="updateItemStatus('${srv}', '${activeScan.scanId}', 'house', ${h.pos}, 'frozen')">❄️</button>
                                    <button class="btn-circle btn-circle-del" title="Удалить" onclick="deleteItem('${srv}', '${activeScan.scanId}', 'house', ${h.pos})">🗑️</button>
                                </div>
                            `;
                        }

                        // В управлении НЕ вычитаются пейдеи, все остается так же, как при сканировании (h.pd или h.basePd)
                        const displayPd = h.basePd !== undefined ? h.basePd : h.pd;

                        housesRows += `
                            <tr>
                                <td>${h.propId ? `№${h.propId}` : `поз. ${h.pos}`}</td>
                                <td><span class="pd-badge">${displayPd} PD</span></td>
                                <td>${getStatusHTML(h.status, isPending)}</td>
                                ${canEdit ? `<td>${actionsHTML}</td>` : ''}
                            </tr>
                        `;
                    });
                }

                let bizRows = '';
                if (activeScan && activeScan.businesses) {
                    activeScan.businesses.forEach(b => {
                        const isPending = b.isPendingPair || b.status === 'pending';

                        let actionsHTML = '';
                        if (canEdit) {
                            actionsHTML = `
                                <div class="btn-circle-group">
                                    <button class="btn-circle ${b.status === 'insured' && !isPending ? 'active-insured' : ''}" title="Страхованный" onclick="updateItemStatus('${srv}', '${activeScan.scanId}', 'biz', ${b.pos}, 'insured')">🛡️</button>
                                    <button class="btn-circle ${b.status === 'uninsured' && !isPending ? 'active-uninsured' : ''}" title="Не страхованный" onclick="updateItemStatus('${srv}', '${activeScan.scanId}', 'biz', ${b.pos}, 'uninsured')">❌</button>
                                    <button class="btn-circle ${b.status === 'no_activity' && !isPending ? 'active-noact' : ''}" title="Без активности" onclick="updateItemStatus('${srv}', '${activeScan.scanId}', 'biz', ${b.pos}, 'no_activity')">🚫</button>
                                    <button class="btn-circle ${b.status === 'frozen' && !isPending ? 'active-frozen' : ''}" title="Заморожен" onclick="updateItemStatus('${srv}', '${activeScan.scanId}', 'biz', ${b.pos}, 'frozen')">❄️️</button>
                                    <button class="btn-circle btn-circle-del" title="Удалить" onclick="deleteItem('${srv}', '${activeScan.scanId}', 'biz', ${b.pos})">🗑️</button>
                                </div>
                            `;
                        }

                        const displayPd = b.basePd !== undefined ? b.basePd : b.pd;

                        bizRows += `
                            <tr>
                                <td>${b.propId ? `№${b.propId}` : `поз. ${b.pos}`}</td>
                                <td><span class="pd-badge">${displayPd} PD</span></td>
                                <td>${getStatusHTML(b.status, isPending)}</td>
                                ${canEdit ? `<td>${actionsHTML}</td>` : ''}
                            </tr>
                        `;
                    });
                }

                const addHouseBtn = (canEdit && activeScan) ? `<button class="btn-add" onclick="addItem('${srv}', '${activeScan.scanId}', 'house')">+ Добавить</button>` : '';
                const addBizBtn = (canEdit && activeScan) ? `<button class="btn-add" onclick="addItem('${srv}', '${activeScan.scanId}', 'biz')">+ Добавить</button>` : '';

                const headerHTML = `
                    <div class="server-header">
                        <div class="server-title">
                            <span>${getServerDisplayName(srv)}</span>
                            <span class="season-badge">${sData.season?.display || ''}</span>
                        </div>
                    </div>
                `;

                const tablesHTML = `
                    <div class="tables-grid">
                        <div>
                            <div class="section-header">
                                <span class="section-title">🏠 Дома</span>
                                ${addHouseBtn}
                            </div>
                            ${housesRows ? `<table><thead><tr><th>Объект</th><th>PD</th><th>Статус</th>${canEdit ? '<th>Действия</th>' : ''}</tr></thead><tbody>${housesRows}</tbody></table>` : '<span class="empty">Нет данных</span>'}
                        </div>
                        <div>
                            <div class="section-header">
                                <span class="section-title">🏢 Бизнесы</span>
                                ${addBizBtn}
                            </div>
                            ${bizRows ? `<table><thead><tr><th>Объект</th><th>PD</th><th>Статус</th>${canEdit ? '<th>Действия</th>' : ''}</tr></thead><tbody>${bizRows}</tbody></table>` : '<span class="empty">Нет данных</span>'}
                        </div>
                    </div>
                `;

                card.innerHTML = headerHTML + scanBarHTML + tablesHTML;
                container.appendChild(card);
            });
        }

        function setActiveScan(srv, scanId) {
            activeServerScans[srv] = scanId;
            renderManageTab();
        }

        async function updateItemStatus(srv, scanId, propType, pos, newStatus) {
            try {
                const res = await fetch('/api/update_status', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server: srv, scanId: scanId, propType: propType, pos: pos, status: newStatus })
                });
                if (res.ok) {
                    await loadPaydaysData();
                } else {
                    const err = await res.json();
                    alert(err.detail || 'Ошибка изменения статуса');
                }
            } catch (e) {
                alert('Ошибка соединения с сервером');
            }
        }

        async function deleteItem(srv, scanId, propType, pos) {
            if (!confirm('Вы уверены, что хотите удалить этот объект?')) return;
            try {
                const res = await fetch('/api/delete_item', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server: srv, scanId: scanId, propType: propType, pos: pos })
                });
                if (res.ok) {
                    await loadPaydaysData();
                } else {
                    alert('Ошибка удаления объекта');
                }
            } catch (e) {
                alert('Ошибка соединения с сервером');
            }
        }

        async function handleDeleteScan(srv, scanId) {
            if (!confirm(`Удалить весь скан ${scanId}?`)) return;
            try {
                const res = await fetch('/api/delete_scan', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server: srv, scanId: scanId })
                });
                if (res.ok) {
                    delete activeServerScans[srv];
                    await loadPaydaysData();
                } else {
                    alert('Ошибка удаления скана');
                }
            } catch (e) {
                alert('Ошибка соединения с сервером');
            }
        }

        async function addItem(srv, scanId, propType) {
            const propIdStr = prompt('Введите номер дома/бизнеса (или оставьте пустым):');
            const pdStr = prompt('Введите количество PayDay:');
            if (pdStr === null) return;
            const pd = parseInt(pdStr);
            if (isNaN(pd)) return alert('Некорректный PayDay');

            const propId = propIdStr && !isNaN(parseInt(propIdStr)) ? parseInt(propIdStr) : null;

            try {
                const res = await fetch('/api/add_item', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server: srv, scanId: scanId, propType: propType, pd: pd, propId: propId })
                });
                if (res.ok) {
                    await loadPaydaysData();
                } else {
                    alert('Ошибка добавления объекта');
                }
            } catch (e) {
                alert('Ошибка соединения с сервером');
            }
        }

        async function renderAdminTab() {
            if (!currentUser || currentUser.role !== 'admin') return;
            await loadAdminUsers();
            await loadAdminScanLogs();
        }

        async function loadManualLogs() {
            try {
                const res = await fetch('/api/admin/manual_action_logs');
                if (!res.ok) return;
                const logs = await res.json();
                const container = document.getElementById('admin-manual-logs-table');
                if (!container) return;

                if (logs.length === 0) {
                    container.innerHTML = '<span class="empty">Логи действий отсутствуют</span>';
                    return;
                }

                let rows = '';
                logs.forEach(l => {
                    rows += `
                        <tr>
                            <td>${l.created_at}</td>
                            <td><b>${l.username}</b></td>
                            <td>${getServerDisplayName(l.server)}</td>
                            <td>${l.details}</td>
                        </tr>
                    `;
                });

                container.innerHTML = `
                    <table>
                        <thead>
                            <tr><th>Время</th><th>Пользователь</th><th>Сервер</th><th>Детали действия</th></tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                `;
            } catch (e) {
                console.error("Ошибка загрузки логов ручных действий:", e);
            }
        }

        async function loadAdminUsers() {
            try {
                const res = await fetch('/api/admin/users');
                if (!res.ok) return;
                const users = await res.json();
                const container = document.getElementById('admin-users-table');

                let rows = '';
                users.forEach(u => {
                    rows += `
                        <tr>
                            <td>${u.id}</td>
                            <td><b>${u.username}</b></td>
                            <td>
                                <select class="select-role" onchange="changeUserRole(${u.id}, this.value)">
                                    <option value="user" ${u.role === 'user' ? 'selected' : ''}>User</option>
                                    <option value="support" ${u.role === 'support' ? 'selected' : ''}>Support</option>
                                    <option value="admin" ${u.role === 'admin' ? 'selected' : ''}>Admin</option>
                                </select>
                            </td>
                            <td>
                                <input type="checkbox" ${u.is_allowed ? 'checked' : ''} onchange="toggleUserAccess(${u.id}, this.checked)"> Доступ
                            </td>
                            <td>
                                <button class="btn-add" onclick="promptChangePassword(${u.id})">Сменить пароль</button>
                                <button class="btn-user-del" onclick="deleteUser(${u.id})">Удалить</button>
                            </td>
                        </tr>
                    `;
                });

                container.innerHTML = `
                    <table>
                        <thead>
                            <tr><th>ID</th><th>Логин</th><th>Роль</th><th>Статус</th><th>Действия</th></tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                `;
            } catch (e) {
                console.error("Ошибка загрузки пользователей:", e);
            }
        }

        async function loadAdminScanLogs() {
            try {
                const res = await fetch('/api/admin/scan_logs');
                if (!res.ok) return;
                const logs = await res.json();
                const container = document.getElementById('admin-scan-logs-table');

                let rows = '';
                logs.forEach(l => {
                    rows += `
                        <tr>
                            <td>${l.created_at}</td>
                            <td>${getServerDisplayName(l.server)}</td>
                            <td>${l.scanner}</td>
                        </tr>
                    `;
                });

                container.innerHTML = `
                    <div style="max-height: 250px; overflow-y: auto;">
                        <table>
                            <thead>
                                <tr><th>Время</th><th>Сервер</th><th>Скрипт / Сканер</th></tr>
                            </thead>
                            <tbody>${rows}</tbody>
                        </table>
                    </div>
                `;
            } catch (e) {
                console.error("Ошибка загрузки логов сканирования:", e);
            }
        }

        async function handleCreateUser() {
            const u = document.getElementById('new-username').value.trim();
            const p = document.getElementById('new-password').value.trim();
            const r = document.getElementById('new-role').value;

            if (!u || !p) return alert('Заполните логин и пароль');

            try {
                const res = await fetch('/api/admin/create_user', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username: u, password: p, role: r })
                });
                if (res.ok) {
                    document.getElementById('new-username').value = '';
                    document.getElementById('new-password').value = '';
                    loadAdminUsers();
                } else {
                    const err = await res.json();
                    alert(err.detail || 'Ошибка создания пользователя');
                }
            } catch (e) {
                alert('Ошибка соединения');
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

        async function changeUserRole(userId, newRole) {
            const res = await fetch('/api/admin/update_role', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, role: newRole })
            });
            if (!res.ok) {
                const err = await res.json();
                alert(err.detail || 'Ошибка смены роли');
            }
            loadAdminUsers();
        }

        async function promptChangePassword(userId) {
            const newPassword = prompt('Введите новый пароль:');
            if (!newPassword) return;

            const res = await fetch('/api/admin/change_password', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, new_password: newPassword })
            });
            if (res.ok) {
                alert('Пароль успешно изменен');
            } else {
                const err = await res.json();
                alert(err.detail || 'Ошибка изменения пароля');
            }
        }

        async function deleteUser(userId) {
            if (!confirm('Вы уверены, что хотите удалить пользователя?')) return;
            const res = await fetch('/api/admin/delete_user', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId })
            });
            if (res.ok) {
                loadAdminUsers();
            } else {
                const err = await res.json();
                alert(err.detail || 'Ошибка удаления пользователя');
            }
        }

        checkAuth();
    </script>
</body>
</html>
"""
