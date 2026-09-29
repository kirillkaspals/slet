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
    1: "По инфе",
    2: "Скорострелы",
    3: "Автогонки",
    4: "По новому",
    5: "Мотогонки"
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
    if status in ["uninsured", "frozen"]:
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

    now_utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    async with db_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT username, expires_at FROM user_sessions WHERE token = $1", token
        )
        if session:
            if session["expires_at"] > now_utc_naive:
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
def load_data_from_file() -> Dict[str, dict]:
    data_store = {srv: {"scans": []} for srv in ALL_SERVERS}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for srv in ALL_SERVERS:
                    if srv in loaded and "scans" in loaded[srv]:
                        data_store[srv] = loaded[srv]
            print("Данные успешно загружены из файла.")
        except Exception as e:
            print(f"Ошибка чтения JSON-файла: {e}")
    return data_store

server_data = load_data_from_file()

async def save_data_to_file_async():
    def _save():
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(server_data, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"Ошибка сохранения данных: {e}")

    await asyncio.to_thread(_save)

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
    """Возвращает последний подтверждённый и активный скан пары"""
    confirmed_scans = [s for s in scans if s.get("isConfirmed", False) and s.get("isActive", True)]
    if confirmed_scans:
        return confirmed_scans[-1]
    return None

# --- АЛГОРИТМ УМНОГО СОПОСТАВЛЕНИЯ ---
def find_pairs_with_offset(prev_items: List[dict], curr_entries: List[PropertyEntry], prop_type: str):
    matches = {}
    used_prev_indices = set()

    def determine_status(diff: int, p_type: str) -> str:
        if diff <= 0:
            return "frozen" if diff == 0 else "insured"
        if p_type == "house":
            return "uninsured" if diff >= 2 else "insured"
        else:
            if diff >= 4:
                return "no_activity"
            elif diff >= 2:
                return "uninsured"
            return "insured"

    # 1. Точное сопоставление по propId
    for c_idx, curr in enumerate(curr_entries):
        curr_prop_id = getattr(curr, 'propId', None)
        if curr_prop_id is not None:
            for p_idx, prev in enumerate(prev_items):
                if p_idx in used_prev_indices:
                    continue
                if prev.get("propId") == curr_prop_id:
                    base_pd = prev.get("basePd", prev.get("pd", 0))
                    diff = base_pd - curr.pd
                    auto_status = determine_status(diff, prop_type)
                    matches[c_idx] = (prev, auto_status)
                    used_prev_indices.add(p_idx)
                    break

    # 2. Сопоставление по позиции в списке pos
    for c_idx, curr in enumerate(curr_entries):
        if c_idx in matches:
            continue

        for p_idx, prev in enumerate(prev_items):
            if p_idx in used_prev_indices:
                continue

            if prev.get("pos") == curr.pos:
                base_pd = prev.get("basePd", prev.get("pd", 0))
                diff = base_pd - curr.pd
                auto_status = determine_status(diff, prop_type)
                matches[c_idx] = (prev, auto_status)
                used_prev_indices.add(p_idx)
                break

    # 3. Резервный проход по допустимой дельте
    for c_idx, curr in enumerate(curr_entries):
        if c_idx in matches:
            continue

        valid_diffs = {0, 1, 2} if prop_type == "house" else {0, 1, 2, 4}
        for p_idx, prev in enumerate(prev_items):
            if p_idx in used_prev_indices:
                continue

            base_pd = prev.get("basePd", prev.get("pd", 0))
            diff = base_pd - curr.pd

            if diff in valid_diffs or diff < 0:
                auto_status = determine_status(diff, prop_type)
                matches[c_idx] = (prev, auto_status)
                used_prev_indices.add(p_idx)
                break

    return matches

# --- ЛОГИКА PAYDAY И ПРОВЕРКИ АКТИВНОСТИ СКАНОВ ---
async def process_hourly_payday():
    now_msk = datetime.now(MSK_TZ)
    is_restart_hour = (now_msk.hour == 5)

    if now_msk.hour == 0 and db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("TRUNCATE TABLE scan_logs;")
                print(f"[{now_msk.strftime('%Y-%m-%d %H:%M:%S')}] Логи сканов успешно очищены (00:00 МСК).")
        except Exception as e:
            print(f"Ошибка очистки логов сканирования: {e}")

    async with data_lock:
        print(f"[{now_msk.strftime('%Y-%m-%d %H:%M:%S')}] Выполнение списания PayDay и проверка статусов сканов...")
        for srv, data in server_data.items():
            scans = data.get("scans", [])
            if not scans:
                continue

            for s in scans:
                scan_dt = datetime.strptime(s["scanTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)
                time_diff = (now_msk - scan_dt).total_seconds() / 3600.0

                if not s.get("isPaired", False) and time_diff >= 1.5:
                    s["isActive"] = False

            latest_scan = get_latest_confirmed_scan(scans)
            if not latest_scan or not latest_scan.get("isConfirmed", False):
                continue

            updated_houses = []
            for h in latest_scan.get("houses", []):
                if h.get("isPendingPair", False):
                    updated_houses.append(h)
                    continue

                st = h.get("status", "insured")
                if st != "frozen" and not is_restart_hour:
                    decrement = 1 if st == "insured" else 2
                    h["pd"] -= decrement
                
                drop_limit = get_drop_limit(srv, "house", st)
                if is_restart_hour or h["pd"] >= drop_limit:
                    updated_houses.append(h)

            latest_scan["houses"] = sorted(updated_houses, key=lambda x: x["pos"])

            updated_biz = []
            for b in latest_scan.get("businesses", []):
                if b.get("isPendingPair", False):
                    updated_biz.append(b)
                    continue

                st = b.get("status", "insured")
                if st != "frozen" and not is_restart_hour:
                    decrement = 1 if st == "insured" else (2 if st == "uninsured" else 4)
                    b["pd"] -= decrement
                
                drop_limit = get_drop_limit(srv, "biz", st)
                if is_restart_hour or b["pd"] >= drop_limit:
                    updated_biz.append(b)

            latest_scan["businesses"] = sorted(updated_biz, key=lambda x: x["pos"])

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
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        await init_db()
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
    user = await get_user_by_username(data.username)
    if not user or not pwd_context.verify(data.password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="Неверный логин или пароль")
    
    if not user["is_allowed"]:
        raise HTTPException(status_code=403, detail="Ваш доступ к сайту заблокирован")

    token = os.urandom(24).hex()
    active_sessions[token] = user["username"]

    expires_at_dt = datetime.now(timezone.utc) + timedelta(days=SESSION_EXPIRE_DAYS)
    expires_at_db = expires_at_dt.replace(tzinfo=None)

    if db_pool:
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
        expires=expires_at_dt,
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

@app.post("/api/admin/toggle_access")
async def toggle_access(data: ToggleAccessModel, username: str = Depends(verify_admin)):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET is_allowed = $1 WHERE id = $2", data.is_allowed, data.user_id)
    return {"status": "success"}

@app.post("/api/admin/create_user")
async def create_user(data: CreateUserModel, username: str = Depends(verify_admin)):
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
    if not data.new_password or len(data.new_password.strip()) < 4:
        raise HTTPException(status_code=400, detail="Пароль слишком короткий (минимум 4 символа)")

    hashed_pw = pwd_context.hash(data.new_password)
    
    async with db_pool.acquire() as conn:
        target = await conn.fetchrow("SELECT username FROM users WHERE id = $1", data.user_id)
        if not target:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        
        await conn.execute("UPDATE users SET password_hash = $1 WHERE id = $2", hashed_pw, data.user_id)

    return {"status": "success"}

# --- ДАШБОРД (HTML / CSS / JS) ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Arizona RP — Мониторинг Слётов</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-main: #0b0f17;
            --bg-card: #151c28;
            --bg-card-hover: #1c2536;
            --bg-input: #1e293b;
            --border-color: #232d3f;
            --accent-primary: #6366f1;
            --accent-primary-hover: #4f46e5;
            --accent-glow: rgba(99, 102, 241, 0.25);
            --accent-danger: #ef4444;
            --accent-warning: #f59e0b;
            --accent-success: #10b981;
            --accent-info: #06b6d4;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --text-dim: #64748b;
            --radius-sm: 8px;
            --radius-md: 12px;
            --radius-lg: 16px;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            -webkit-tap-highlight-color: transparent;
        }

        body {
            background-color: var(--bg-main);
            color: var(--text-main);
            padding: 12px;
            min-height: 100vh;
        }

        @media (min-width: 768px) {
            body { padding: 24px; }
        }

        /* --- Header & Title --- */
        .app-header {
            text-align: center;
            margin-bottom: 20px;
        }

        h1 {
            font-size: 1.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, #a5b4fc 0%, #6366f1 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: -0.02em;
        }

        @media (min-width: 768px) {
            h1 { font-size: 2rem; margin-bottom: 8px; }
        }

        /* --- Login Box --- */
        .login-box {
            max-width: 380px;
            margin: 60px auto;
            background: var(--bg-card);
            padding: 28px;
            border-radius: var(--radius-lg);
            border: 1px solid var(--border-color);
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5), 0 8px 10px -6px rgba(0, 0, 0, 0.5);
            text-align: center;
        }

        .login-box h2 {
            margin-bottom: 20px;
            font-size: 1.25rem;
            color: var(--text-main);
        }

        .login-box input {
            width: 100%;
            padding: 12px 14px;
            margin-bottom: 14px;
            background: var(--bg-input);
            border: 1px solid var(--border-color);
            color: var(--text-main);
            border-radius: var(--radius-sm);
            font-size: 0.95rem;
            outline: none;
            transition: border-color 0.2s;
        }

        .login-box input:focus {
            border-color: var(--accent-primary);
        }

        .login-box button {
            width: 100%;
            padding: 12px;
            background: linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-primary-hover) 100%);
            border: none;
            font-weight: 600;
            cursor: pointer;
            border-radius: var(--radius-sm);
            color: #fff;
            font-size: 0.95rem;
            box-shadow: 0 4px 12px var(--accent-glow);
            transition: opacity 0.2s;
        }

        .login-box button:active { opacity: 0.85; }

        /* --- User Navigation & Info --- */
        .user-nav {
            display: flex;
            justify-content: space-between;
            align-items: center;
            max-width: 1200px;
            margin: 0 auto 16px auto;
            background: var(--bg-card);
            padding: 10px 16px;
            border-radius: var(--radius-md);
            border: 1px solid var(--border-color);
            font-size: 0.85rem;
        }

        .btn-logout {
            background: rgba(239, 68, 68, 0.15);
            color: #fca5a5;
            border: 1px solid rgba(239, 68, 68, 0.3);
            padding: 6px 14px;
            border-radius: var(--radius-sm);
            cursor: pointer;
            font-weight: 500;
            font-size: 0.8rem;
            transition: all 0.2s;
        }

        .btn-logout:hover {
            background: rgba(239, 68, 68, 0.25);
            color: #fff;
        }

        /* --- Tabs --- */
        .tabs {
            display: flex;
            justify-content: flex-start;
            gap: 8px;
            margin-bottom: 20px;
            max-width: 1200px;
            margin-left: auto;
            margin-right: auto;
            overflow-x: auto;
            padding-bottom: 4px;
            -webkit-overflow-scrolling: touch;
        }

        .tab-btn {
            background-color: var(--bg-card);
            color: var(--text-muted);
            border: 1px solid var(--border-color);
            padding: 10px 18px;
            font-size: 0.85rem;
            font-weight: 600;
            border-radius: var(--radius-md);
            cursor: pointer;
            white-space: nowrap;
            transition: all 0.2s;
            flex-shrink: 0;
        }

        .tab-btn.active {
            background: var(--accent-primary);
            color: #ffffff;
            border-color: var(--accent-primary);
            box-shadow: 0 4px 14px var(--accent-glow);
        }

        .tab-content { display: none; }
        .tab-content.active { display: block; }

        /* --- Upcoming Filters --- */
        .upcoming-filters {
            display: flex;
            justify-content: center;
            gap: 8px;
            margin-bottom: 20px;
        }

        .time-filter-btn {
            background-color: var(--bg-card);
            color: var(--text-muted);
            border: 1px solid var(--border-color);
            padding: 8px 16px;
            font-size: 0.85rem;
            font-weight: 600;
            border-radius: 20px;
            cursor: pointer;
            transition: all 0.2s;
        }

        .time-filter-btn.active {
            background-color: var(--accent-danger);
            color: #ffffff;
            border-color: var(--accent-danger);
            box-shadow: 0 0 12px rgba(239, 68, 68, 0.4);
        }

        /* --- Filter Panel --- */
        .filter-panel {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-md);
            padding: 12px 16px;
            margin: 0 auto 20px auto;
            max-width: 1200px;
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            align-items: center;
        }

        .filter-group {
            display: flex;
            flex-direction: column;
            gap: 4px;
            flex: 1 1 140px;
        }

        .filter-group label {
            font-size: 0.75rem;
            color: var(--text-muted);
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .filter-select {
            background: var(--bg-input);
            color: var(--text-main);
            border: 1px solid var(--border-color);
            padding: 8px 12px;
            border-radius: var(--radius-sm);
            font-size: 0.85rem;
            outline: none;
            width: 100%;
        }

        .fav-btn {
            cursor: pointer;
            font-size: 1.1rem;
            user-select: none;
            margin-right: 6px;
            transition: transform 0.15s ease;
            display: inline-block;
        }

        .fav-btn:hover { transform: scale(1.2); }

        /* --- Servers Grid & Cards --- */
        .servers-container {
            display: flex;
            flex-direction: column;
            gap: 16px;
            max-width: 1200px;
            margin: 0 auto;
        }

        .server-card {
            background-color: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-md);
            padding: 16px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
            transition: border-color 0.2s;
        }

        .server-card:hover {
            border-color: #334155;
        }

        .server-header {
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 10px;
            margin-bottom: 14px;
        }

        .server-title {
            font-size: 1.1rem;
            font-weight: 700;
            color: var(--text-main);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 8px;
        }

        .season-badge {
            font-size: 0.75rem;
            background: rgba(245, 158, 11, 0.1);
            color: var(--accent-warning);
            border: 1px solid rgba(245, 158, 11, 0.25);
            padding: 3px 10px;
            border-radius: 12px;
            font-weight: 500;
        }

        /* Scan Subtabs Bar */
        .scan-tabs-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 14px;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 8px;
            gap: 10px;
        }

        .scan-tabs-container {
            display: flex;
            gap: 6px;
            overflow-x: auto;
            padding-bottom: 4px;
            -webkit-overflow-scrolling: touch;
        }

        .scan-subtab {
            background-color: var(--bg-input);
            color: var(--text-muted);
            border: 1px solid var(--border-color);
            padding: 5px 12px;
            font-size: 0.8rem;
            border-radius: var(--radius-sm);
            cursor: pointer;
            white-space: nowrap;
            transition: all 0.2s;
        }

        .scan-subtab.active {
            background-color: var(--accent-primary);
            color: #fff;
            border-color: var(--accent-primary);
            font-weight: 600;
        }

        .scan-subtab.single { border-color: rgba(245, 158, 11, 0.5); color: var(--accent-warning); }
        .scan-subtab.inactive { opacity: 0.4; }

        .btn-delete-scan {
            background-color: rgba(239, 68, 68, 0.15);
            color: #fca5a5;
            border: 1px solid rgba(239, 68, 68, 0.3);
            padding: 4px 10px;
            border-radius: var(--radius-sm);
            font-size: 0.75rem;
            font-weight: 600;
            cursor: pointer;
            white-space: nowrap;
        }

        .btn-delete-scan:hover { background-color: var(--accent-danger); color: #fff; }

        /* Tables & Layout */
        .tables-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 16px;
        }

        @media (min-width: 900px) {
            .tables-grid { grid-template-columns: 1fr 1fr; gap: 20px; }
        }

        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }

        .section-title {
            font-size: 0.85rem;
            font-weight: 700;
            color: var(--accent-info);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .btn-add {
            background-color: rgba(99, 102, 241, 0.15);
            color: #a5b4fc;
            border: 1px solid rgba(99, 102, 241, 0.3);
            border-radius: var(--radius-sm);
            padding: 3px 10px;
            font-size: 0.75rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }

        .btn-add:hover { background-color: var(--accent-primary); color: #fff; }

        .table-responsive {
            width: 100%;
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
            border-radius: var(--radius-sm);
            border: 1px solid var(--border-color);
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.8rem;
            text-align: left;
            background-color: rgba(15, 23, 42, 0.4);
        }

        th, td {
            padding: 8px 10px;
            border-bottom: 1px solid var(--border-color);
            vertical-align: middle;
        }

        th {
            background-color: var(--bg-input);
            color: var(--text-muted);
            font-weight: 600;
            font-size: 0.75rem;
            text-transform: uppercase;
        }

        tr:last-child td { border-bottom: none; }

        /* Badges */
        .pd-badge {
            background-color: rgba(239, 68, 68, 0.2);
            color: #fca5a5;
            border: 1px solid rgba(239, 68, 68, 0.3);
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: 700;
        }

        .pd-badge-drop {
            background-color: var(--accent-danger);
            color: #fff;
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: 700;
            animation: pulse 1.5s infinite;
        }

        .pd-badge-fixed {
            background-color: rgba(100, 116, 139, 0.2);
            color: #cbd5e1;
            border: 1px solid var(--border-color);
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: 700;
        }

        .time-left-badge {
            font-weight: 600;
            color: var(--accent-warning);
            font-size: 0.8rem;
            background-color: rgba(245, 158, 11, 0.1);
            padding: 3px 8px;
            border-radius: 6px;
            border: 1px solid rgba(245, 158, 11, 0.25);
            display: inline-block;
            white-space: nowrap;
        }

        .time-left-frozen {
            font-weight: 600;
            color: var(--accent-info);
            font-size: 0.8rem;
            white-space: nowrap;
        }

        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.6; }
            100% { opacity: 1; }
        }

        /* Controls in tables */
        .btn-group {
            display: flex;
            gap: 4px;
            flex-wrap: wrap;
        }

        .btn-opt {
            background-color: var(--bg-input);
            color: var(--text-muted);
            border: 1px solid var(--border-color);
            padding: 4px 8px;
            font-size: 0.75rem;
            border-radius: 4px;
            cursor: pointer;
            transition: all 0.15s;
        }

        .btn-opt.active-insured { background-color: var(--accent-success); color: #fff; border-color: var(--accent-success); }
        .btn-opt.active-uninsured { background-color: var(--accent-danger); color: #fff; border-color: var(--accent-danger); }
        .btn-opt.active-noact { background-color: #b91c1c; color: #fff; border-color: #b91c1c; font-weight: 700; }
        .btn-opt.active-frozen { background-color: #0284c7; color: #fff; border-color: #0284c7; font-weight: 700; }

        .status-text { font-weight: 600; font-size: 0.8rem; padding: 2px 6px; border-radius: 4px; display: inline-block; }
        .status-insured { color: var(--accent-success); }
        .status-uninsured { color: #f87171; }
        .status-noact { color: #ef4444; }
        .status-frozen { color: #38bdf8; }
        .status-pending { color: var(--accent-warning); font-style: italic; }

        .btn-del {
            background-color: transparent;
            color: #f87171;
            border: 1px solid rgba(239, 68, 68, 0.4);
            padding: 2px 6px;
            font-size: 0.75rem;
            border-radius: 4px;
            cursor: pointer;
        }

        .btn-del:hover { background-color: var(--accent-danger); color: #fff; }

        .btn-user-del {
            background-color: rgba(239, 68, 68, 0.2);
            color: #fca5a5;
            border: 1px solid rgba(239, 68, 68, 0.4);
            padding: 4px 8px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.75rem;
        }

        .select-role {
            background: var(--bg-input);
            color: var(--text-main);
            border: 1px solid var(--border-color);
            padding: 4px;
            border-radius: 4px;
            font-size: 0.8rem;
        }

        .empty { color: var(--text-dim); font-style: italic; font-size: 0.8rem; }
        .empty-center {
            text-align: center;
            color: var(--text-muted);
            font-style: italic;
            padding: 24px;
            background-color: var(--bg-card);
            border-radius: var(--radius-md);
            border: 1px solid var(--border-color);
            max-width: 1200px;
            margin: 0 auto;
        }
    </style>
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

        function getPropRules(info, type) {
            const defaultRules = { insured: 2, uninsured_min: 2, uninsured_max: 3 };
            if (!info || !info.dropRules) return defaultRules;
            return info.dropRules[type] || defaultRules;
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
            const res = await fetch('/api/auth/me');
            const data = await res.json();
            if (data.authenticated) {
                currentUser = data;
                document.getElementById('login-screen').style.display = 'none';
                document.getElementById('main-dashboard').style.display = 'block';
                document.getElementById('user-info').innerText = `${data.username} (${data.role})`;
                
                const canManage = data.role === 'admin' || data.role === 'support';
                const isAdmin = data.role === 'admin';

                document.getElementById('btn-tab-manage').style.display = canManage ? 'inline-block' : 'none';
                document.getElementById('btn-tab-admin').style.display = isAdmin ? 'inline-block' : 'none';
                
                initDashboard();
            } else {
                document.getElementById('login-screen').style.display = 'block';
                document.getElementById('main-dashboard').style.display = 'none';
            }
        }

        async function handleLogin() {
            const username = document.getElementById('login-username').value;
            const password = document.getElementById('login-password').value;
            const res = await fetch('/api/auth/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password })
            });
            if (res.ok) {
                checkAuth();
            } else {
                const err = await res.json();
                alert(err.detail || 'Ошибка авторизации');
            }
        }

        async function handleLogout() {
            await fetch('/api/auth/logout', { method: 'POST' });
            location.reload();
        }

        function switchTab(tabName) {
            const canManage = currentUser && (currentUser.role === 'admin' || currentUser.role === 'support');
            const isAdmin = currentUser && currentUser.role === 'admin';

            if (tabName === 'manage' && !canManage) {
                alert('Недостаточно прав доступа');
                return;
            }
            if (tabName === 'admin' && !isAdmin) {
                alert('Недостаточно прав доступа');
                return;
            }

            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            
            if (tabName === 'manage') {
                document.getElementById('btn-tab-manage').classList.add('active');
                document.getElementById('tab-manage').classList.add('active');
            } else if (tabName === 'upcoming') {
                document.getElementById('btn-tab-upcoming').classList.add('active');
                document.getElementById('tab-upcoming').classList.add('active');
            } else if (tabName === 'admin') {
                document.getElementById('btn-tab-admin').classList.add('active');
                document.getElementById('tab-admin').classList.add('active');
                loadAdminUsers();
                loadScanLogs();
            } else {
                document.getElementById('btn-tab-view').classList.add('active');
                document.getElementById('tab-view').classList.add('active');
            }
        }

        function setUpcomingHoursFilter(hours) {
            selectedUpcomingHours = hours;
            document.querySelectorAll('.time-filter-btn').forEach(btn => btn.classList.remove('active'));
            document.getElementById(`btn-upcoming-${hours}h`).classList.add('active');
            loadData();
        }

        async function handleCreateUser() {
            const username = document.getElementById('new-username').value;
            const password = document.getElementById('new-password').value;
            const role = document.getElementById('new-role').value;

            if (!username || !password) {
                alert('Заполните логин и пароль!');
                return;
            }

            const res = await fetch('/api/admin/create_user', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password, role })
            });

            if (res.ok) {
                alert('Пользователь успешно создан');
                document.getElementById('new-username').value = '';
                document.getElementById('new-password').value = '';
                loadAdminUsers();
            } else {
                const err = await res.json();
                alert(err.detail || 'Ошибка создания пользователя');
            }
        }

        async function loadAdminUsers() {
            const res = await fetch('/api/admin/users');
            if (!res.ok) return;
            const users = await res.json();
            
            let html = `<div class="table-responsive"><table><tr><th>ID</th><th>Логин</th><th>Роль</th><th>Доступ</th><th>Действия</th></tr>`;
            users.forEach(u => {
                const isSelf = u.username === currentUser.username;
                
                const toggleAccessBtn = !isSelf ? 
                    `<button class="btn-add" onclick="toggleUserAccess(${u.id}, ${u.is_allowed})">${u.is_allowed ? 'Заблокировать' : 'Разблокировать'}</button>` : '—';
                
                const deleteUserBtn = !isSelf ? 
                    `<button class="btn-user-del" onclick="deleteUser(${u.id}, '${u.username}')">Удалить</button>` : '';

                const changePwBtn = `<button class="btn-add" style="background-color: var(--accent-warning); color: #000;" onclick="changeUserPassword(${u.id}, '${u.username}')">🔑 Пароль</button>`;

                const roleSelect = !isSelf ? `
                    <select class="select-role" onchange="changeUserRole(${u.id}, this.value)">
                        <option value="user" ${u.role === 'user' ? 'selected' : ''}>User</option>
                        <option value="support" ${u.role === 'support' ? 'selected' : ''}>Support</option>
                        <option value="admin" ${u.role === 'admin' ? 'selected' : ''}>Admin</option>
                    </select>
                ` : `<b>${u.role}</b>`;

                html += `<tr>
                    <td>${u.id}</td>
                    <td>${u.username}</td>
                    <td>${roleSelect}</td>
                    <td>${u.is_allowed ? '✅ Разрешен' : '❌ Заблокирован'}</td>
                    <td>
                        <div style="display: flex; gap: 4px; align-items: center; flex-wrap: wrap;">
                            ${changePwBtn}
                            ${toggleAccessBtn} 
                            ${deleteUserBtn}
                        </div>
                    </td>
                </tr>`;
            });
            document.getElementById('admin-users-table').innerHTML = html + '</table></div>';
        }

        async function loadScanLogs() {
            const res = await fetch('/api/admin/scan_logs');
            if (!res.ok) return;
            const logs = await res.json();
            
            if (logs.length === 0) {
                document.getElementById('admin-scan-logs-table').innerHTML = '<span class="empty">За сегодня сканов еще не зафиксировано</span>';
                return;
            }

            let html = `<div class="table-responsive"><table><tr><th>Время (МСК)</th><th>Сервер</th><th>Отправитель</th></tr>`;
            logs.forEach(l => {
                html += `<tr>
                    <td><b>${l.created_at}</b></td>
                    <td><span style="color: var(--accent-info);">${l.server}</span></td>
                    <td><span style="color: var(--accent-warning);">${l.scanner}</span></td>
                </tr>`;
            });
            document.getElementById('admin-scan-logs-table').innerHTML = html + '</table></div>';
        }

        async function changeUserPassword(userId, username) {
            const newPassword = prompt(`Введите новый пароль для пользователя ${username}:`);
            if (!newPassword) return;
            if (newPassword.trim().length < 4) {
                alert('Пароль слишком короткий (минимум 4 символа)');
                return;
            }

            const res = await fetch('/api/admin/change_password', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, new_password: newPassword })
            });

            if (res.ok) {
                alert(`Пароль для ${username} успешно изменен!`);
            } else {
                const err = await res.json();
                alert(err.detail || 'Ошибка смены пароля');
            }
        }

        async function toggleUserAccess(userId, currentStatus) {
            await fetch('/api/admin/toggle_access', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, is_allowed: !currentStatus })
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
                alert(err.detail || 'Ошибка обновления роли');
            }
            loadAdminUsers();
        }

        async function deleteUser(userId, username) {
            if (!confirm(`Вы действительно хотите безвозвратно удалить аккаунт ${username}?`)) return;
            
            const res = await fetch('/api/admin/delete_user', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId })
            });

            if (res.ok) {
                alert('Пользователь успешно удален');
                loadAdminUsers();
            } else {
                const err = await res.json();
                alert(err.detail || 'Ошибка удаления');
            }
        }

        function selectScanTab(server, scanId) {
            activeServerScans[server] = scanId;
            loadData();
        }

        async function setStatus(server, scanId, propType, pos, status) {
            try {
                await fetch('/api/update_status', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server, scanId, propType, pos, status })
                });
                loadData();
            } catch(e) { console.error(e); }
        }

        async function deleteItem(server, scanId, propType, pos) {
            if (!confirm('Удалить эту запись?')) return;
            try {
                await fetch('/api/delete_item', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server, scanId, propType, pos })
                });
                loadData();
            } catch(e) { console.error(e); }
        }

        async function deleteWholeScan(server) {
            const scanId = activeServerScans[server];
            if (!scanId) return;
            if (!confirm(`Удалить весь скан ${scanId} для сервера ${server}?`)) return;

            try {
                await fetch('/api/delete_scan', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server, scanId })
                });
                delete activeServerScans[server];
                loadData();
            } catch(e) { console.error(e); }
        }

        async function addItemPrompt(server, propType) {
            const scanId = activeServerScans[server];
            if (!scanId) {
                alert('Сначала выберите или дождитесь сканирования!');
                return;
            }

            const title = propType === 'house' ? 'дом' : 'бизнес';
            const pdStr = prompt(`Введите оставшиеся PayDay для нового ${title}:`);
            if (!pdStr) return;
            
            const pd = parseInt(pdStr, 10);
            if (isNaN(pd) || pd <= 0) {
                alert('Некорректное значение PayDay!');
                return;
            }

            const idStr = prompt(`Введите ID ${title} (или оставьте пустым):`);
            const propId = idStr && !isNaN(parseInt(idStr, 10)) ? parseInt(idStr, 10) : null;

            try {
                await fetch('/api/add_item', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ server, scanId, propType, pd, propId })
                });
                loadData();
            } catch(e) { console.error(e); }
        }

        function calculateDropPaydays(item, typeRules) {
            if (!item || item.isPendingPair || item.status === 'frozen') return null;

            const st = item.status || 'insured';
            const rules = typeRules || { insured: 2, uninsured_min: 2, uninsured_max: 3 };

            let decrement = 1;
            let dropLimit = rules.insured;

            if (st === 'uninsured') {
                decrement = 2;
                dropLimit = rules.uninsured_min;
            } else if (st === 'no_activity') {
                decrement = 4;
                dropLimit = rules.uninsured_min;
            }

            const neededPayDays = Math.max(0, Math.ceil((item.pd - (dropLimit - 1)) / decrement));
            return neededPayDays;
        }

        function calculateDropTime(pd, status, typeRules, propType) {
            if (status === 'frozen') {
                return { text: '❄️ Заморожен', isFrozen: true, paydays: null };
            }

            const rules = typeRules || { insured: 2, uninsured_min: 2, uninsured_max: 3 };
            let decrement = 1;
            let dropLimit = rules.insured;

            if (status === 'uninsured') {
                decrement = 2;
                dropLimit = rules.uninsured_min;
            } else if (status === 'no_activity') {
                decrement = 4;
                dropLimit = rules.uninsured_min;
            }

            const neededPayDays = Math.max(0, Math.ceil((pd - (dropLimit - 1)) / decrement));

            const now = new Date();
            const utcTime = now.getTime() + (now.getTimezoneOffset() * 60000);
            const mskNow = new Date(utcTime + (3 * 3600000));

            let targetTime = new Date(mskNow);
            targetTime.setMinutes(0, 0, 0);
            targetTime.setHours(targetTime.getHours() + 1);

            let paydaysApplied = 0;
            while (paydaysApplied < neededPayDays) {
                if (targetTime.getHours() !== 5) {
                    paydaysApplied++;
                }
                if (paydaysApplied < neededPayDays) {
                    targetTime.setHours(targetTime.getHours() + 1);
                }
            }

            const diffMs = targetTime.getTime() - mskNow.getTime();
            if (diffMs <= 0) return { text: '< 1 мин', isFrozen: false, paydays: neededPayDays };

            const totalMinutes = Math.floor(diffMs / 60000);
            const hours = Math.floor(totalMinutes / 60);
            const minutes = totalMinutes % 60;

            let timeStr = '';
            if (hours > 0) timeStr += `${hours} ч `;
            timeStr += `${minutes} мин`;

            return { text: timeStr, isFrozen: false, paydays: neededPayDays };
        }

        function renderTable(items, server, scanId, type, interactive = true, isDropTab = false, info = {}) {
            if (!items || items.length === 0) return '<span class="empty">Нет данных</span>';
            
            let html = '<div class="table-responsive"><table><tr><th>№</th><th>ID</th><th>PD</th><th>Статус</th>' + 
                       (!interactive ? '<th>Слёт через</th>' : '') + 
                       (interactive ? '<th></th>' : '') + '</tr>';
            
            items.forEach((item) => {
                const st = item.status || 'insured';
                const isPending = item.isPendingPair;
                const displayPd = interactive ? (item.basePd !== undefined ? item.basePd : item.pd) : item.pd;
                const badgeClass = interactive ? 'pd-badge-fixed' : (isDropTab ? 'pd-badge-drop' : 'pd-badge');

                let statusControl = '';
                if (interactive) {
                    statusControl = `
                        <div class="btn-group">
                            <button class="btn-opt ${st === 'insured' && !isPending ? 'active-insured' : ''}" onclick="setStatus('${server}', '${scanId}', '${type}', ${item.pos}, 'insured')">Страх.</button>
                            <button class="btn-opt ${st === 'uninsured' && !isPending ? 'active-uninsured' : ''}" onclick="setStatus('${server}', '${scanId}', '${type}', ${item.pos}, 'uninsured')">Не страх.</button>
                            ${type === 'biz' ? `<button class="btn-opt ${st === 'no_activity' && !isPending ? 'active-noact' : ''}" onclick="setStatus('${server}', '${scanId}', 'biz',${item.pos}, 'no_activity')">Без зан.</button>` : ''}
                            <button class="btn-opt ${st === 'frozen' && !isPending ? 'active-frozen' : ''}" onclick="setStatus('${server}', '${scanId}', '${type}', ${item.pos}, 'frozen')">Замор.</button>
                        </div>
                        ${isPending ? '<span class="status-pending">⏳ Ждем пару</span>' : ''}`;
                } else {
                    if (isPending) {
                        statusControl = `<span class="status-pending">⏳ Ожидание 2-го скана</span>`;
                    } else {
                        let label = 'Страховка';
                        let classNm = 'status-insured';
                        
                        if (st === 'uninsured') { label = 'Без страховки'; classNm = 'status-uninsured'; }
                        else if (st === 'no_activity') { label = 'Без занятости'; classNm = 'status-noact'; }
                        else if (st === 'frozen') { label = '❄️ Заморожен'; classNm = 'status-frozen'; }

                        statusControl = `<span class="status-text ${classNm}">${label}</span>`;
                    }
                }

                let dropTimeTd = '';
                if (!interactive) {
                    if (isPending) {
                        dropTimeTd = '<td><span class="empty">—</span></td>';
                    } else {
                        const typeRules = getPropRules(info, type);
                        const dropInfo = calculateDropTime(item.pd, st, typeRules, type);
                        const badgeStyle = dropInfo.isFrozen ? 'time-left-frozen' : 'time-left-badge';
                        dropTimeTd = `<td><span class="${badgeStyle}">${dropInfo.text}</span></td>`;
                    }
                }

                html += `<tr>
                    <td><b>${item.pos}</b></td>
                    <td>${item.propId ? '№' + item.propId : '—'}</td>
                    <td><span class="${badgeClass}">${displayPd} pd</span></td>
                    <td>${statusControl}</td>
                    ${dropTimeTd}
                    ${interactive ? `<td><button class="btn-del" onclick="deleteItem('${server}', '${scanId}', '${type}',${item.pos})">✖</button></td>` : ''}
                </tr>`;
            });
            return html + '</table></div>';
        }

        function renderViewTab() {
            const containerView = document.getElementById('servers-view');
            if (!containerView) return;

            const seasonFilter = document.getElementById('filter-season').value;
            const favFilter = document.getElementById('filter-fav').value;

            let filteredServers = ALL_SERVERS.filter(srv => {
                const info = globalServerData[srv];
                if (!info) return true;

                if (seasonFilter !== 'all') {
                    if (!info.season || String(info.season.id) !== seasonFilter) {
                        return false;
                    }
                }

                if (favFilter === 'fav_only') {
                    if (!favoriteServers.includes(srv)) {
                        return false;
                    }
                }

                return true;
            });

            containerView.innerHTML = '';

            if (filteredServers.length === 0) {
                containerView.innerHTML = '<div class="empty-center">Нет серверов, соответствующих выбранным фильтрам</div>';
                return;
            }

            filteredServers.forEach(srv => {
                const isFav = favoriteServers.includes(srv);
                const starIcon = isFav ? '⭐' : '☆';
                const displayName = getServerDisplayName(srv);

                const cardView = `
                    <div class="server-card">
                        <div class="server-header">
                            <div class="server-title">
                                <span>
                                    <span class="fav-btn" title="В избранное" onclick="toggleFavorite('${srv}')">${starIcon}</span>
                                    ${displayName}
                                </span>
                                <span class="season-badge season-badge-${srv}">Загрузка...</span>
                            </div>
                        </div>
                        <div class="tables-grid">
                            <div>
                                <div class="section-title">Дома</div>
                                <div id="houses-view-${srv}"></div>
                            </div>
                            <div>
                                <div class="section-title">Бизнесы</div>
                                <div id="biz-view-${srv}"></div>
                            </div>
                        </div>
                    </div>`;
                containerView.insertAdjacentHTML('beforeend', cardView);
            });

            updateViewTables();
        }

        function updateViewTables() {
            for (const srv of ALL_SERVERS) {
                const info = globalServerData[srv];
                if (!info) continue;

                if (info.season) {
                    document.querySelectorAll(`.season-badge-${srv}`).forEach(elem => {
                        elem.innerText = `Сезон: ${info.season.display}`;
                    });
                }

                const elemHouse = document.getElementById(`houses-view-${srv}`);
                const elemBiz = document.getElementById(`biz-view-${srv}`);
                if (!elemHouse || !elemBiz) continue;

                const latestConfirmed = info.latestConfirmedScan;

                if (latestConfirmed) {
                    elemHouse.innerHTML = renderTable(latestConfirmed.houses, srv, latestConfirmed.scanId, 'house', false, false, info);
                    elemBiz.innerHTML = renderTable(latestConfirmed.businesses, srv, latestConfirmed.scanId, 'biz', false, false, info);
                } else {
                    elemHouse.innerHTML = '<span class="empty">Ожидание 2-го скана</span>';
                    elemBiz.innerHTML = '<span class="empty">Ожидание 2-го скана</span>';
                }
            }
        }

        function willDropInExactHours(item, typeRules, targetHours) {
            const paydaysToDrop = calculateDropPaydays(item, typeRules);
            if (paydaysToDrop === null) return false;
            return paydaysToDrop === targetHours;
        }

        async function loadData() {
            try {
                const res = await fetch('/api/paydays');
                if (!res.ok) {
                    if (res.status === 401 || res.status === 403) checkAuth();
                    return;
                }
                const data = await res.json();
                globalServerData = data;
                
                renderViewTab();

                const containerUpcoming = document.getElementById('servers-upcoming');
                containerUpcoming.innerHTML = '';
                let hasUpcomingDrops = false;

                const canManage = currentUser && (currentUser.role === 'admin' || currentUser.role === 'support');

                for (const srv of ALL_SERVERS) {
                    const info = data[srv];
                    if (!info) continue;

                    const scans = info.scans || [];
                    const manageTabsElem = document.getElementById(`scan-tabs-${srv}`);
                    const delScanBtnElem = document.getElementById(`btn-del-scan-${srv}`);
                    
                    if (scans.length === 0) {
                        if (manageTabsElem) manageTabsElem.innerHTML = '<span class="empty">Сканирований нет</span>';
                        if (delScanBtnElem) delScanBtnElem.style.display = 'none';
                        if (canManage) {
                            const hm = document.getElementById(`houses-manage-${srv}`);
                            const bm = document.getElementById(`biz-manage-${srv}`);
                            if (hm) hm.innerHTML = '<span class="empty">Нет данных</span>';
                            if (bm) bm.innerHTML = '<span class="empty">Нет данных</span>';
                        }
                        continue;
                    }

                    if (delScanBtnElem) delScanBtnElem.style.display = 'inline-block';

                    if (!activeServerScans[srv] || !scans.some(s => s.scanId === activeServerScans[srv])) {
                        activeServerScans[srv] = scans[scans.length - 1].scanId;
                    }

                    const activeScanId = activeServerScans[srv];

                    if (manageTabsElem) {
                        manageTabsElem.innerHTML = scans.map(s => {
                            let extraClass = '';
                            if (!s.isPaired) extraClass += ' single';
                            if (!s.isActive) extraClass += ' inactive';

                            return `<button class="scan-subtab ${s.scanId === activeScanId ? 'active' : ''}${extraClass}" 
                                    onclick="selectScanTab('${srv}', '${s.scanId}')">
                                ${s.hourLabel} ${!s.isPaired ? '⏳' : ''} ${!s.isActive ? '(неакт)' : ''}
                            </button>`;
                        }).join('');
                    }

                    const curScan = scans.find(s => s.scanId === activeScanId) || scans[scans.length - 1];

                    if (canManage) {
                        const hm = document.getElementById(`houses-manage-${srv}`);
                        const bm = document.getElementById(`biz-manage-${srv}`);
                        if (hm) hm.innerHTML = renderTable(curScan.houses, srv, curScan.scanId, 'house', true, false, info);
                        if (bm) bm.innerHTML = renderTable(curScan.businesses, srv, curScan.scanId, 'biz', true, false, info);
                    }

                    const latestConfirmed = info.latestConfirmedScan;
                    if (latestConfirmed && latestConfirmed.isConfirmed) {
                        const houseRules = getPropRules(info, 'house');
                        const bizRules = getPropRules(info, 'biz');

                        const droppingHouses = (latestConfirmed.houses || []).filter(h => willDropInExactHours(h, houseRules, selectedUpcomingHours));
                        const droppingBiz = (latestConfirmed.businesses || []).filter(b => willDropInExactHours(b, bizRules, selectedUpcomingHours));

                        if (droppingHouses.length > 0 || droppingBiz.length > 0) {
                            hasUpcomingDrops = true;
                            
                            let hourTitleStr = 'в этот час';
                            if (selectedUpcomingHours === 2) hourTitleStr = 'через 2 часа';
                            if (selectedUpcomingHours === 3) hourTitleStr = 'через 3 часа';

                            const displayName = getServerDisplayName(srv);

                            const cardUpcoming = `
                                <div class="server-card">
                                    <div class="server-header">
                                        <div class="server-title">
                                            <span>${displayName}</span>
                                            <span class="season-badge">${info.season ? info.season.display : ''}</span>
                                        </div>
                                    </div>
                                    <div class="tables-grid">
                                        <div>
                                            <div class="section-title">Слетающие дома (${hourTitleStr})</div>
                                            ${renderTable(droppingHouses, srv, latestConfirmed.scanId, 'house', false, true, info)}
                                        </div>
                                        <div>
                                            <div class="section-title">Слетающие бизнесы (${hourTitleStr})</div>
                                            ${renderTable(droppingBiz, srv, latestConfirmed.scanId, 'biz', false, true, info)}
                                        </div>
                                    </div>
                                </div>`;
                            containerUpcoming.insertAdjacentHTML('beforeend', cardUpcoming);
                        }
                    }
                }

                if (!hasUpcomingDrops) {
                    let hourMsg = 'в этот час';
                    if (selectedUpcomingHours === 2) hourMsg = 'через 2 часа';
                    if (selectedUpcomingHours === 3) hourMsg = 'через 3 часа';
                    containerUpcoming.innerHTML = `<div class="empty-center">Слётов имущества ${hourMsg} не ожидается</div>`;
                }
            } catch(e) { console.error(e); }
        }

        function initDashboard() {
            const containerManage = document.getElementById('servers-manage');
            containerManage.innerHTML = '';

            const canManage = currentUser && (currentUser.role === 'admin' || currentUser.role === 'support');

            if (canManage) {
                ALL_SERVERS.forEach(srv => {
                    const cardManage = `
                        <div class="server-card">
                            <div class="server-header">
                                <div class="server-title">
                                    <span>${srv}</span>
                                    <span class="season-badge season-badge-${srv}">Загрузка...</span>
                                </div>
                            </div>
                            <div class="scan-tabs-bar">
                                <div class="scan-tabs-container" id="scan-tabs-${srv}"></div>
                                <button class="btn-delete-scan" id="btn-del-scan-${srv}" onclick="deleteWholeScan('${srv}')">Удалить скан</button>
                            </div>
                            <div class="tables-grid">
                                <div>
                                    <div class="section-header">
                                        <span class="section-title">Дома</span>
                                        <button class="btn-add" onclick="addItemPrompt('${srv}', 'house')">+ Дом</button>
                                    </div>
                                    <div id="houses-manage-${srv}"></div>
                                </div>
                                <div>
                                    <div class="section-header">
                                        <span class="section-title">Бизнесы</span>
                                        <button class="btn-add" onclick="addItemPrompt('${srv}', 'biz')">+ Бизнес</button>
                                    </div>
                                    <div id="biz-manage-${srv}"></div>
                                </div>
                            </div>
                        </div>`;
                    containerManage.insertAdjacentHTML('beforeend', cardManage);
                });
            }

            renderViewTab();
            loadData();
            setInterval(loadData, 10000);
        }

        document.addEventListener('DOMContentLoaded', checkAuth);
    </script>
</head>
<body>
    <div class="app-header">
        <h1>Arizona RP — Мониторинг Слётов</h1>
    </div>

    <!-- Экран входа -->
    <div id="login-screen" class="login-box" style="display: none;">
        <h2>Авторизация</h2>
        <input type="text" id="login-username" placeholder="Логин">
        <input type="password" id="login-password" placeholder="Пароль">
        <button onclick="handleLogin()">Войти</button>
    </div>

    <!-- Основной Дашборд -->
    <div id="main-dashboard" style="display: none;">
        <div class="user-nav">
            <span id="user-info">...</span>
            <button class="btn-logout" onclick="handleLogout()">Выйти</button>
        </div>

        <div class="tabs">
            <button id="btn-tab-view" class="tab-btn active" onclick="switchTab('view')">Общий вид</button>
            <button id="btn-tab-upcoming" class="tab-btn" onclick="switchTab('upcoming')">🔥 Ближайшие слёты</button>
            <button id="btn-tab-manage" class="tab-btn" style="display: none;" onclick="switchTab('manage')">Управление</button>
            <button id="btn-tab-admin" class="tab-btn" style="display: none;" onclick="switchTab('admin')">👑 Админ-панель</button>
        </div>

        <div id="tab-view" class="tab-content active">
            <div class="filter-panel">
                <div class="filter-group">
                    <label for="filter-season">Сезон слётов</label>
                    <select id="filter-season" class="filter-select" onchange="renderViewTab()">
                        <option value="all">Все сезоны</option>
                        <option value="1">1 — По инфе</option>
                        <option value="2">2 — Скорострелы</option>
                        <option value="3">3 — Автогонки</option>
                        <option value="4">4 — По новому</option>
                        <option value="5">5 — Мотогонки</option>
                    </select>
                </div>

                <div class="filter-group">
                    <label for="filter-fav">Серверы</label>
                    <select id="filter-fav" class="filter-select" onchange="renderViewTab()">
                        <option value="all">Все серверы</option>
                        <option value="fav_only">Только избранные ⭐</option>
                    </select>
                </div>
            </div>

            <div class="servers-container" id="servers-view"></div>
        </div>

        <div id="tab-upcoming" class="tab-content">
            <div class="upcoming-filters">
                <button id="btn-upcoming-1h" class="time-filter-btn active" onclick="setUpcomingHoursFilter(1)">В этот час</button>
                <button id="btn-upcoming-2h" class="time-filter-btn" onclick="setUpcomingHoursFilter(2)">Через 2 часа</button>
                <button id="btn-upcoming-3h" class="time-filter-btn" onclick="setUpcomingHoursFilter(3)">Через 3 часа</button>
            </div>
            <div class="servers-container" id="servers-upcoming"></div>
        </div>

        <div id="tab-manage" class="tab-content">
            <div class="servers-container" id="servers-manage"></div>
        </div>

        <div id="tab-admin" class="tab-content">
            <div class="server-card" style="max-width: 1200px; margin: 0 auto 20px auto;">
                <h3 style="margin-bottom: 12px; font-size: 1rem; color: var(--text-main);">Создать нового пользователя</h3>
                <div style="display: flex; flex-wrap: wrap; gap: 10px;">
                    <input type="text" id="new-username" placeholder="Логин" class="filter-select" style="flex: 1; min-width: 140px;">
                    <input type="password" id="new-password" placeholder="Пароль" class="filter-select" style="flex: 1; min-width: 140px;">
                    <select id="new-role" class="select-role" style="padding: 8px 12px;">
                        <option value="user">User</option>
                        <option value="support">Support</option>
                        <option value="admin">Admin</option>
                    </select>
                    <button class="btn-add" style="padding: 8px 16px; background-color: var(--accent-primary); color: #fff;" onclick="handleCreateUser()">Создать</button>
                </div>
            </div>

            <div class="server-card" style="max-width: 1200px; margin: 0 auto 20px auto;">
                <h3 style="margin-bottom: 12px; font-size: 1rem; color: var(--text-main);">Список пользователей</h3>
                <div id="admin-users-table">Загрузка...</div>
            </div>

            <div class="server-card" style="max-width: 1200px; margin: 0 auto;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                    <h3 style="margin: 0; font-size: 1rem; color: var(--text-main);">📜 Логи сканирования (за сегодня)</h3>
                    <button class="btn-add" onclick="loadScanLogs()">Обновить</button>
                </div>
                <div id="admin-scan-logs-table">Загрузка логов...</div>
            </div>
        </div>
    </div>
</body>
</html>
"""

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
        if srv not in server_data:
            server_data[srv] = {"scans": []}
        if "scans" not in server_data[srv]:
            server_data[srv]["scans"] = []

        scans = server_data[srv]["scans"]

        waiting_scan = None
        for s in reversed(scans):
            if not s.get("isPaired", False) and s.get("isActive", True):
                s_dt = datetime.strptime(s["scanTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)
                if 3000 <= (now_msk - s_dt).total_seconds() <= 4200:
                    waiting_scan = s
                    break

        house_entries = [e for e in payload.entries if e.propType == "house"]
        biz_entries = [e for e in payload.entries if e.propType != "house"]

        is_pair = waiting_scan is not None

        prev_houses = waiting_scan.get("houses", []) if is_pair else []
        prev_biz = waiting_scan.get("businesses", []) if is_pair else []

        house_matches = find_pairs_with_offset(prev_houses, house_entries, "house") if is_pair else {}
        biz_matches = find_pairs_with_offset(prev_biz, biz_entries, "biz") if is_pair else {}

        houses = []
        for idx, item in enumerate(house_entries):
            if idx in house_matches:
                prev_item, auto_status = house_matches[idx]
                prev_item["isPendingPair"] = False
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": auto_status,
                    "isPendingPair": False,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            else:
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": "insured",
                    "isPendingPair": not is_pair,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            houses.append(record)

        businesses = []
        for idx, item in enumerate(biz_entries):
            if idx in biz_matches:
                prev_item, auto_status = biz_matches[idx]
                prev_item["isPendingPair"] = False
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": auto_status,
                    "isPendingPair": False,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            else:
                record = {
                    "basePd": item.pd,
                    "pd": item.pd,
                    "propId": item.propId,
                    "pos": item.pos,
                    "status": "insured",
                    "isPendingPair": not is_pair,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            businesses.append(record)

        houses = sorted(houses, key=lambda x: x["pos"])
        businesses = sorted(businesses, key=lambda x: x["pos"])

        existing_scan = next((s for s in scans if s["scanId"] == scan_id), None)
        
        if existing_scan:
            # Объединяем существующие данные с новыми (если пришел только один тип имущества, второй не затирается)
            merged_houses = houses if house_entries else existing_scan.get("houses", [])
            merged_businesses = businesses if biz_entries else existing_scan.get("businesses", [])

            existing_scan["houses"] = sorted(merged_houses, key=lambda x: x["pos"])
            existing_scan["businesses"] = sorted(merged_businesses, key=lambda x: x["pos"])
            existing_scan["scanTime"] = now_msk.strftime("%Y-%m-%d %H:%M:%S")

            if is_pair:
                existing_scan["isConfirmed"] = True
                existing_scan["isPaired"] = True
                existing_scan["pairScanId"] = waiting_scan["scanId"]
        else:
            new_scan_obj = {
                "scanId": scan_id,
                "hourLabel": hour_label,
                "scanTime": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                "houses": houses,
                "businesses": businesses,
                "isConfirmed": is_pair,
                "isPaired": is_pair,
                "isActive": True,
                "pairScanId": waiting_scan["scanId"] if is_pair else None
            }
            scans.append(new_scan_obj)

        if is_pair:
            waiting_scan["isConfirmed"] = True
            waiting_scan["isPaired"] = True
            waiting_scan["pairScanId"] = scan_id

            for s in scans:
                if s["scanId"] not in [scan_id, waiting_scan["scanId"]]:
                    s["isActive"] = False

        await save_data_to_file_async()

    return {"status": "ok", "scanId": scan_id, "count": len(payload.entries)}

@app.post("/api/update_status")
async def update_status(data: UpdateStatusModel, username: str = Depends(verify_editor)):
    srv = data.server
    async with data_lock:
        if srv in server_data:
            for scan in server_data[srv].get("scans", []):
                if scan["scanId"] == data.scanId:
                    target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
                    for item in scan[target_key]:
                        if item["pos"] == data.pos:
                            item["status"] = data.status
                            item["isPendingPair"] = False
                            await save_data_to_file_async()
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
                    scan[target_key] = [item for item in scan[target_key] if item["pos"] != data.pos]
                    for new_idx, item in enumerate(scan[target_key], start=1):
                        item["pos"] = new_idx
                    await save_data_to_file_async()
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
                    return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server or Scan not found")

@app.get("/api/paydays")
async def get_paydays(username: str = Depends(verify_auth)):
    async with data_lock:
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
