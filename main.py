import asyncio
import contextlib
import json
import os
import zoneinfo
from datetime import datetime, timedelta
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

    async with db_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT username, expires_at FROM user_sessions WHERE token = $1", token
        )
        if session:
            if session["expires_at"] > datetime.utcnow():
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
    confirmed_scans = [s for s in scans if s.get("isConfirmed", False)]
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
        curr_prop_id = getattr(curr, 'propId', None)
        if curr_prop_id is not None:
            for p_idx, prev in enumerate(prev_items):
                if p_idx in used_prev_indices:
                    continue
                if prev.get("propId") == curr_prop_id:
                    base_pd = prev.get("basePd", prev.get("pd", 0))
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

            base_pd = prev.get("basePd", prev.get("pd", 0))
            diff = base_pd - curr.pd

            if diff in valid_diffs:
                auto_status = determine_status(diff, prop_type)
                matches[c_idx] = (prev, auto_status)
                used_prev_indices.add(p_idx)
                break

    return matches

# --- ЛОГИКА PAYDAY И ОЧИСТКИ ОДИНОЧНЫХ СТАРЫХ СКАНОВ ---
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
        print(f"[{now_msk.strftime('%Y-%m-%d %H:%M:%S')}] Выполнение списания PayDay и очистка сканов...")
        for srv, data in server_data.items():
            scans = data.get("scans", [])
            if not scans:
                continue

            valid_scans = []
            for s in scans:
                scan_dt = datetime.strptime(s["scanTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)
                time_diff = (now_msk - scan_dt).total_seconds() / 3600.0

                # Если скан был одиночным без пары и пролежал более 1.1 часа — удаляем его
                if not s.get("hasPair", False) and time_diff >= 1.1:
                    print(f"[{srv}] Удален одиночный устаревший скан {s['scanId']}")
                    continue
                valid_scans.append(s)

            data["scans"] = valid_scans
            scans = valid_scans

            latest_scan = get_latest_confirmed_scan(scans)
            if not latest_scan:
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

    expires_at = datetime.utcnow() + timedelta(days=SESSION_EXPIRE_DAYS)

    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO user_sessions (token, username, expires_at) VALUES ($1, $2, $3)",
                token, user["username"], expires_at
            )
    
    max_age = SESSION_EXPIRE_DAYS * 24 * 3600
    response.set_cookie(
        key="session_token",
        value=token,
        max_age=max_age,
        expires=expires_at,
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
        
        last_scan = scans[-1] if scans else None
        prev_scan = None
        is_consecutive_hour = False

        if last_scan:
            last_scan_dt = datetime.strptime(last_scan["scanTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=MSK_TZ)
            time_diff_sec = (now_msk - last_scan_dt).total_seconds()
            
            # Проверяем, был ли предыдущий скан ровно 1 час назад (с небольшим запасом от 50 до 70 минут)
            if 3000 <= time_diff_sec <= 4200:
                prev_scan = last_scan
                is_consecutive_hour = True

        house_entries = [e for e in payload.entries if e.propType == "house"]
        biz_entries = [e for e in payload.entries if e.propType != "house"]

        prev_houses = prev_scan.get("houses", []) if (prev_scan and is_consecutive_hour) else []
        prev_biz = prev_scan.get("businesses", []) if (prev_scan and is_consecutive_hour) else []

        house_matches = find_pairs_with_offset(prev_houses, house_entries, "house") if (prev_scan and is_consecutive_hour) else {}
        biz_matches = find_pairs_with_offset(prev_biz, biz_entries, "biz") if (prev_scan and is_consecutive_hour) else {}

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
                    "isPendingPair": True,
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
                    "isPendingPair": True,
                    "updatedAt": now_msk.strftime("%H:%M:%S")
                }
            businesses.append(record)

        houses = sorted(houses, key=lambda x: x["pos"])
        businesses = sorted(businesses, key=lambda x: x["pos"])

        is_pair_found = is_consecutive_hour and (len(house_matches) > 0 or len(biz_matches) > 0)

        if is_pair_found and prev_scan:
            prev_scan["isConfirmed"] = True
            prev_scan["hasPair"] = True

        existing_scan = next((s for s in scans if s["scanId"] == scan_id), None)
        has_houses_in_payload = len(house_entries) > 0
        has_biz_in_payload = len(biz_entries) > 0

        new_scan_obj = {
            "scanId": scan_id,
            "hourLabel": hour_label,
            "scanTime": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
            "houses": houses,
            "businesses": businesses,
            "isConfirmed": is_pair_found,
            "hasPair": is_pair_found
        }

        if existing_scan:
            if has_houses_in_payload:
                existing_scan["houses"] = houses
            if has_biz_in_payload:
                existing_scan["businesses"] = businesses
            existing_scan["scanTime"] = now_msk.strftime("%Y-%m-%d %H:%M:%S")
            existing_scan["isConfirmed"] = existing_scan.get("isConfirmed", False) or is_pair_found
            existing_scan["hasPair"] = existing_scan.get("hasPair", False) or is_pair_found
        else:
            if is_consecutive_hour:
                # Последовательный скан -> Добавляем в общую цепочку
                scans.append(new_scan_obj)
            else:
                # Произошел разрыв во времени (> 1 часа).
                # Удаляем ВСЕ прошлые сканы и оставляем только текущий новый скан одиночкой.
                server_data[srv]["scans"] = [new_scan_obj]

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

# --- ДАШБОРД ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Arizona RP — Мониторинг Слётов</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #121212; color: #e0e0e0; margin: 0; padding: 20px; }
        h1 { text-align: center; color: #ff9800; margin-bottom: 20px; }
        
        .login-box { max-width: 400px; margin: 80px auto; background: #1e1e1e; padding: 30px; border-radius: 8px; border: 1px solid #333; text-align: center; }
        .login-box input { width: 90%; padding: 10px; margin: 10px 0; background: #2a2a2a; border: 1px solid #444; color: #fff; border-radius: 4px; }
        .login-box button { width: 95%; padding: 10px; background: #ff9800; border: none; font-weight: bold; cursor: pointer; border-radius: 4px; color: #121212; }

        .user-nav { display: flex; justify-content: space-between; align-items: center; max-width: 1100px; margin: 0 auto 20px auto; }
        .btn-logout { background: #c62828; color: white; border: none; padding: 6px 14px; border-radius: 4px; cursor: pointer; }

        .tabs { display: flex; justify-content: center; gap: 10px; margin-bottom: 25px; }
        .tab-btn { background-color: #1e1e1e; color: #aaa; border: 1px solid #333; padding: 10px 24px; font-size: 1em; font-weight: bold; border-radius: 6px; cursor: pointer; transition: 0.2s; }
        .tab-btn.active { background-color: #ff9800; color: #121212; border-color: #ff9800; }
        .tab-btn:hover:not(.active) { background-color: #2a2a2a; color: #fff; }

        .tab-content { display: none; }
        .tab-content.active { display: block; }

        .upcoming-filters {
            display: flex;
            justify-content: center;
            gap: 12px;
            margin-bottom: 20px;
        }
        .time-filter-btn {
            background-color: #1e1e1e;
            color: #aaa;
            border: 1px solid #333;
            padding: 8px 18px;
            font-size: 0.9em;
            font-weight: bold;
            border-radius: 20px;
            cursor: pointer;
            transition: 0.2s;
        }
        .time-filter-btn.active {
            background-color: #e53935;
            color: #ffffff;
            border-color: #ef5350;
            box-shadow: 0 0 8px rgba(229, 57, 53, 0.4);
        }

        .filter-panel {
            background: #1e1e1e;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 20px;
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            align-items: center;
            justify-content: center;
        }
        .filter-group {
            display: flex;
            flex-direction: column;
            gap: 5px;
        }
        .filter-group label {
            font-size: 0.85em;
            color: #ff9800;
            font-weight: bold;
        }
        .filter-select {
            background: #2a2a2a;
            color: #fff;
            border: 1px solid #444;
            padding: 6px 10px;
            border-radius: 4px;
            font-size: 0.9em;
        }
        .fav-btn {
            cursor: pointer;
            font-size: 1.1em;
            user-select: none;
            margin-right: 6px;
            transition: transform 0.1s;
        }
        .fav-btn:hover {
            transform: scale(1.2);
        }

        .servers-container { display: flex; flex-direction: column; gap: 15px; max-width: 1100px; margin: 0 auto; }
        .server-card { background-color: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 15px 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
        .server-header { border-bottom: 1px solid #333; padding-bottom: 8px; margin-bottom: 12px; }
        .server-title { font-size: 1.3em; font-weight: bold; color: #4caf50; display: flex; justify-content: space-between; align-items: center; }
        .season-badge { font-size: 0.75em; background-color: #332a12; color: #ffb74d; border: 1px solid #ff9800; padding: 3px 8px; border-radius: 12px; font-weight: normal; }
        
        .scan-tabs-bar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; border-bottom: 1px solid #2a2a2a; padding-bottom: 6px; }
        .scan-tabs-container { display: flex; gap: 6px; overflow-x: auto; }
        .scan-subtab { background-color: #252525; color: #888; border: 1px solid #3a3a3a; padding: 4px 12px; font-size: 0.85em; border-radius: 4px; cursor: pointer; white-space: nowrap; transition: 0.2s; }
        .scan-subtab.active { background-color: #1e88e5; color: #fff; border-color: #64b5f6; font-weight: bold; }
        .scan-subtab.single { border-color: #ff9800; color: #ffb74d; }
        .scan-subtab:hover:not(.active) { background-color: #333; color: #ddd; }

        .btn-delete-scan { background-color: #b71c1c; color: #fff; border: none; padding: 4px 8px; border-radius: 4px; font-size: 0.75em; font-weight: bold; cursor: pointer; transition: 0.2s; }
        .btn-delete-scan:hover { background-color: #d32f2f; }

        .tables-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
        @media (max-width: 850px) { .tables-grid { grid-template-columns: 1fr; } }
        .section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
        .section-title { font-size: 0.95em; font-weight: bold; color: #00bcd4; }
        .btn-add { background-color: #008cba; color: white; border: none; border-radius: 4px; padding: 2px 8px; font-size: 0.85em; font-weight: bold; cursor: pointer; transition: 0.2s; }
        .btn-add:hover { background-color: #005f73; }
        table { width: 100%; border-collapse: collapse; font-size: 0.85em; }
        th, td { padding: 6px 8px; text-align: left; border-bottom: 1px solid #2a2a2a; }
        th { background-color: #252525; color: #aaa; }
        .pd-badge { background-color: #e53935; color: #fff; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
        .pd-badge-drop { background-color: #d32f2f; color: #ffeb3b; padding: 2px 6px; border-radius: 4px; font-weight: bold; animation: pulse 1.5s infinite; }
        .pd-badge-fixed { background-color: #37474f; color: #81d4fa; border: 1px solid #00838f; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
        
        .time-left-badge { font-weight: bold; color: #ffb74d; font-size: 0.85em; background-color: #231b0c; padding: 2px 6px; border-radius: 4px; border: 1px solid #5d4037; }
        .time-left-frozen { font-weight: bold; color: #64b5f6; font-size: 0.85em; }

        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.6; }
            100% { opacity: 1; }
        }

        .btn-group { display: flex; gap: 3px; flex-wrap: wrap; }
        .btn-opt { background-color: #2a2a2a; color: #888; border: 1px solid #444; padding: 3px 6px; font-size: 0.75em; border-radius: 4px; cursor: pointer; transition: 0.2s; }
        .btn-opt.active-insured { background-color: #2e7d32; color: #fff; border-color: #4caf50; }
        .btn-opt.active-uninsured { background-color: #c62828; color: #fff; border-color: #ef5350; }
        .btn-opt.active-noact { background-color: #b71c1c; color: #fff; border-color: #ff1744; font-weight: bold; }
        .btn-opt.active-frozen { background-color: #1565c0; color: #fff; border-color: #42a5f5; font-weight: bold; }
        
        .status-text { font-weight: bold; font-size: 0.85em; padding: 2px 6px; border-radius: 4px; display: inline-block; }
        .status-insured { color: #81c784; }
        .status-uninsured { color: #e57373; }
        .status-noact { color: #ff5252; }
        .status-frozen { color: #64b5f6; }
        .status-pending { color: #ffb74d; font-style: italic; }

        .btn-del { background-color: transparent; color: #ef5350; border: 1px solid #ef5350; padding: 2px 6px; font-size: 0.8em; border-radius: 4px; cursor: pointer; transition: 0.2s; }
        .btn-del:hover { background-color: #ef5350; color: #fff; }
        .btn-user-del { background-color: #c62828; color: #fff; border: none; padding: 4px 8px; border-radius: 4px; cursor: pointer; font-size: 0.8em; }
        .btn-user-del:hover { background-color: #e53935; }
        
        .select-role { background: #2a2a2a; color: #fff; border: 1px solid #444; padding: 4px; border-radius: 4px; font-size: 0.85em; }
        
        .empty { color: #666; font-style: italic; font-size: 0.85em; }
        .empty-center { text-align: center; color: #888; font-style: italic; padding: 20px; background-color: #1e1e1e; border-radius: 8px; border: 1px solid #333; }
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
            
            let html = `<table><tr><th>ID</th><th>Логин</th><th>Роль</th><th>Доступ</th><th>Действия</th></tr>`;
            users.forEach(u => {
                const isSelf = u.username === currentUser.username;
                
                const toggleAccessBtn = !isSelf ? 
                    `<button onclick="toggleUserAccess(${u.id}, ${u.is_allowed})">${u.is_allowed ? 'Заблокировать' : 'Разблокировать'}</button>` : '—';
                
                const deleteUserBtn = !isSelf ? 
                    `<button class="btn-user-del" onclick="deleteUser(${u.id}, '${u.username}')">Удалить</button>` : '';

                const changePwBtn = `<button class="btn-add" style="background-color: #f57c00;" onclick="changeUserPassword(${u.id}, '${u.username}')">🔑 Пароль</button>`;

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
                    <td style="display: flex; gap: 5px; align-items: center; flex-wrap: wrap;">
                        ${changePwBtn}
                        ${toggleAccessBtn} 
                        ${deleteUserBtn}
                    </td>
                </tr>`;
            });
            document.getElementById('admin-users-table').innerHTML = html + '</table>';
        }

        async function loadScanLogs() {
            const res = await fetch('/api/admin/scan_logs');
            if (!res.ok) return;
            const logs = await res.json();
            
            if (logs.length === 0) {
                document.getElementById('admin-scan-logs-table').innerHTML = '<span class="empty">За сегодня сканов еще не зафиксировано</span>';
                return;
            }

            let html = `<table><tr><th>Время (МСК)</th><th>Сервер</th><th>Отправитель</th></tr>`;
            logs.forEach(l => {
                html += `<tr>
                    <td><b>${l.created_at}</b></td>
                    <td><span style="color: #00bcd4;">${l.server}</span></td>
                    <td><span style="color: #ffb74d;">${l.scanner}</span></td>
                </tr>`;
            });
            document.getElementById('admin-scan-logs-table').innerHTML = html + '</table>';
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
            
            let html = '<table><tr><th>№</th><th>ID</th><th>PD</th><th>Статус</th>' + 
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
                            <button class="btn-opt ${st === 'frozen' && !isPending ? 'active-frozen' : ''}" onclick="setStatus('${server}', '${scanId}', '${type}', ${item.pos}, 'frozen')">Заморожен</button>
                        </div>
                        ${isPending ? '<span class="status-pending">⏳ Ожидание пары</span>' : ''}`;
                } else {
                    if (isPending) {
                        statusControl = `<span class="status-pending">⏳ Ожидание пары</span>`;
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
                    <td>${item.pos}</td>
                    <td>${item.propId ? '№' + item.propId : '—'}</td>
                    <td><span class="${badgeClass}">${displayPd} pd</span></td>
                    <td>${statusControl}</td>
                    ${dropTimeTd}
                    ${interactive ? `<td><button class="btn-del" onclick="deleteItem('${server}', '${scanId}', '${type}',${item.pos})">✖</button></td>` : ''}
                </tr>`;
            });
            return html + '</table>';
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
                                    <span class="fav-btn" title="Добавить в избранное" onclick="toggleFavorite('${srv}')">${starIcon}</span>
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
                    elemHouse.innerHTML = '<span class="empty">Ожидание последовательного скана</span>';
                    elemBiz.innerHTML = '<span class="empty">Ожидание последовательного скана</span>';
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
                        manageTabsElem.innerHTML = scans.map(s => `
                            <button class="scan-subtab ${s.scanId === activeScanId ? 'active' : ''} ${!s.hasPair ? 'single' : ''}" 
                                    onclick="selectScanTab('${srv}', '${s.scanId}')">
                                ${s.hourLabel} ${!s.hasPair ? '⏳' : ''}
                            </button>
                        `).join('');
                    }

                    const curScan = scans.find(s => s.scanId === activeScanId) || scans[scans.length - 1];

                    if (canManage) {
                        const hm = document.getElementById(`houses-manage-${srv}`);
                        const bm = document.getElementById(`biz-manage-${srv}`);
                        if (hm) hm.innerHTML = renderTable(curScan.houses, srv, curScan.scanId, 'house', true, false, info);
                        if (bm) bm.innerHTML = renderTable(curScan.businesses, srv, curScan.scanId, 'biz', true, false, info);
                    }

                    const latestConfirmed = info.latestConfirmedScan;
                    if (latestConfirmed) {
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
    <h1>Arizona RP — Мониторинг Слётов</h1>

    <!-- Экран входа -->
    <div id="login-screen" class="login-box" style="display: none;">
        <h2>Авторизация</h2>
        <input type="text" id="login-username" placeholder="Логин"><br>
        <input type="password" id="login-password" placeholder="Пароль"><br>
        <button onclick="handleLogin()">Войти</button>
    </div>

    <!-- Основной Дашборд -->
    <div id="main-dashboard" style="display: none;">
        <div class="user-nav">
            <span id="user-info"></span>
            <button class="btn-logout" onclick="handleLogout()">Выйти</button>
        </div>

        <div class="tabs">
            <button id="btn-tab-view" class="tab-btn active" onclick="switchTab('view')">Общий вид</button>
            <button id="btn-tab-upcoming" class="tab-btn" onclick="switchTab('upcoming')">🔥 Ближайшие слёты</button>
            <button id="btn-tab-manage" class="tab-btn" style="display: none;" onclick="switchTab('manage')">Управление сканами</button>
            <button id="btn-tab-admin" class="tab-btn" style="display: none;" onclick="switchTab('admin')">👑 Админ-панель</button>
        </div>

        <div id="tab-view" class="tab-content active">
            <div class="filter-panel">
                <div class="filter-group">
                    <label for="filter-season">Сезон слётов:</label>
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
                    <label for="filter-fav">Серверы:</label>
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
            <div class="server-card" style="max-width: 1100px; margin: 0 auto 20px auto;">
                <h3>Создать нового пользователя</h3>
                <input type="text" id="new-username" placeholder="Новый логин" style="padding: 6px; margin-right: 10px;">
                <input type="password" id="new-password" placeholder="Новый пароль" style="padding: 6px; margin-right: 10px;">
                <select id="new-role" class="select-role" style="padding: 6px; margin-right: 10px;">
                    <option value="user">User</option>
                    <option value="support">Support</option>
                    <option value="admin">Admin</option>
                </select>
                <button class="btn-add" style="padding: 6px 12px;" onclick="handleCreateUser()">Создать аккаунт</button>
            </div>

            <div class="server-card" style="max-width: 1100px; margin: 0 auto 20px auto;">
                <h3>Список пользователей</h3>
                <div id="admin-users-table">Загрузка...</div>
            </div>

            <div class="server-card" style="max-width: 1100px; margin: 0 auto;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                    <h3 style="margin: 0;">📜 Логи сканирования (за сегодня)</h3>
                    <button class="btn-add" onclick="loadScanLogs()">Обновить</button>
                </div>
                <div id="admin-scan-logs-table">Загрузка логов...</div>
            </div>
        </div>
    </div>
</body>
</html>
"""
