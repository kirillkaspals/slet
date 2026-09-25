from fastapi import FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

app = FastAPI(title="Arizona Property Tracker API")

# Разрешаем запросы с любых источников (для работы сайта/панели)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Секретный ключ (должен совпадать с SECRET_KEY в Lua-скрипте)
SECRET_KEY = "usefguIHSFUSDFGUjhjfk88448"

# Хранилище данных в памяти (при необходимости замените на SQLite / PostgreSQL)
db_paydays = []
db_logs = []


# Модели данных Pydantic для валидации входного JSON
class PropertyEntry(BaseModel):
    propType: str
    pd: int
    propId: Optional[int] = None
    pos: int


class Payload(BaseModel):
    server: str
    scanner: Optional[str] = "unknown"
    entries: List[PropertyEntry]


@app.post("/api/paydays")
async def receive_paydays(payload: Payload, x_secret_key: Optional[str] = Header(None)):
    # Проверка секретного ключа (API Key)
    if x_secret_key != SECRET_KEY:
        print(f"[AUTH ERROR] Неверный заголовок X-Secret-Key: {x_secret_key}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Invalid Secret Key"
        )

    timestamp = datetime.utcnow().isoformat()

    # Сохраняем полученные объекты
    for entry in payload.entries:
        db_paydays.append({
            "server": payload.server,
            "scanner": payload.scanner,
            "propType": entry.propType,
            "pd": entry.pd,
            "propId": entry.propId,
            "pos": entry.pos,
            "receivedAt": timestamp
        })

    # Сохраняем лог сессии сканирования
    db_logs.append({
        "server": payload.server,
        "scanner": payload.scanner,
        "count": len(payload.entries),
        "timestamp": timestamp
    })

    print(f"[SCAN SUCCESS] Сервер: {payload.server} | Ник: {payload.scanner} | Записей: {len(payload.entries)}")
    return {"status": "ok", "count": len(payload.entries)}


@app.get("/api/paydays")
async def get_paydays():
    """Эндпоинт для отображения информации на вашем сайте"""
    return db_paydays


@app.get("/api/logs")
async def get_logs():
    """Эндпоинт для просмотра истории сканов"""
    return db_logs


@app.get("/")
async def root():
    return {"message": "Arizona Property Tracker API (Python) is running"}
