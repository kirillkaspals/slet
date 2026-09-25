import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Header, HTTPException, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Literal

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY", "SECRET_KEY_12345")
SITE_PASSWORD = os.getenv("SITE_PASSWORD", "mysecretpass")

# Надежное определение пути к папке с шаблонами
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

def get_db():
    if not DATABASE_URL:
        raise HTTPException(
            status_code=500, 
            detail="DATABASE_URL variable is not configured in Environment Variables!"
        )
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    try:
        yield conn
    finally:
        conn.close()

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Access denied")

class SletItem(BaseModel):
    id: int
    paydays: int

class SletPayload(BaseModel):
    server_id: int
    type: Literal["house", "biz"]
    items: List[SletItem]

@app.on_event("startup")
def startup_db():
    if not DATABASE_URL:
        print("[WARNING] DATABASE_URL is not set, skipping database init.")
        return
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS house_paydays (
                id SERIAL PRIMARY KEY,
                house_id INT NOT NULL,
                paydays_left INT NOT NULL,
                server_id INT NOT NULL,
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS biz_paydays (
                id SERIAL PRIMARY KEY,
                biz_id INT NOT NULL,
                paydays_left INT NOT NULL,
                server_id INT NOT NULL,
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("[SUCCESS] Tables initialized successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to initialize database on startup: {e}")

@app.post("/api/paydays", dependencies=[Depends(verify_api_key)])
def save_paydays(payload: SletPayload, conn=Depends(get_db)):
    table = "house_paydays" if payload.type == "house" else "biz_paydays"
    id_col = "house_id" if payload.type == "house" else "biz_id"
    
    cur = conn.cursor()
    try:
        for item in payload.items:
            cur.execute(f"""
                INSERT INTO {table} ({id_col}, paydays_left, server_id, scanned_at)
                VALUES (%s, %s, %s, NOW())
            """, (item.id, item.paydays, payload.server_id))
        conn.commit()
        return {"status": "success", "saved": len(payload.items)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()

@app.get("/", response_class=HTMLResponse)
def view_site(request: Request, password: str = "", server_id: int = 1):
    if password != SITE_PASSWORD:
        return HTMLResponse(
            "<body style='background:#181825;color:white;font-family:sans-serif;text-align:center;padding-top:50px;'>"
            "<h2>🔒 Доступ закрыт</h2><p>Укажите верный пароль в ссылке: <code>?password=ВАШ_ПАРОЛЬ</code></p></body>", 
            status_code=403
        )
    
    if not DATABASE_URL:
        return HTMLResponse(
            "<body style='background:#181825;color:#f38ba8;font-family:sans-serif;padding:20px;text-align:center;'>"
            "<h2>⚠️ Ошибка конфигурации</h2><p>Переменная <code>DATABASE_URL</code> не добавлена в Environment Variables на Render.</p></body>",
            status_code=500
        )

    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT DISTINCT ON (house_id) house_id, paydays_left, scanned_at 
            FROM house_paydays WHERE server_id = %s
            ORDER BY house_id, scanned_at DESC
        """, (server_id,))
        houses = cur.fetchall()
        houses = sorted(houses, key=lambda x: x['paydays_left'])

        cur.execute("""
            SELECT DISTINCT ON (biz_id) biz_id, paydays_left, scanned_at 
            FROM biz_paydays WHERE server_id = %s
            ORDER BY biz_id, scanned_at DESC
        """, (server_id,))
        bizs = cur.fetchall()
        bizs = sorted(bizs, key=lambda x: x['paydays_left'])

        cur.close()
        conn.close()

        return templates.TemplateResponse("index.html", {
            "request": request, 
            "houses": houses, 
            "bizs": bizs,
            "server_id": server_id,
            "password": password
        })
    except Exception as e:
        return HTMLResponse(
            f"<body style='background:#181825;color:#f38ba8;font-family:sans-serif;padding:20px;'>"
            f"<h2>❌ Ошибка базы данных:</h2><pre>{str(e)}</pre></body>",
            status_code=500
        )
