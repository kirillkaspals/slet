from fastapi import FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
import asyncio
import contextlib
import zoneinfo
import json
import os

SECRET_KEY = "usefguIHSFUSDFGUjhjfk88448"
DATA_FILE = "server_data.json"

# Список всех 33 серверов Arizona RP
ALL_SERVERS = [
    "Phoenix", "Tucson", "Scottdale", "Chandler", "Brainburg",
    "Saint-Rose", "Mesa", "Red-Rock", "Yuma", "Surprise",
    "Prescott", "Glendale", "Kingman", "Winslow", "Payson",
    "Gilbert", "Show Low", "Casa-Grande", "Page", "Sun-City",
    "Queen-Creek", "Sedona", "Holiday", "Wednesday", "Yava",
    "Faraway", "Bumble Bee", "Christmas", "Love", "Mirage",
    "Drake", "Space", "Home"
]

# Словарик сезонов ловли
SEASONS_MAP = {
    1: "По инфе",
    2: "Скорострелы",
    3: "Автогонки",
    4: "По новому",
    5: "Мотогонки"
}

# Исходная расстановка сезонов на базовую неделю
BASE_WEEK_START = datetime(2025, 9, 22, 5, 0, 0, tzinfo=zoneinfo.ZoneInfo("Europe/Moscow"))

BASE_SERVER_SEASONS = {
    "Phoenix": 4,      # 01 - По новому(4)
    "Tucson": 1,       # 02 - По инфе(1)
    "Scottdale": 2,    # 03 - Скорострелы(2)
    "Chandler": 1,     # 04 - По инфе(1)
    "Brainburg": 4,    # 05 - По новому(4)
    "Saint-Rose": 4,   # 06 - По новому(4)
    "Mesa": 3,         # 07 - Автогонки(3)
    "Red-Rock": 2,     # 08 - Скорострелы(2)
    "Yuma": 4,         # 09 - По новому(4)
    "Surprise": 2,     # 10 - Скорострелы(2)
    "Prescott": 1,     # 11 - По инфе(1)
    "Glendale": 2,     # 12 - Скорострелы(2)
    "Kingman": 5,      # 13 - Мотогонки(5)
    "Winslow": 3,      # 14 - Автогонки(3)
    "Payson": 3,       # 15 - Автогонки(3)
    "Gilbert": 5,      # 16 - Мотогонки(5)
    "Show Low": 5,     # 17 - Мотогонки(5)
    "Casa-Grande": 5,  # 18 - Мотогонки(5)
    "Page": 1,         # 19 - По инфе(1)
    "Sun-City": 3,     # 20 - Автогонки(3)
    "Queen-Creek": 5,  # 21 - Мотогонки(5)
    "Sedona": 1,       # 22 - По инфе(1)
    "Holiday": 4,      # 23 - По новому(4)
    "Wednesday": 2,    # 24 - Скорострелы(2)
    "Yava": 2,         # 25 - Скорострелы(2)
    "Faraway": 2,      # 26 - Скорострелы(2)
    "Bumble Bee": 5,   # 27 - Мотогонки(5)
    "Christmas": 2,    # 28 - Скорострелы(2)
    "Love": 2,         # 29 - Скорострелы(2)
    "Mirage": 2,       # 30 - Скорострелы(2)
    "Drake": 2,        # 31 - Скорострелы(2)
    "Space": 5,        # 32 - Мотогонки(5)
    "Home": 1          # 33 - По инфе(1)
}

# --- Логика сохранения и загрузки данных ---
def save_data_to_file():
    """Сохраняет текущие данные серверов в JSON-файл."""
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(server_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка сохранения данных в файл: {e}")

def load_data_from_file():
    """Загружает данные из JSON-файла при старте."""
    data_store = {srv: {"houses": [], "businesses": []} for srv in ALL_SERVERS}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for srv in ALL_SERVERS:
                    if srv in loaded:
                        data_store[srv] = loaded[srv]
            print("Данные успешно загружены из файла!")
        except Exception as e:
            print(f"Ошибка загрузки данных из файла: {e}")
    return data_store

# Инициализация хранилища данных с загрузкой из файла
server_data = load_data_from_file()

def get_current_season_id(server: str) -> int:
    """Вычисляет ID сезона с учетом смещения недель от базового понедельника (5:00 МСК)."""
    base_id = BASE_SERVER_SEASONS.get(server, 1)
    now_msk = datetime.now(zoneinfo.ZoneInfo("Europe/Moscow"))
    
    diff = now_msk - BASE_WEEK_START
    weeks_passed = diff.days // 7
    
    if diff.total_seconds() < 0:
        weeks_passed = 0
        
    current_season_id = ((base_id - 1 + weeks_passed) % 5) + 1
    return current_season_id

def get_server_season_info(server: str) -> dict:
    season_id = get_current_season_id(server)
    season_name = SEASONS_MAP[season_id]
    return {
        "id": season_id,
        "name": season_name,
        "display": f"{season_name} ({season_id})"
    }

# Функция списания PayDay
def process_hourly_payday():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Списание PayDay...")
    for srv, data in server_data.items():
        # Дома (1 - страхован, 2 - не страхован)
        updated_houses = []
        for h in data["houses"]:
            decrement = 1 if h.get("status") == "insured" else 2
            h["pd"] -= decrement
            if h["pd"] > 0:
                updated_houses.append(h)
        data["houses"] = sorted(updated_houses, key=lambda x: x["pd"])

        # Бизнесы (1 - страхован, 2 - не страхован, 4 - нет занятости)
        updated_biz = []
        for b in data["businesses"]:
            st = b.get("status", "insured")
            if st == "insured":
                decrement = 1
            elif st == "uninsured":
                decrement = 2
            else:  # no_activity
                decrement = 4
            
            b["pd"] -= decrement
            if b["pd"] > 0:
                updated_biz.append(b)
        data["businesses"] = sorted(updated_biz, key=lambda x: x["pd"])

    save_data_to_file()

# Фоновый цикл
async def hourly_loop():
    while True:
        now = datetime.now()
        seconds_until_next_hour = (60 - now.minute - 1) * 60 + (60 - now.second)
        if seconds_until_next_hour <= 0:
            seconds_until_next_hour = 3600
        
        await asyncio.sleep(seconds_until_next_hour)
        process_hourly_payday()

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(hourly_loop())
    yield
    task.cancel()
    save_data_to_file()

app = FastAPI(title="Arizona Property Tracker API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    propType: str
    pos: int
    status: str

class DeleteItemModel(BaseModel):
    server: str
    propType: str
    pos: int

class AddItemModel(BaseModel):
    server: str
    propType: str
    pd: int
    propId: Optional[int] = None

@app.post("/api/paydays")
async def receive_paydays(payload: Payload, x_secret_key: Optional[str] = Header(None)):
    if x_secret_key != SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Invalid Secret Key"
        )

    srv = payload.server
    if srv not in server_data:
        server_data[srv] = {"houses": [], "businesses": []}

    houses = []
    businesses = []

    for item in payload.entries:
        record = {
            "pd": item.pd,
            "propId": item.propId,
            "pos": item.pos,
            "status": "insured",
            "updatedAt": datetime.utcnow().strftime("%H:%M:%S")
        }
        if item.propType == "house":
            houses.append(record)
        else:
            businesses.append(record)

    if houses:
        server_data[srv]["houses"] = sorted(houses, key=lambda x: x["pd"])
    if businesses:
        server_data[srv]["businesses"] = sorted(businesses, key=lambda x: x["pd"])

    save_data_to_file()
    return {"status": "ok", "count": len(payload.entries)}

@app.post("/api/update_status")
async def update_status(data: UpdateStatusModel):
    srv = data.server
    if srv in server_data:
        target_list = server_data[srv]["houses"] if data.propType in ["house", "houses"] else server_data[srv]["businesses"]
        for item in target_list:
            if item["pos"] == data.pos:
                item["status"] = data.status
                save_data_to_file()
                return {"status": "success"}
    raise HTTPException(status_code=404, detail="Item not found")

@app.post("/api/delete_item")
async def delete_item(data: DeleteItemModel):
    srv = data.server
    if srv in server_data:
        target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
        server_data[srv][target_key] = [item for item in server_data[srv][target_key] if item["pos"] != data.pos]
        save_data_to_file()
        return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server not found")

@app.post("/api/add_item")
async def add_item(data: AddItemModel):
    srv = data.server
    if srv in server_data:
        target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
        
        existing_positions = [item["pos"] for item in server_data[srv][target_key]]
        new_pos = max(existing_positions, default=0) + 1
        
        new_record = {
            "pd": data.pd,
            "propId": data.propId,
            "pos": new_pos,
            "status": "insured",
            "updatedAt": datetime.utcnow().strftime("%H:%M:%S")
        }
        
        server_data[srv][target_key].append(new_record)
        server_data[srv][target_key] = sorted(server_data[srv][target_key], key=lambda x: x["pd"])
        save_data_to_file()
        return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server not found")

@app.get("/api/paydays")
async def get_paydays():
    response_data = {}
    for srv, data in server_data.items():
        response_data[srv] = {
            **data,
            "season": get_server_season_info(srv)
        }
    return response_data

@app.get("/", response_class=HTMLResponse)
async def render_dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Arizona RP — Мониторинг Слётов</title>
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #121212;
                color: #e0e0e0;
                margin: 0;
                padding: 20px;
            }
            h1 {
                text-align: center;
                color: #ff9800;
                margin-bottom: 30px;
            }
            .servers-container {
                display: flex;
                flex-direction: column;
                gap: 15px;
                max-width: 1000px;
                margin: 0 auto;
            }
            .server-card {
                background-color: #1e1e1e;
                border: 1px solid #333;
                border-radius: 8px;
                padding: 15px 20px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.3);
            }
            .server-title {
                font-size: 1.3em;
                font-weight: bold;
                color: #4caf50;
                border-bottom: 1px solid #333;
                padding-bottom: 8px;
                margin-bottom: 12px;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            .season-badge {
                font-size: 0.75em;
                background-color: #332a12;
                color: #ffb74d;
                border: 1px solid #ff9800;
                padding: 3px 8px;
                border-radius: 12px;
                font-weight: normal;
            }
            .tables-grid {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 15px;
            }
            @media (max-width: 768px) {
                .tables-grid { grid-template-columns: 1fr; }
            }
            .section-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 6px;
            }
            .section-title {
                font-size: 0.95em;
                font-weight: bold;
                color: #00bcd4;
            }
            .btn-add {
                background-color: #008cba;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 2px 8px;
                font-size: 0.85em;
                font-weight: bold;
                cursor: pointer;
                transition: 0.2s;
            }
            .btn-add:hover {
                background-color: #005f73;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                font-size: 0.85em;
            }
            th, td {
                padding: 6px 8px;
                text-align: left;
                border-bottom: 1px solid #2a2a2a;
            }
            th { background-color: #252525; color: #aaa; }
            .pd-badge {
                background-color: #e53935;
                color: #fff;
                padding: 2px 6px;
                border-radius: 4px;
                font-weight: bold;
            }
            .btn-group {
                display: flex;
                gap: 3px;
            }
            .btn-opt {
                background-color: #2a2a2a;
                color: #888;
                border: 1px solid #444;
                padding: 3px 6px;
                font-size: 0.75em;
                border-radius: 4px;
                cursor: pointer;
                transition: 0.2s;
            }
            .btn-opt.active-insured {
                background-color: #2e7d32;
                color: #fff;
                border-color: #4caf50;
            }
            .btn-opt.active-uninsured {
                background-color: #c62828;
                color: #fff;
                border-color: #ef5350;
            }
            .btn-opt.active-noact {
                background-color: #b71c1c;
                color: #fff;
                border-color: #ff1744;
                font-weight: bold;
            }
            .btn-del {
                background-color: transparent;
                color: #ef5350;
                border: 1px solid #ef5350;
                padding: 2px 6px;
                font-size: 0.8em;
                border-radius: 4px;
                cursor: pointer;
                transition: 0.2s;
            }
            .btn-del:hover {
                background-color: #ef5350;
                color: #fff;
            }
            .empty { color: #666; font-style: italic; font-size: 0.85em; }
        </style>
        <script>
            async function setStatus(server, propType, pos, status) {
                try {
                    await fetch('/api/update_status', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ server, propType, pos, status })
                    });
                    loadData();
                } catch(e) { console.error(e); }
            }

            async function deleteItem(server, propType, pos) {
                if (!confirm('Удалить эту запись?')) return;
                try {
                    await fetch('/api/delete_item', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ server, propType, pos })
                    });
                    loadData();
                } catch(e) { console.error(e); }
            }

            async function addItemPrompt(server, propType) {
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
                        body: JSON.stringify({ server, propType, pd, propId })
                    });
                    loadData();
                } catch(e) { console.error(e); }
            }

            async function loadData() {
                try {
                    const res = await fetch('/api/paydays');
                    const data = await res.json();
                    
                    for (const [server, info] of Object.entries(data)) {
                        const seasonElem = document.getElementById(`season-${server}`);
                        if (seasonElem && info.season) {
                            seasonElem.innerText = `Сезон: ${info.season.display}`;
                        }

                        const hTable = document.getElementById(`houses-${server}`);
                        const bTable = document.getElementById(`biz-${server}`);
                        
                        if (hTable) {
                            if (info.houses.length === 0) {
                                hTable.innerHTML = '<span class="empty">Нет данных</span>';
                            } else {
                                let html = '<table><tr><th>№</th><th>ID</th><th>PD</th><th>Тип слета</th><th></th></tr>';
                                info.houses.forEach((item, idx) => {
                                    const st = item.status || 'insured';
                                    html += `<tr>
                                        <td>${idx+1}</td>
                                        <td>${item.propId ? '№' + item.propId : '—'}</td>
                                        <td><span class="pd-badge">${item.pd} pd</span></td>
                                        <td>
                                            <div class="btn-group">
                                                <button class="btn-opt ${st === 'insured' ? 'active-insured' : ''}" onclick="setStatus('${server}', 'house', ${item.pos}, 'insured')">Страх.</button>
                                                <button class="btn-opt ${st === 'uninsured' ? 'active-uninsured' : ''}" onclick="setStatus('${server}', 'house', ${item.pos}, 'uninsured')">Не страх.</button>
                                            </div>
                                        </td>
                                        <td>
                                            <button class="btn-del" onclick="deleteItem('${server}', 'house', ${item.pos})">✖</button>
                                        </td>
                                    </tr>`;
                                });
                                html += '</table>';
                                hTable.innerHTML = html;
                            }
                        }

                        if (bTable) {
                            if (info.businesses.length === 0) {
                                bTable.innerHTML = '<span class="empty">Нет данных</span>';
                            } else {
                                let html = '<table><tr><th>№</th><th>ID</th><th>PD</th><th>Тип слета</th><th></th></tr>';
                                info.businesses.forEach((item, idx) => {
                                    const st = item.status || 'insured';
                                    html += `<tr>
                                        <td>${idx+1}</td>
                                        <td>${item.propId ? '№' + item.propId : '—'}</td>
                                        <td><span class="pd-badge">${item.pd} pd</span></td>
                                        <td>
                                            <div class="btn-group">
                                                <button class="btn-opt ${st === 'insured' ? 'active-insured' : ''}" onclick="setStatus('${server}', 'biz', ${item.pos}, 'insured')">Страх.</button>
                                                <button class="btn-opt ${st === 'uninsured' ? 'active-uninsured' : ''}" onclick="setStatus('${server}', 'biz', ${item.pos}, 'uninsured')">Не страх.</button>
                                                <button class="btn-opt ${st === 'no_activity' ? 'active-noact' : ''}" onclick="setStatus('${server}', 'biz', ${item.pos}, 'no_activity')">Без зан.</button>
                                            </div>
                                        </td>
                                        <td>
                                            <button class="btn-del" onclick="deleteItem('${server}', 'biz', ${item.pos})">✖</button>
                                        </td>
                                    </tr>`;
                                });
                                html += '</table>';
                                bTable.innerHTML = html;
                            }
                        }
                    }
                } catch(e) { console.error(e); }
            }
            setInterval(loadData, 5000);
            window.onload = loadData;
        </script>
    </head>
    <body>
        <h1>Arizona RP — Мониторинг Слётов</h1>
        <div class="servers-container">
    """

    for idx, srv in enumerate(ALL_SERVERS, 1):
        season_info = get_server_season_info(srv)
        html_content += f"""
            <div class="server-card">
                <div class="server-title">
                    <span>#{idx:02d} {srv}</span>
                    <span id="season-{srv}" class="season-badge">Сезон: {season_info['display']}</span>
                </div>
                <div class="tables-grid">
                    <div>
                        <div class="section-header">
                            <span class="section-title">🏠 Дома</span>
                            <button class="btn-add" onclick="addItemPrompt('{srv}', 'house')">+ Добавить</button>
                        </div>
                        <div id="houses-{srv}"><span class="empty">Загрузка...</span></div>
                    </div>
                    <div>
                        <div class="section-header">
                            <span class="section-title">🏢 Бизнесы</span>
                            <button class="btn-add" onclick="addItemPrompt('{srv}', 'biz')">+ Добавить</button>
                        </div>
                        <div id="biz-{srv}"><span class="empty">Загрузка...</span></div>
                    </div>
                </div>
            </div>
        """

    html_content += """
        </div>
    </body>
    </html>
    """
    return html_content
