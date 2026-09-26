import asyncio
import contextlib
import json
import os
import zoneinfo
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

# --- КОНФИГУРАЦИЯ ---
SECRET_KEY = "usefguIHSFUSDFGUjhjfk88448"
DATA_FILE = "server_data.json"
MSK_TZ = zoneinfo.ZoneInfo("Europe/Moscow")

# Понедельник перед слётом (21 сентября 2026, 05:00 МСК)
BASE_WEEK_START = datetime(2026, 9, 21, 5, 0, 0, tzinfo=MSK_TZ)

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
    "Show Low": 5, "Casa-Grande": 5, "Page": 1, "Sun-City": 3,
    "Queen-Creek": 5, "Sedona": 1, "Holiday": 4, "Wednesday": 2,
    "Yava": 2, "Faraway": 2, "Bumble Bee": 5, "Christmas": 2,
    "Love": 2, "Mirage": 2, "Drake": 2, "Space": 5, "Home": 1
}

# Мьютекс для безопасной работы с JSON из разных потоков/задач
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


# --- РАБОТА С ФАЙЛАМИ И ДАННЫМИ ---
def load_data_from_file() -> Dict[str, dict]:
    data_store = {srv: {"houses": [], "businesses": [], "lastScanTime": None} for srv in ALL_SERVERS}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for srv in ALL_SERVERS:
                    if srv in loaded:
                        data_store[srv] = loaded[srv]
                        if "lastScanTime" not in data_store[srv]:
                            data_store[srv]["lastScanTime"] = None
            print("Данные успешно загружены из файла.")
        except Exception as e:
            print(f"Ошибка чтения JSON-файла: {e}")
    return data_store

server_data = load_data_from_file()

async def save_data_to_file_async():
    """Асинхронное сохранение данных во избежание блокировки event loop."""
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

# --- ЛОГИКА PAYDAY ---
async def process_hourly_payday():
    async with data_lock:
        print(f"[{datetime.now(MSK_TZ).strftime('%Y-%m-%d %H:%M:%S')}] Выполнение списания PayDay...")
        for srv, data in server_data.items():
            # Дома (insured: -1, uninsured: -2)
            updated_houses = []
            for h in data["houses"]:
                decrement = 1 if h.get("status") == "insured" else 2
                h["pd"] -= decrement
                if h["pd"] > 0:
                    updated_houses.append(h)
            data["houses"] = sorted(updated_houses, key=lambda x: x["pd"])

            # Бизнесы (insured: -1, uninsured: -2, no_activity: -4)
            updated_biz = []
            for b in data["businesses"]:
                st = b.get("status", "insured")
                decrement = 1 if st == "insured" else (2 if st == "uninsured" else 4)
                b["pd"] -= decrement
                if b["pd"] > 0:
                    updated_biz.append(b)
            data["businesses"] = sorted(updated_biz, key=lambda x: x["pd"])

        await save_data_to_file_async()

async def hourly_loop():
    """Фоновый таймер, срабатывающий строго в начале каждого часа."""
    while True:
        now = datetime.now(MSK_TZ)
        seconds_until_next_hour = (60 - now.minute - 1) * 60 + (60 - now.second)
        if seconds_until_next_hour <= 0:
            seconds_until_next_hour = 3600
        
        await asyncio.sleep(seconds_until_next_hour)
        await process_hourly_payday()

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(hourly_loop())
    yield
    task.cancel()
    async with data_lock:
        await save_data_to_file_async()

# --- ИНИЦИАЛИЗАЦИЯ FASTAPI ---
app = FastAPI(title="Arizona Property Tracker API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ЭНДПОИНТЫ API ---

@app.post("/api/paydays")
async def receive_paydays(payload: Payload, x_secret_key: Optional[str] = Header(None)):
    if x_secret_key != SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Invalid Secret Key"
        )

    srv = payload.server
    scan_time = datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S")

    async with data_lock:
        if srv not in server_data:
            server_data[srv] = {"houses": [], "businesses": [], "lastScanTime": scan_time}
        
        server_data[srv]["lastScanTime"] = scan_time

        for item in payload.entries:
            record = {
                "pd": item.pd,
                "propId": item.propId,
                "pos": item.pos,
                "status": "insured",
                "updatedAt": datetime.now(MSK_TZ).strftime("%H:%M:%S")
            }
            target_key = "houses" if item.propType == "house" else "businesses"
            existing_list = server_data[srv][target_key]
            
            updated = False
            for idx, existing_item in enumerate(existing_list):
                if existing_item["pos"] == item.pos:
                    record["status"] = existing_item.get("status", "insured")
                    existing_list[idx] = record
                    updated = True
                    break
            
            if not updated:
                existing_list.append(record)

            server_data[srv][target_key] = sorted(existing_list, key=lambda x: x["pd"])

        await save_data_to_file_async()
    return {"status": "ok", "count": len(payload.entries), "scanTime": scan_time}

@app.post("/api/update_status")
async def update_status(data: UpdateStatusModel):
    srv = data.server
    async with data_lock:
        if srv in server_data:
            target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
            for item in server_data[srv][target_key]:
                if item["pos"] == data.pos:
                    item["status"] = data.status
                    await save_data_to_file_async()
                    return {"status": "success"}
    raise HTTPException(status_code=404, detail="Item or Server not found")

@app.post("/api/delete_item")
async def delete_item(data: DeleteItemModel):
    srv = data.server
    async with data_lock:
        if srv in server_data:
            target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
            server_data[srv][target_key] = [
                item for item in server_data[srv][target_key] if item["pos"] != data.pos
            ]
            await save_data_to_file_async()
            return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server not found")

@app.post("/api/add_item")
async def add_item(data: AddItemModel):
    srv = data.server
    async with data_lock:
        if srv in server_data:
            target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
            existing_positions = [item["pos"] for item in server_data[srv][target_key]]
            new_pos = max(existing_positions, default=0) + 1
            
            new_record = {
                "pd": data.pd,
                "propId": data.propId,
                "pos": new_pos,
                "status": "insured",
                "updatedAt": datetime.now(MSK_TZ).strftime("%H:%M:%S")
            }
            
            server_data[srv][target_key].append(new_record)
            server_data[srv][target_key] = sorted(server_data[srv][target_key], key=lambda x: x["pd"])
            await save_data_to_file_async()
            return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server not found")

@app.get("/api/paydays")
async def get_paydays():
    async with data_lock:
        return {
            srv: {
                **data,
                "season": get_server_season_info(srv)
            }
            for srv, data in server_data.items()
        }

# --- ДАШБОРД (ИНТЕРФЕЙС) ---
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
        
        /* Вкладки */
        .tabs { display: flex; justify-content: center; gap: 10px; margin-bottom: 25px; }
        .tab-btn { background-color: #1e1e1e; color: #aaa; border: 1px solid #333; padding: 10px 24px; font-size: 1em; font-weight: bold; border-radius: 6px; cursor: pointer; transition: 0.2s; }
        .tab-btn.active { background-color: #ff9800; color: #121212; border-color: #ff9800; }
        .tab-btn:hover:not(.active) { background-color: #2a2a2a; color: #fff; }

        .tab-content { display: none; }
        .tab-content.active { display: block; }

        .servers-container { display: flex; flex-direction: column; gap: 15px; max-width: 1000px; margin: 0 auto; }
        .server-card { background-color: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 15px 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
        .server-header { border-bottom: 1px solid #333; padding-bottom: 8px; margin-bottom: 12px; }
        .server-title { font-size: 1.3em; font-weight: bold; color: #4caf50; display: flex; justify-content: space-between; align-items: center; }
        .server-meta { display: flex; gap: 10px; align-items: center; margin-top: 4px; font-size: 0.85em; }
        .scan-time-badge { background-color: #1a2733; color: #64b5f6; border: 1px solid #1e88e5; padding: 2px 8px; border-radius: 4px; font-weight: 500; }
        .season-badge { font-size: 0.75em; background-color: #332a12; color: #ffb74d; border: 1px solid #ff9800; padding: 3px 8px; border-radius: 12px; font-weight: normal; }
        
        .tables-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
        @media (max-width: 768px) { .tables-grid { grid-template-columns: 1fr; } }
        .section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
        .section-title { font-size: 0.95em; font-weight: bold; color: #00bcd4; }
        .btn-add { background-color: #008cba; color: white; border: none; border-radius: 4px; padding: 2px 8px; font-size: 0.85em; font-weight: bold; cursor: pointer; transition: 0.2s; }
        .btn-add:hover { background-color: #005f73; }
        table { width: 100%; border-collapse: collapse; font-size: 0.85em; }
        th, td { padding: 6px 8px; text-align: left; border-bottom: 1px solid #2a2a2a; }
        th { background-color: #252525; color: #aaa; }
        .pd-badge { background-color: #e53935; color: #fff; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
        
        .btn-group { display: flex; gap: 3px; }
        .btn-opt { background-color: #2a2a2a; color: #888; border: 1px solid #444; padding: 3px 6px; font-size: 0.75em; border-radius: 4px; cursor: pointer; transition: 0.2s; }
        .btn-opt.active-insured { background-color: #2e7d32; color: #fff; border-color: #4caf50; }
        .btn-opt.active-uninsured { background-color: #c62828; color: #fff; border-color: #ef5350; }
        .btn-opt.active-noact { background-color: #b71c1c; color: #fff; border-color: #ff1744; font-weight: bold; }
        
        .status-text { font-weight: bold; font-size: 0.85em; padding: 2px 6px; border-radius: 4px; display: inline-block; }
        .status-insured { color: #81c784; }
        .status-uninsured { color: #e57373; }
        .status-noact { color: #ff5252; }

        .btn-del { background-color: transparent; color: #ef5350; border: 1px solid #ef5350; padding: 2px 6px; font-size: 0.8em; border-radius: 4px; cursor: pointer; transition: 0.2s; }
        .btn-del:hover { background-color: #ef5350; color: #fff; }
        .empty { color: #666; font-style: italic; font-size: 0.85em; }
    </style>
    <script>
        function switchTab(tabName) {
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            
            if (tabName === 'manage') {
                document.getElementById('btn-tab-manage').classList.add('active');
                document.getElementById('tab-manage').classList.add('active');
            } else {
                document.getElementById('btn-tab-view').classList.add('active');
                document.getElementById('tab-view').classList.add('active');
            }
        }

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

        function renderTable(items, server, type, interactive = true) {
            if (!items || items.length === 0) return '<span class="empty">Нет данных</span>';
            
            let html = '<table><tr><th>№</th><th>ID</th><th>PD</th><th>Статус</th>' + (interactive ? '<th></th>' : '') + '</tr>';
            
            items.forEach((item, idx) => {
                const st = item.status || 'insured';
                
                let statusControl = '';
                if (interactive) {
                    statusControl = `
                        <div class="btn-group">
                            <button class="btn-opt ${st === 'insured' ? 'active-insured' : ''}" onclick="setStatus('${server}', '${type}', ${item.pos}, 'insured')">Страх.</button>
                            <button class="btn-opt ${st === 'uninsured' ? 'active-uninsured' : ''}" onclick="setStatus('${server}', '${type}', ${item.pos}, 'uninsured')">Не страх.</button>
                            ${type === 'biz' ? `<button class="btn-opt ${st === 'no_activity' ? 'active-noact' : ''}" onclick="setStatus('${server}', 'biz',${item.pos}, 'no_activity')">Без зан.</button>` : ''}
                        </div>`;
                } else {
                    let label = st === 'insured' ? 'Страховка' : (st === 'uninsured' ? 'Без страховки' : 'Без занятости');
                    let classNm = st === 'insured' ? 'status-insured' : (st === 'uninsured' ? 'status-uninsured' : 'status-noact');
                    statusControl = `<span class="status-text ${classNm}">${label}</span>`;
                }

                html += `<tr>
                    <td>${idx + 1}</td>
                    <td>${item.propId ? '№' + item.propId : '—'}</td>
                    <td><span class="pd-badge">${item.pd} pd</span></td>
                    <td>${statusControl}</td>
                    ${interactive ? `<td><button class="btn-del" onclick="deleteItem('${server}', '${type}',${item.pos})">✖</button></td>` : ''}
                </tr>`;
            });
            return html + '</table>';
        }

        async function loadData() {
            try {
                const res = await fetch('/api/paydays');
                const data = await res.json();
                
                for (const [server, info] of Object.entries(data)) {
                    // Обновление сезонов
                    document.querySelectorAll(`.season-badge-${server}`).forEach(elem => {
                        if (info.season) elem.innerText = `Сезон: ${info.season.display}`;
                    });

                    // Обновление времени сканирования
                    document.querySelectorAll(`.scan-time-${server}`).forEach(elem => {
                        elem.innerText = info.lastScanTime ? `Сканирование: ${info.lastScanTime}` : 'Сканирование: Нет данных';
                    });

                    // Таблицы управления
                    const hManage = document.getElementById(`houses-manage-${server}`);
                    const bManage = document.getElementById(`biz-manage-${server}`);
                    if (hManage) hManage.innerHTML = renderTable(info.houses, server, 'house', true);
                    if (bManage) bManage.innerHTML = renderTable(info.businesses, server, 'biz', true);

                    // Таблицы просмотра
                    const hView = document.getElementById(`houses-view-${server}`);
                    const bView = document.getElementById(`biz-view-${server}`);
                    if (hView) hView.innerHTML = renderTable(info.houses, server, 'house', false);
                    if (bView) bView.innerHTML = renderTable(info.businesses, server, 'biz', false);
                }
            } catch(e) { console.error(e); }
        }

        setInterval(loadData, 5000);
        window.onload = loadData;
    </script>
</head>
<body>
    <h1>Arizona RP — Мониторинг Слётов</h1>
    
    <div class="tabs">
        <button id="btn-tab-manage" class="tab-btn active" onclick="switchTab('manage')">🎛️ Управление</button>
        <button id="btn-tab-view" class="tab-btn" onclick="switchTab('view')">📊 Общий вид (Только чтение)</button>
    </div>

    <!-- Вкладка: Управление -->
    <div id="tab-manage" class="tab-content active">
        <div class="servers-container">
            <!-- CARDS_MANAGE_PLACEHOLDER -->
        </div>
    </div>

    <!-- Вкладка: Общий вид -->
    <div id="tab-view" class="tab-content">
        <div class="servers-container">
            <!-- CARDS_VIEW_PLACEHOLDER -->
        </div>
    </div>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def render_dashboard():
    manage_cards = []
    view_cards = []

    for idx, srv in enumerate(ALL_SERVERS, 1):
        season_info = get_server_season_info(srv)
        
        # Карточка для интерактивного управления
        card_manage = f"""
        <div class="server-card">
            <div class="server-header">
                <div class="server-title">
                    <span>#{idx:02d} {srv}</span>
                    <span class="season-badge season-badge-{srv}">Сезон: {season_info['display']}</span>
                </div>
                <div class="server-meta">
                    <span class="scan-time-badge scan-time-{srv}">Сканирование: Загрузка...</span>
                </div>
            </div>
            <div class="tables-grid">
                <div>
                    <div class="section-header">
                        <span class="section-title">🏠 Дома</span>
                        <button class="btn-add" onclick="addItemPrompt('{srv}', 'house')">+ Добавить</button>
                    </div>
                    <div id="houses-manage-{srv}"><span class="empty">Загрузка...</span></div>
                </div>
                <div>
                    <div class="section-header">
                        <span class="section-title">🏢 Бизнесы</span>
                        <button class="btn-add" onclick="addItemPrompt('{srv}', 'biz')">+ Добавить</button>
                    </div>
                    <div id="biz-manage-{srv}"><span class="empty">Загрузка...</span></div>
                </div>
            </div>
        </div>
        """
        manage_cards.append(card_manage)

        # Карточка для чистого просмотра без кнопок
        card_view = f"""
        <div class="server-card">
            <div class="server-header">
                <div class="server-title">
                    <span>#{idx:02d} {srv}</span>
                    <span class="season-badge season-badge-{srv}">Сезон: {season_info['display']}</span>
                </div>
                <div class="server-meta">
                    <span class="scan-time-badge scan-time-{srv}">Сканирование: Загрузка...</span>
                </div>
            </div>
            <div class="tables-grid">
                <div>
                    <div class="section-header">
                        <span class="section-title">🏠 Дома</span>
                    </div>
                    <div id="houses-view-{srv}"><span class="empty">Загрузка...</span></div>
                </div>
                <div>
                    <div class="section-header">
                        <span class="section-title">🏢 Бизнесы</span>
                    </div>
                    <div id="biz-view-{srv}"><span class="empty">Загрузка...</span></div>
                </div>
            </div>
        </div>
        """
        view_cards.append(card_view)

    html = DASHBOARD_HTML.replace("<!-- CARDS_MANAGE_PLACEHOLDER -->", "\n".join(manage_cards))
    html = html.replace("<!-- CARDS_VIEW_PLACEHOLDER -->", "\n".join(view_cards))
    return html
