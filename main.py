import asyncio
import contextlib
import json
import os
import zoneinfo
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# --- КОНФИГУРАЦИЯ ---
SECRET_KEY = "usefguIHSFUSDFGUjhjfk88448"
DATA_FILE = "server_data.json"
MSK_TZ = zoneinfo.ZoneInfo("Europe/Moscow")

BASE_WEEK_START = datetime(2026, 9, 21, 5, 0, 0, tzinfo=MSK_TZ)

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
    dependent_scans = [s for s in scans if s.get("hasPair", False)]
    if dependent_scans:
        return dependent_scans[-1]
    return scans[-1] if scans else None

# --- ЛОГИКА PAYDAY ДЛЯ ОБЩЕГО ВИДА ---
async def process_hourly_payday():
    async with data_lock:
        print(f"[{datetime.now(MSK_TZ).strftime('%Y-%m-%d %H:%M:%S')}] Списание PayDay для общего вида...")
        for srv, data in server_data.items():
            scans = data.get("scans", [])
            if not scans:
                continue
            
            latest_scan = get_latest_confirmed_scan(scans)
            if not latest_scan:
                continue

            updated_houses = []
            for h in latest_scan.get("houses", []):
                # Пропускаем списание, если дом еще ждет пару для сравнения
                if h.get("isPendingPair", False):
                    updated_houses.append(h)
                    continue

                decrement = 1 if h.get("status") == "insured" else 2
                h["pd"] -= decrement
                if h["pd"] > 0:
                    updated_houses.append(h)
            latest_scan["houses"] = sorted(updated_houses, key=lambda x: x["pd"])

            updated_biz = []
            for b in latest_scan.get("businesses", []):
                if b.get("isPendingPair", False):
                    updated_biz.append(b)
                    continue

                st = b.get("status", "insured")
                decrement = 1 if st == "insured" else (2 if st == "uninsured" else 4)
                b["pd"] -= decrement
                if b["pd"] > 0:
                    updated_biz.append(b)
            latest_scan["businesses"] = sorted(updated_biz, key=lambda x: x["pd"])

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
    task = asyncio.create_task(hourly_loop())
    yield
    task.cancel()
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

# --- ЭНДПОИНТЫ API ---

@app.post("/api/paydays")
async def receive_paydays(payload: Payload, x_secret_key: Optional[str] = Header(None)):
    if x_secret_key != SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: Invalid Secret Key"
        )

    srv = payload.server
    now_msk = datetime.now(MSK_TZ)
    hour_label = now_msk.strftime("%H:00")
    scan_id = f"{now_msk.strftime('%Y-%m-%d')} {hour_label}"

    async with data_lock:
        if srv not in server_data:
            server_data[srv] = {"scans": []}
        if "scans" not in server_data[srv]:
            server_data[srv]["scans"] = []

        scans = server_data[srv]["scans"]
        
        # Получаем прошлый скан для поиска соответствий
        prev_scan = None
        if len(scans) > 0:
            prev_scan = scans[-1] if scans[-1]["scanId"] != scan_id else (scans[-2] if len(scans) > 1 else None)

        def find_item_in_scan(scan_obj, prop_type: str, prop_id: Optional[int], pos: int):
            if not scan_obj:
                return None
            items = scan_obj["houses"] if prop_type == "house" else scan_obj["businesses"]
            if prop_id is not None:
                for it in items:
                    if it.get("propId") == prop_id:
                        return it
            for it in items:
                if it.get("pos") == pos:
                    return it
            return None

        houses = []
        businesses = []
        is_pair_found = prev_scan is not None

        for item in payload.entries:
            prev_item = find_item_in_scan(prev_scan, item.propType, item.propId, item.pos)
            
            auto_status = "insured"
            is_pending = False

            if prev_item is not None:
                # Если элемент уже был в прошлом скане
                base_pd = prev_item.get("basePd", prev_item["pd"])
                diff = base_pd - item.pd

                if item.propType == "house":
                    auto_status = "uninsured" if diff >= 2 else "insured"
                else: # biz
                    if diff >= 4:
                        auto_status = "no_activity"
                    elif diff >= 2:
                        auto_status = "uninsured"
                    else:
                        auto_status = "insured"
            else:
                # Если элемент ПОЯВИЛСЯ ВПЕРВЫЕ во 2-м (или последующем) скане:
                # Назначаем его "ожидающим третьей точки сканирования"
                is_pending = True

            record = {
                "basePd": item.pd,
                "pd": item.pd,
                "propId": item.propId,
                "pos": item.pos,
                "status": auto_status,
                "isPendingPair": is_pending,
                "updatedAt": now_msk.strftime("%H:%M:%S")
            }
            if item.propType == "house":
                houses.append(record)
            else:
                businesses.append(record)

        houses = sorted(houses, key=lambda x: x["pd"])
        businesses = sorted(businesses, key=lambda x: x["pd"])

        # Обновляем предыдущий скан
        if is_pair_found and prev_scan:
            prev_scan["isConfirmed"] = True
            prev_scan["hasPair"] = True

            # Если в пред. скане были дома, которые ждали пару, снимаем с них этот статус
            for target_key in ["houses", "businesses"]:
                prop_type = "house" if target_key == "houses" else "biz"
                for prev_it in prev_scan.get(target_key, []):
                    curr_it = find_item_in_scan({"houses": houses, "businesses": businesses}, prop_type, prev_it.get("propId"), prev_it["pos"])
                    if curr_it:
                        prev_it["isPendingPair"] = False

        existing_scan = next((s for s in scans if s["scanId"] == scan_id), None)
        if existing_scan:
            existing_scan["houses"] = houses
            existing_scan["businesses"] = businesses
            existing_scan["scanTime"] = now_msk.strftime("%Y-%m-%d %H:%M:%S")
            existing_scan["isConfirmed"] = is_pair_found
            existing_scan["hasPair"] = is_pair_found
        else:
            scans.append({
                "scanId": scan_id,
                "hourLabel": hour_label,
                "scanTime": now_msk.strftime("%Y-%m-%d %H:%M:%S"),
                "houses": houses,
                "businesses": businesses,
                "isConfirmed": is_pair_found,
                "hasPair": is_pair_found
            })

        await save_data_to_file_async()

    return {"status": "ok", "scanId": scan_id, "count": len(payload.entries)}

@app.post("/api/update_status")
async def update_status(data: UpdateStatusModel):
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
async def delete_item(data: DeleteItemModel):
    srv = data.server
    async with data_lock:
        if srv in server_data:
            for scan in server_data[srv].get("scans", []):
                if scan["scanId"] == data.scanId:
                    target_key = "houses" if data.propType in ["house", "houses"] else "businesses"
                    scan[target_key] = [item for item in scan[target_key] if item["pos"] != data.pos]
                    await save_data_to_file_async()
                    return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server or Scan not found")

@app.post("/api/delete_scan")
async def delete_scan(data: DeleteScanModel):
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
async def add_item(data: AddItemModel):
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
                    scan[target_key] = sorted(scan[target_key], key=lambda x: x["pd"])
                    await save_data_to_file_async()
                    return {"status": "success"}
    raise HTTPException(status_code=404, detail="Server or Scan not found")

@app.get("/api/paydays")
async def get_paydays():
    async with data_lock:
        res = {}
        for srv, data in server_data.items():
            scans = data.get("scans", [])
            latest_confirmed = get_latest_confirmed_scan(scans)
            res[srv] = {
                "scans": scans,
                "latestConfirmedScan": latest_confirmed,
                "season": get_server_season_info(srv)
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
        .season-badge { font-size: 0.75em; background-color: #332a12; color: #ffb74d; border: 1px solid #ff9800; padding: 3px 8px; border-radius: 12px; font-weight: normal; }
        
        .scan-tabs-bar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; border-bottom: 1px solid #2a2a2a; padding-bottom: 6px; }
        .scan-tabs-container { display: flex; gap: 6px; overflow-x: auto; }
        .scan-subtab { background-color: #252525; color: #888; border: 1px solid #3a3a3a; padding: 4px 12px; font-size: 0.85em; border-radius: 4px; cursor: pointer; whitespace: nowrap; transition: 0.2s; }
        .scan-subtab.active { background-color: #1e88e5; color: #fff; border-color: #64b5f6; font-weight: bold; }
        .scan-subtab.single { border-color: #ff9800; color: #ffb74d; }
        .scan-subtab:hover:not(.active) { background-color: #333; color: #ddd; }

        .btn-delete-scan { background-color: #b71c1c; color: #fff; border: none; padding: 4px 8px; border-radius: 4px; font-size: 0.75em; font-weight: bold; cursor: pointer; transition: 0.2s; }
        .btn-delete-scan:hover { background-color: #d32f2f; }

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
        .pd-badge-fixed { background-color: #37474f; color: #81d4fa; border: 1px solid #00838f; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
        
        .btn-group { display: flex; gap: 3px; }
        .btn-opt { background-color: #2a2a2a; color: #888; border: 1px solid #444; padding: 3px 6px; font-size: 0.75em; border-radius: 4px; cursor: pointer; transition: 0.2s; }
        .btn-opt.active-insured { background-color: #2e7d32; color: #fff; border-color: #4caf50; }
        .btn-opt.active-uninsured { background-color: #c62828; color: #fff; border-color: #ef5350; }
        .btn-opt.active-noact { background-color: #b71c1c; color: #fff; border-color: #ff1744; font-weight: bold; }
        
        .status-text { font-weight: bold; font-size: 0.85em; padding: 2px 6px; border-radius: 4px; display: inline-block; }
        .status-insured { color: #81c784; }
        .status-uninsured { color: #e57373; }
        .status-noact { color: #ff5252; }
        .status-pending { color: #ffb74d; font-style: italic; }

        .btn-del { background-color: transparent; color: #ef5350; border: 1px solid #ef5350; padding: 2px 6px; font-size: 0.8em; border-radius: 4px; cursor: pointer; transition: 0.2s; }
        .btn-del:hover { background-color: #ef5350; color: #fff; }
        .empty { color: #666; font-style: italic; font-size: 0.85em; }
    </style>
    <script>
        const activeServerScans = {};

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

        function renderTable(items, server, scanId, type, interactive = true) {
            if (!items || items.length === 0) return '<span class="empty">Нет данных</span>';
            
            let html = '<table><tr><th>№</th><th>ID</th><th>PD</th><th>Статус</th>' + (interactive ? '<th></th>' : '') + '</tr>';
            
            items.forEach((item, idx) => {
                const st = item.status || 'insured';
                const isPending = item.isPendingPair;
                const displayPd = interactive ? (item.basePd !== undefined ? item.basePd : item.pd) : item.pd;
                const badgeClass = interactive ? 'pd-badge-fixed' : 'pd-badge';

                let statusControl = '';
                if (interactive) {
                    statusControl = `
                        <div class="btn-group">
                            <button class="btn-opt ${st === 'insured' && !isPending ? 'active-insured' : ''}" onclick="setStatus('${server}', '${scanId}', '${type}', ${item.pos}, 'insured')">Страх.</button>
                            <button class="btn-opt ${st === 'uninsured' && !isPending ? 'active-uninsured' : ''}" onclick="setStatus('${server}', '${scanId}', '${type}', ${item.pos}, 'uninsured')">Не страх.</button>
                            ${type === 'biz' ? `<button class="btn-opt ${st === 'no_activity' && !isPending ? 'active-noact' : ''}" onclick="setStatus('${server}', '${scanId}', 'biz',${item.pos}, 'no_activity')">Без зан.</button>` : ''}
                        </div>
                        ${isPending ? '<span class="status-pending">⏳ Ждет 2-ю точку</span>' : ''}`;
                } else {
                    if (isPending) {
                        statusControl = `<span class="status-pending">⏳ Ожидание сравнения</span>`;
                    } else {
                        let label = st === 'insured' ? 'Страховка' : (st === 'uninsured' ? 'Без страховки' : 'Без занятости');
                        let classNm = st === 'insured' ? 'status-insured' : (st === 'uninsured' ? 'status-uninsured' : 'status-noact');
                        statusControl = `<span class="status-text ${classNm}">${label}</span>`;
                    }
                }

                html += `<tr>
                    <td>${idx + 1}</td>
                    <td>${item.propId ? '№' + item.propId : '—'}</td>
                    <td><span class="${badgeClass}">${displayPd} pd</span></td>
                    <td>${statusControl}</td>
                    ${interactive ? `<td><button class="btn-del" onclick="deleteItem('${server}', '${scanId}', '${type}',${item.pos})">✖</button></td>` : ''}
                </tr>`;
            });
            return html + '</table>';
        }

        async function loadData() {
            try {
                const res = await fetch('/api/paydays');
                const data = await res.json();
                
                for (const [server, info] of Object.entries(data)) {
                    document.querySelectorAll(`.season-badge-${server}`).forEach(elem => {
                        if (info.season) elem.innerText = `Сезон: ${info.season.display}`;
                    });

                    const scans = info.scans || [];
                    const manageTabsElem = document.getElementById(`scan-tabs-${server}`);
                    const delScanBtnElem = document.getElementById(`btn-del-scan-${server}`);
                    
                    if (scans.length === 0) {
                        if (manageTabsElem) manageTabsElem.innerHTML = '<span class="empty">Сканирований нет</span>';
                        if (delScanBtnElem) delScanBtnElem.style.display = 'none';
                        document.getElementById(`houses-manage-${server}`).innerHTML = '<span class="empty">Нет данных</span>';
                        document.getElementById(`biz-manage-${server}`).innerHTML = '<span class="empty">Нет данных</span>';
                        document.getElementById(`houses-view-${server}`).innerHTML = '<span class="empty">Нет данных</span>';
                        document.getElementById(`biz-view-${server}`).innerHTML = '<span class="empty">Нет данных</span>';
                        continue;
                    }

                    if (delScanBtnElem) delScanBtnElem.style.display = 'inline-block';

                    if (!activeServerScans[server] || !scans.some(s => s.scanId === activeServerScans[server])) {
                        activeServerScans[server] = scans[scans.length - 1].scanId;
                    }

                    let tabsHtml = '';
                    scans.forEach(scan => {
                        const isActive = scan.scanId === activeServerScans[server];
                        const label = scan.hourLabel || (scan.scanTime ? scan.scanTime.split(' ')[1].substring(0, 5) : 'Скан');
                        const isSingle = !scan.isConfirmed;
                        const singleBadge = isSingle ? ' ⚠️ (одиночный)' : '';

                        tabsHtml += `<button class="scan-subtab ${isActive ? 'active' : ''} ${isSingle ? 'single' : ''}" onclick="selectScanTab('${server}', '${scan.scanId}')" title="${isSingle ? 'Одиночное сканирование' : 'Подтвержденный скан'}">
                            🕒 ${label}${singleBadge}
                        </button>`;
                    });
                    if (manageTabsElem) manageTabsElem.innerHTML = tabsHtml;

                    const selectedScan = scans.find(s => s.scanId === activeServerScans[server]) || scans[scans.length - 1];
                    const hManage = document.getElementById(`houses-manage-${server}`);
                    const bManage = document.getElementById(`biz-manage-${server}`);
                    if (hManage) hManage.innerHTML = renderTable(selectedScan.houses, server, selectedScan.scanId, 'house', true);
                    if (bManage) bManage.innerHTML = renderTable(selectedScan.businesses, server, selectedScan.scanId, 'biz', true);

                    const viewScan = info.latestConfirmedScan;
                    const hView = document.getElementById(`houses-view-${server}`);
                    const bView = document.getElementById(`biz-view-${server}`);
                    if (viewScan) {
                        if (hView) hView.innerHTML = renderTable(viewScan.houses, server, viewScan.scanId, 'house', false);
                        if (bView) bView.innerHTML = renderTable(viewScan.businesses, server, viewScan.scanId, 'biz', false);
                    } else {
                        if (hView) hView.innerHTML = '<span class="empty">Ожидание 2-го сканирования...</span>';
                        if (bView) bView.innerHTML = '<span class="empty">Ожидание 2-го сканирования...</span>';
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
        
        card_manage = f"""
        <div class="server-card">
            <div class="server-header">
                <div class="server-title">
                    <span>#{idx:02d} {srv}</span>
                    <span class="season-badge season-badge-{srv}">Сезон: {season_info['display']}</span>
                </div>
            </div>
            
            <div class="scan-tabs-bar">
                <div id="scan-tabs-{srv}" class="scan-tabs-container">
                    <span class="empty">Загрузка...</span>
                </div>
                <button id="btn-del-scan-{srv}" class="btn-delete-scan" onclick="deleteWholeScan('{srv}')">🗑️ Удалить скан</button>
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

        card_view = f"""
        <div class="server-card">
            <div class="server-header">
                <div class="server-title">
                    <span>#{idx:02d} {srv}</span>
                    <span class="season-badge season-badge-{srv}">Сезон: {season_info['display']}</span>
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
