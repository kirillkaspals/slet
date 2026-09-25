import datetime
from typing import List, Optional
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from fastapi.responses import HTMLResponse

app = FastAPI(title="Arizona Slet Scanner")

# Словарь для перевода названий серверов в числовой ID для сайта
SERVER_NAME_TO_ID = {
    "Phoenix": 1, "Tucson": 2, "Scottdale": 3, "Chandler": 4, "Brainburg": 5,
    "Saint-Rose": 6, "Mesa": 7, "Red-Rock": 8, "Yuma": 9, "Surprise": 10,
    "Prescott": 11, "Glendale": 12, "Kingman": 13, "Winslow": 14, "Payson": 15,
    "Gilbert": 16, "Show Low": 17, "Casa-Grande": 18, "Page": 19, "Sun-City": 20,
    "Queen-Creek": 21, "Sedona": 22, "Holiday": 23, "Wednesday": 24, "Yava": 25,
    "Faraway": 26, "Bumble Bee": 27, "Christmas": 28, "Love": 29, "Mirage": 30,
    "Drake": 31, "Space": 32, "Home": 33
}

# Временная база данных в памяти для всех 33 серверов
database = {
    "servers": {s_id: {"houses": [], "biz": [], "last_scan": None} for s_id in SERVER_NAME_TO_ID.values()}
}

SECRET_KEY = "SECRET_KEY_12345"

class PropertyEntry(BaseModel):
    propType: str
    pd: int
    propId: Optional[int] = None
    pos: Optional[int] = None

class Payload(BaseModel):
    server: str
    scanner: Optional[str] = "unknown"
    entries: List[PropertyEntry]

# --- ЭНДПОИНТ ПРИЕМА ДАННЫХ ОТ СКРИПТА ---
@app.post("/api/paydays")
async def receive_paydays(data: Payload, x_secret_key: str = Header(None)):
    if x_secret_key != SECRET_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    
    server_name = data.server
    server_id = SERVER_NAME_TO_ID.get(server_name)
    
    if not server_id:
        raise HTTPException(status_code=400, detail=f"Unknown server name: {server_name}")

    houses = []
    biz = []

    for item in data.entries:
        entry_data = {
            "id": item.propId if item.propId is not None else item.pos,  # Если ID нет, подставляем позицию в списке
            "paydays": item.pd
        }
        if item.propType == "house":
            houses.append(entry_data)
        elif item.propType == "business":
            biz.append(entry_data)

    # Сохраняем в базу данных
    database["servers"][server_id]["houses"] = houses
    database["servers"][server_id]["biz"] = biz
    database["servers"][server_id]["last_scan"] = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    return {"status": "ok", "received": len(data.entries)}

# --- ЭНДПОИНТ ОБЩЕЙ СВОДКИ ---
@app.get("/api/summary")
async def get_summary():
    summary = []
    for name, srv_id in SERVER_NAME_TO_ID.items():
        srv_data = database["servers"][srv_id]
        summary.append({
            "server_id": srv_id,
            "server_name": name,
            "last_scan": srv_data["last_scan"] or "Нет данных",
            "houses_count": len(srv_data["houses"]),
            "biz_count": len(srv_data["biz"])
        })
    return summary

# --- ЭНДПОИНТ ДАННЫХ КОНКРЕТНОГО СЕРВЕРА ---
@app.get("/api/server/{server_id}")
async def get_server_data(server_id: int):
    if server_id not in database["servers"]:
        raise HTTPException(status_code=404, detail="Server not found")
    
    srv_data = database["servers"][server_id]
    # Находим имя сервера по ID
    server_name = next((k for k, v in SERVER_NAME_TO_ID.items() if v == server_id), "Unknown")
    
    return {
        "server_id": server_id,
        "server_name": server_name,
        "last_scan": srv_data["last_scan"],
        "houses": sorted(srv_data["houses"], key=lambda x: x["paydays"]),
        "biz": sorted(srv_data["biz"], key=lambda x: x["paydays"])
    }

# --- ИНТЕРФЕЙС САЙТА ---
@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>Arizona Slet Scanner</title>
        <style>
            body { font-family: Arial, sans-serif; background: #1a1a1a; color: #fff; margin: 20px; }
            .nav-tabs { display: flex; flex-wrap: wrap; gap: 5px; border-bottom: 2px solid #333; padding-bottom: 10px; }
            .tab-btn { background: #2a2a2a; color: #fff; border: 1px solid #444; padding: 8px 15px; cursor: pointer; border-radius: 4px; }
            .tab-btn.active { background: #007bff; border-color: #0056b3; }
            .tab-btn.summary-btn { background: #28a745; }
            .content-area { margin-top: 20px; }
            .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 15px; }
            .card { background: #2a2a2a; padding: 15px; border-radius: 8px; border: 1px solid #333; }
            .card h3 { margin-top: 0; color: #007bff; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th, td { border: 1px solid #333; padding: 8px; text-align: left; }
            th { background: #252525; }
        </style>
    </head>
    <body>
        <h1>Arizona RP — Сканер Слётов</h1>
        <div class="nav-tabs" id="tabs-header">
            <button class="tab-btn summary-btn active" onclick="loadSummary()">📊 Общая сводка</button>
        </div>
        <div class="content-area" id="content"></div>

        <script>
            const SERVER_NAMES = {
                1: "Phoenix", 2: "Tucson", 3: "Scottdale", 4: "Chandler", 5: "Brainburg",
                6: "Saint-Rose", 7: "Mesa", 8: "Red-Rock", 9: "Yuma", 10: "Surprise",
                11: "Prescott", 12: "Glendale", 13: "Kingman", 14: "Winslow", 15: "Payson",
                16: "Gilbert", 17: "Show Low", 18: "Casa-Grande", 19: "Page", 20: "Sun-City",
                21: "Queen-Creek", 22: "Sedona", 23: "Holiday", 24: "Wednesday", 25: "Yava",
                26: "Faraway", 27: "Bumble Bee", 28: "Christmas", 29: "Love", 30: "Mirage",
                31: "Drake", 32: "Space", 33: "Home"
            };

            function initTabs() {
                const header = document.getElementById('tabs-header');
                for (let id in SERVER_NAMES) {
                    const btn = document.createElement('button');
                    btn.className = 'tab-btn';
                    btn.innerText = `${id}. ${SERVER_NAMES[id]}`;
                    btn.onclick = () => loadServerData(id, btn);
                    header.appendChild(btn);
                }
            }

            function setActiveTab(element) {
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                element.classList.add('active');
            }

            async function loadSummary() {
                setActiveTab(document.querySelector('.summary-btn'));
                const res = await fetch('/api/summary');
                const data = await res.json();
                let html = `<h2>📊 Общая сводка по серверам</h2><div class="grid">`;
                data.forEach(srv => {
                    html += `
                        <div class="card">
                            <h3>${srv.server_id}. ${srv.server_name}</h3>
                            <p><b>Скан:</b> ${srv.last_scan}</p>
                            <p>🏠 Домов: <b>${srv.houses_count}</b></p>
                            <p>🏢 Бизнесов: <b>${srv.biz_count}</b></p>
                        </div>
                    `;
                });
                html += `</div>`;
                document.getElementById('content').innerHTML = html;
            }

            async function loadServerData(serverId, btnElement) {
                setActiveTab(btnElement);
                const res = await fetch(`/api/server/${serverId}`);
                const data = await res.json();

                let html = `<h2>Сервер: ${data.server_name} (${data.server_id})</h2>`;
                html += `<p>Последнее обновление: <b>${data.last_scan || 'Нет данных'}</b></p>`;
                html += `<h3>🏠 Дома</h3>` + renderTable(data.houses);
                html += `<h3>🏢 Бизнесы</h3>` + renderTable(data.biz);
                document.getElementById('content').innerHTML = html;
            }

            function renderTable(items) {
                if (!items || items.length === 0) return `<p>Нет данных</p>`;
                let t = `<table><tr><th>ID / Номер в списке</th><th>Осталось PayDay</th></tr>`;
                items.forEach(i => {
                    t += `<tr><td>№${i.id}</td><td><b>${i.paydays} PD</b></td></tr>`;
                });
                return t + `</table>`;
            }

            initTabs();
            loadSummary();
        </script>
    </body>
    </html>
    """
