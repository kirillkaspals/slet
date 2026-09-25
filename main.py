from fastapi import FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

app = FastAPI(title="Arizona Property Tracker API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SECRET_KEY = "usefguIHSFUSDFGUjhjfk88448"

# Список всех 33 серверов Arizona RP по порядку
ALL_SERVERS = [
    "Phoenix", "Tucson", "Scottdale", "Chandler", "Brainburg",
    "Saint-Rose", "Mesa", "Red-Rock", "Yuma", "Surprise",
    "Prescott", "Glendale", "Kingman", "Winslow", "Payson",
    "Gilbert", "Show Low", "Casa-Grande", "Page", "Sun-City",
    "Queen-Creek", "Sedona", "Holiday", "Wednesday", "Yava",
    "Faraway", "Bumble Bee", "Christmas", "Love", "Mirage",
    "Drake", "Space", "Home"
]

# Хранилище данных: { "Saint-Rose": { "houses": [...], "businesses": [...] } }
server_data = {
    srv: {"houses": [], "businesses": []} for srv in ALL_SERVERS
}

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
            "updatedAt": datetime.utcnow().strftime("%H:%M:%S")
        }
        if item.propType == "house":
            houses.append(record)
        else:
            businesses.append(record)

    # Обновляем списки по серверу (сортируем по PayDay)
    if houses:
        server_data[srv]["houses"] = sorted(houses, key=lambda x: x["pd"])
    if businesses:
        server_data[srv]["businesses"] = sorted(businesses, key=lambda x: x["pd"])

    return {"status": "ok", "count": len(payload.entries)}

@app.get("/api/paydays")
async def get_paydays():
    return server_data

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
                max-width: 900px;
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
            }
            .tables-grid {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 15px;
            }
            @media (max-width: 600px) {
                .tables-grid { grid-template-columns: 1fr; }
            }
            .section-title {
                font-size: 0.95em;
                font-weight: bold;
                margin-bottom: 6px;
                color: #00bcd4;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                font-size: 0.9em;
            }
            th, td {
                padding: 6px 10px;
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
            .empty { color: #666; font-style: italic; font-size: 0.85em; }
        </style>
        <script>
            async function loadData() {
                try {
                    const res = await fetch('/api/paydays');
                    const data = await res.json();
                    
                    for (const [server, info] of Object.entries(data)) {
                        const hTable = document.getElementById(`houses-${server}`);
                        const bTable = document.getElementById(`biz-${server}`);
                        
                        if (hTable) {
                            if (info.houses.length === 0) {
                                hTable.innerHTML = '<span class="empty">Нет данных</span>';
                            } else {
                                let html = '<table><tr><th>№</th><th>ID Дома</th><th>PayDay</th></tr>';
                                info.houses.forEach((item, idx) => {
                                    html += `<tr><td>${idx+1}</td><td>${item.propId ? '№' + item.propId : '—'}</td><td><span class="pd-badge">${item.pd} pd</span></td></tr>`;
                                });
                                html += '</table>';
                                hTable.innerHTML = html;
                            }
                        }

                        if (bTable) {
                            if (info.businesses.length === 0) {
                                bTable.innerHTML = '<span class="empty">Нет данных</span>';
                            } else {
                                let html = '<table><tr><th>№</th><th>ID Бизнеса</th><th>PayDay</th></tr>';
                                info.businesses.forEach((item, idx) => {
                                    html += `<tr><td>${idx+1}</td><td>${item.propId ? '№' + item.propId : '—'}</td><td><span class="pd-badge">${item.pd} pd</span></td></tr>`;
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
        html_content += f"""
            <div class="server-card">
                <div class="server-title">
                    <span>#{idx} {srv}</span>
                </div>
                <div class="tables-grid">
                    <div>
                        <div class="section-title">🏠 Дома</div>
                        <div id="houses-{srv}"><span class="empty">Загрузка...</span></div>
                    </div>
                    <div>
                        <div class="section-title">🏢 Бизнесы</div>
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
