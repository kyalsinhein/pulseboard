"""PulseBoard: async uptime & latency monitor with live SSE dashboard."""
import asyncio, json, os, random, sqlite3, time
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, HttpUrl

DB = os.getenv("DB_PATH", "pulseboard.db")
INTERVAL = int(os.getenv("CHECK_INTERVAL", "10"))
PORT = os.getenv("PORT", "8000")
subscribers: set[asyncio.Queue] = set()

def db():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def init():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS targets(id INTEGER PRIMARY KEY, name TEXT, url TEXT UNIQUE);
        CREATE TABLE IF NOT EXISTS checks(target_id INT, ts REAL, ok INT, ms REAL, status INT);
        CREATE TABLE IF NOT EXISTS events(ts REAL, target_id INT, kind TEXT);
        CREATE INDEX IF NOT EXISTS ix ON checks(target_id, ts);""")
        if not c.execute("SELECT 1 FROM targets").fetchone():
            seeds = [("Python.org", "https://www.python.org"),
                     ("GitHub", "https://github.com"),
                     ("Wikipedia", "https://www.wikipedia.org"),
                     ("Flaky demo service", f"http://127.0.0.1:{PORT}/api/demo/flaky")]
            c.executemany("INSERT INTO targets(name,url) VALUES(?,?)", seeds)

def percentile(vals, p):
    if not vals: return None
    vals = sorted(vals)
    return round(vals[min(len(vals) - 1, int(len(vals) * p))], 1)

def summary(c, t):
    rows = c.execute("SELECT ts,ok,ms FROM checks WHERE target_id=? ORDER BY ts DESC LIMIT 60", (t["id"],)).fetchall()
    day = c.execute("SELECT AVG(ok) a, COUNT(*) n FROM checks WHERE target_id=? AND ts>?",
                    (t["id"], time.time() - 86400)).fetchone()
    oks = [r["ms"] for r in rows if r["ok"]]
    return {"id": t["id"], "name": t["name"], "url": t["url"],
            "up": bool(rows[0]["ok"]) if rows else None,
            "uptime": round(day["a"] * 100, 2) if day["n"] else None,
            "p50": percentile(oks, .5), "p95": percentile(oks, .95),
            "history": [{"ok": r["ok"], "ms": r["ms"]} for r in reversed(rows)]}

async def check(client, t):
    start = time.perf_counter()
    try:
        r = await client.get(t["url"], timeout=8, follow_redirects=True)
        ok, status = int(r.status_code < 400), r.status_code
    except Exception:
        ok, status = 0, 0
    ms = round((time.perf_counter() - start) * 1000, 1)
    now = time.time()
    with db() as c:
        prev = c.execute("SELECT ok FROM checks WHERE target_id=? ORDER BY ts DESC LIMIT 1", (t["id"],)).fetchone()
        c.execute("INSERT INTO checks VALUES(?,?,?,?,?)", (t["id"], now, ok, ms, status))
        c.execute("DELETE FROM checks WHERE ts<?", (now - 7 * 86400,))
        event = None
        if prev is not None and prev["ok"] != ok:
            event = {"ts": now, "target": t["name"], "kind": "up" if ok else "down"}
            c.execute("INSERT INTO events VALUES(?,?,?)", (now, t["id"], event["kind"]))
        data = summary(c, t)
    msg = json.dumps({"target": data, "event": event, "ms": ms, "ok": ok})
    for q in list(subscribers):
        q.put_nowait(msg)

async def monitor():
    async with httpx.AsyncClient(headers={"User-Agent": "PulseBoard/1.0"}) as client:
        while True:
            with db() as c:
                targets = c.execute("SELECT * FROM targets").fetchall()
            await asyncio.gather(*(check(client, t) for t in targets))
            await asyncio.sleep(INTERVAL)

@asynccontextmanager
async def lifespan(app):
    init()
    task = asyncio.create_task(monitor())
    yield
    task.cancel()

app = FastAPI(title="PulseBoard", lifespan=lifespan)

class NewTarget(BaseModel):
    name: str
    url: HttpUrl

@app.get("/health")
def health(): return {"status": "ok"}

@app.get("/api/demo/flaky")
async def flaky():
    """Deliberately unreliable endpoint so incidents appear without external outages."""
    await asyncio.sleep(random.choice([0.05, 0.1, 0.2, 0.9]))
    if random.random() < 0.25: raise HTTPException(503, "simulated outage")
    return {"ok": True}

@app.get("/api/targets")
def targets():
    with db() as c:
        return [summary(c, t) for t in c.execute("SELECT * FROM targets")]

@app.post("/api/targets", status_code=201)
def add(t: NewTarget):
    try:
        with db() as c:
            c.execute("INSERT INTO targets(name,url) VALUES(?,?)", (t.name.strip()[:60], str(t.url)))
    except sqlite3.IntegrityError:
        raise HTTPException(409, "That URL is already monitored")
    return {"ok": True}

@app.delete("/api/targets/{tid}")
def remove(tid: int):
    with db() as c:
        for tbl, col in (("targets", "id"), ("checks", "target_id"), ("events", "target_id")):
            c.execute(f"DELETE FROM {tbl} WHERE {col}=?", (tid,))
    return {"ok": True}

@app.get("/api/events")
def events():
    with db() as c:
        rows = c.execute("""SELECT e.ts, e.kind, t.name target FROM events e
                            JOIN targets t ON t.id=e.target_id ORDER BY e.ts DESC LIMIT 30""").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/stream")
async def stream():
    q: asyncio.Queue = asyncio.Queue()
    subscribers.add(q)
    async def gen():
        try:
            while True:
                try:
                    yield f"data: {await asyncio.wait_for(q.get(), 15)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            subscribers.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/")
def index(): return FileResponse("static/index.html")
app.mount("/static", StaticFiles(directory="static"), name="static")
