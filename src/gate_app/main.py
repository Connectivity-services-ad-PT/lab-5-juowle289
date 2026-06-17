import os
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ──────────────────────────────────────────────
# Config từ environment variables
# ──────────────────────────────────────────────
SERVICE_NAME    = os.getenv("SERVICE_NAME", "access-gate")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.4.0")
AUTH_TOKEN      = os.getenv("AUTH_TOKEN", "local-dev-token")

DB_HOST     = os.getenv("DB_HOST", "")           # rỗng = không dùng DB
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "gatedb")
DB_USER     = os.getenv("DB_USER", "gateuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "gatepass")

USE_DB = bool(DB_HOST)  # True khi chạy Compose, False khi chạy mock/lab03

# ──────────────────────────────────────────────
# App
# ──────────────────────────────────────────────
app = FastAPI(
    title="FIT4110 Lab 05 – Access Gate Service",
    version=SERVICE_VERSION,
    description="Access Gate API with PostgreSQL backend (Lab 05).",
)

# ──────────────────────────────────────────────
# DB Connection — retry để xử lý race condition
# ──────────────────────────────────────────────
_db_conn = None

def get_db():
    global _db_conn
    if not USE_DB:
        return None
    if _db_conn is None or _db_conn.closed:
        retries = 5
        for i in range(retries):
            try:
                _db_conn = psycopg2.connect(
                    host=DB_HOST, port=DB_PORT,
                    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                )
                _db_conn.autocommit = True
                break
            except psycopg2.OperationalError as e:
                if i < retries - 1:
                    time.sleep(2)
                else:
                    raise RuntimeError(f"Cannot connect to DB after {retries} retries: {e}")
    return _db_conn

# ──────────────────────────────────────────────
# In-memory fallback (dùng khi không có DB)
# ──────────────────────────────────────────────
_mem_events: List[Dict] = []
_mem_cards: Dict[str, Dict] = {
    "CARD-001": {"card_id": "CARD-001", "owner_name": "Tran Thi B", "role": "staff",
                 "status": "active", "valid_until": "2027-06-30",
                 "created_at": "2026-01-01T00:00:00+00:00"},
    "CARD-002": {"card_id": "CARD-002", "owner_name": "Le Van C", "role": "student",
                 "status": "active", "valid_until": "2027-12-31",
                 "created_at": "2026-01-01T00:00:00+00:00"},
}

# ──────────────────────────────────────────────
# Enums & Schemas
# ──────────────────────────────────────────────
class Direction(str, Enum):
    entry = "entry"
    exit  = "exit"

class AccessResult(str, Enum):
    allow = "allow"
    deny  = "deny"

class CardRole(str, Enum):
    student    = "student"
    staff      = "staff"
    visitor    = "visitor"
    contractor = "contractor"

class CardStatus(str, Enum):
    active  = "active"
    expired = "expired"
    revoked = "revoked"

class HealthResponse(BaseModel):
    status:  str
    service: str
    version: str

class ProblemDetails(BaseModel):
    type:     str = "about:blank"
    title:    str
    status:   int = Field(..., ge=400, le=599)
    detail:   str
    instance: Optional[str] = None

class AccessEventCreate(BaseModel):
    card_id:   str       = Field(..., min_length=3, examples=["CARD-001"])
    gate_id:   str       = Field(..., min_length=3, examples=["GATE-A1"])
    direction: Direction = Field(..., examples=["entry"])
    timestamp: str       = Field(..., examples=["2026-05-13T08:00:00+07:00"])

class AccessEventCreated(BaseModel):
    event_id:  str
    card_id:   str
    gate_id:   str
    direction: Direction
    result:    AccessResult
    reason:    Optional[str] = None
    timestamp: str

class CardCreate(BaseModel):
    card_id:     str          = Field(..., min_length=3, examples=["CARD-010"])
    owner_name:  str          = Field(..., min_length=2, examples=["Nguyen Van A"])
    role:        CardRole     = Field(..., examples=["student"])
    valid_until: Optional[str] = Field(default=None, examples=["2027-12-31"])

class Card(BaseModel):
    card_id:     str
    owner_name:  str
    role:        CardRole
    status:      CardStatus
    valid_until: Optional[str] = None
    created_at:  str

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def next_event_id() -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    count = len(_mem_events) + 1 if not USE_DB else 0
    return f"EVT-{today}-{count:04d}"

def build_problem(*, status_code: int, title: str, detail: str,
                  instance: Optional[str] = None,
                  problem_type: str = "about:blank") -> Dict:
    p = {"type": problem_type, "title": title, "status": status_code, "detail": detail}
    if instance:
        p["instance"] = instance
    return p

def normalize_row(row: Dict) -> Dict:
    """Chuyển kiểu dữ liệu PostgreSQL (date, enum) thành str để Pydantic parse được."""
    result = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):   # datetime.date / datetime.datetime
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result

# ──────────────────────────────────────────────
# Exception handlers
# ──────────────────────────────────────────────
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    problem = exc.detail if isinstance(exc.detail, dict) else build_problem(
        status_code=exc.status_code, title="HTTP Error",
        detail=str(exc.detail), instance=str(request.url.path))
    return JSONResponse(status_code=exc.status_code, content=problem,
                        media_type="application/problem+json")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    first = exc.errors()[0] if exc.errors() else {}
    loc   = ".".join(str(x) for x in first.get("loc", []))
    msg   = first.get("msg", "Validation error")
    return JSONResponse(status_code=422, content=build_problem(
        status_code=422, title="Validation error",
        detail=f"{loc}: {msg}" if loc else msg,
        instance=str(request.url.path),
        problem_type="https://smart-campus.local/problems/validation-error"),
        media_type="application/problem+json")

# ──────────────────────────────────────────────
# Auth dependency
# ──────────────────────────────────────────────
def verify_bearer_token(authorization: Optional[str] = Header(default=None)) -> None:
    if not authorization:
        raise HTTPException(status_code=401, detail=build_problem(
            status_code=401, title="Unauthorized",
            detail="Missing Authorization header",
            problem_type="https://smart-campus.local/problems/unauthorized"))
    if authorization != f"Bearer {AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail=build_problem(
            status_code=401, title="Unauthorized",
            detail="Invalid bearer token",
            problem_type="https://smart-campus.local/problems/unauthorized"))

# ──────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    # Kiểm tra DB kết nối nếu đang dùng
    if USE_DB:
        try:
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            raise HTTPException(status_code=503, detail=build_problem(
                status_code=503, title="Service Unavailable",
                detail="Database connection failed"))
    return HealthResponse(status="ok", service=SERVICE_NAME, version=SERVICE_VERSION)


@app.post("/access-events", response_model=AccessEventCreated,
          status_code=201, dependencies=[Depends(verify_bearer_token)])
def create_access_event(payload: AccessEventCreate) -> AccessEventCreated:
    event_id  = next_event_id()
    timestamp = now_iso()

    if USE_DB:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Đếm số event để tạo ID duy nhất
            cur.execute("SELECT COUNT(*) as cnt FROM access_events")
            cnt = cur.fetchone()["cnt"]
            today = datetime.now(timezone.utc).strftime("%Y%m%d")
            event_id = f"EVT-{today}-{int(cnt) + 1:04d}"

            cur.execute("SELECT status FROM cards WHERE card_id = %s", (payload.card_id,))
            row = cur.fetchone()
            result = AccessResult.allow if (row and row["status"] == "active") else AccessResult.deny
            reason = None if result == AccessResult.allow else ("card_not_found" if not row else "card_revoked")

            cur.execute(
                """INSERT INTO access_events (event_id, card_id, gate_id, direction, result, reason, timestamp)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (event_id, payload.card_id, payload.gate_id,
                 payload.direction.value,          # .value để lưu string "entry"/"exit"
                 result.value, reason, timestamp))
    else:
        card   = _mem_cards.get(payload.card_id)
        result = AccessResult.allow if (card and card["status"] == "active") else AccessResult.deny
        reason = None if result == AccessResult.allow else ("card_not_found" if not card else "card_revoked")
        _mem_events.append({"event_id": event_id, "card_id": payload.card_id,
                            "gate_id": payload.gate_id, "direction": payload.direction.value,
                            "result": result.value, "reason": reason, "timestamp": timestamp})

    return AccessEventCreated(event_id=event_id, card_id=payload.card_id,
                              gate_id=payload.gate_id, direction=payload.direction,
                              result=result, reason=reason, timestamp=timestamp)


@app.get("/access-events", dependencies=[Depends(verify_bearer_token)])
def list_access_events(
    gate_id: Optional[str] = Query(default=None),
    card_id: Optional[str] = Query(default=None),
    result:  Optional[str] = Query(default=None),
    limit:   int           = Query(default=10, ge=1, le=100),
) -> Dict:
    if USE_DB:
        conn   = get_db()
        clause = ["1=1"]
        params = []
        if gate_id: clause.append("gate_id = %s"); params.append(gate_id)
        if card_id: clause.append("card_id = %s"); params.append(card_id)
        if result:  clause.append("result = %s");  params.append(result)
        params.append(limit)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM access_events WHERE {' AND '.join(clause)} ORDER BY timestamp DESC LIMIT %s", params)
            items = [dict(r) for r in cur.fetchall()]
        return {"items": items, "total": len(items)}
    else:
        items = _mem_events
        if gate_id: items = [e for e in items if e["gate_id"] == gate_id]
        if card_id: items = [e for e in items if e["card_id"] == card_id]
        if result:  items = [e for e in items if e["result"] == result]
        items = items[-limit:]
        return {"items": items, "total": len(items)}


@app.post("/cards", response_model=Card, status_code=201,
          dependencies=[Depends(verify_bearer_token)])
def create_card(payload: CardCreate) -> Card:
    created_at = now_iso()
    if USE_DB:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT card_id FROM cards WHERE card_id = %s", (payload.card_id,))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail=build_problem(
                    status_code=409, title="Conflict",
                    detail=f"card_id {payload.card_id} already exists",
                    instance="/cards",
                    problem_type="https://smart-campus.local/problems/conflict"))
            cur.execute(
                """INSERT INTO cards (card_id, owner_name, role, status, valid_until, created_at)
                   VALUES (%s,%s,%s,'active',%s,%s)""",
                (payload.card_id, payload.owner_name, payload.role.value,
                 payload.valid_until, created_at))
    else:
        if payload.card_id in _mem_cards:
            raise HTTPException(status_code=409, detail=build_problem(
                status_code=409, title="Conflict",
                detail=f"card_id {payload.card_id} already exists",
                instance="/cards",
                problem_type="https://smart-campus.local/problems/conflict"))
        _mem_cards[payload.card_id] = {
            "card_id": payload.card_id, "owner_name": payload.owner_name,
            "role": payload.role.value, "status": "active",
            "valid_until": payload.valid_until, "created_at": created_at}

    return Card(card_id=payload.card_id, owner_name=payload.owner_name,
                role=payload.role, status=CardStatus.active,
                valid_until=payload.valid_until, created_at=created_at)


@app.get("/cards/{card_id}", response_model=Card,
         dependencies=[Depends(verify_bearer_token)])
def get_card(card_id: str) -> Card:
    if USE_DB:
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM cards WHERE card_id = %s", (card_id,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=build_problem(
                status_code=404, title="Not Found",
                detail=f"Card {card_id} not found",
                instance=f"/cards/{card_id}",
                problem_type="https://smart-campus.local/problems/not-found"))
        return Card(**normalize_row(dict(row)))
    else:
        card = _mem_cards.get(card_id)
        if not card:
            raise HTTPException(status_code=404, detail=build_problem(
                status_code=404, title="Not Found",
                detail=f"Card {card_id} not found",
                instance=f"/cards/{card_id}",
                problem_type="https://smart-campus.local/problems/not-found"))
        return Card(**card)