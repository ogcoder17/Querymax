# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Any, List, Dict
import os, sqlite3, requests, time, traceback, glob
import re
import sqlparse

from rag_engine import RagEngine  # <-- our separated RAG module
from typing import Literal

# -------------------------
# CONFIG
# -------------------------
DB_PATH     = os.getenv("QM_DB_PATH", "database.db")
OLLAMA_BASE = os.getenv("QM_OLLAMA_URL", "http://127.0.0.1:11434")
GEN_MODEL   = os.getenv("QM_MODEL", "qwen2.5-coder:3b-instruct-q4_K_M")
EMB_MODEL   = os.getenv("QM_EMBED_MODEL", "nomic-embed-text")
TOP_K       = int(os.getenv("QM_TOPK", "6"))
INTERNAL_TABLE_PREFIXES = ("sqlite_", "rag__", "rag_", "cache_", "_ai_")
SQL_PREFIX_RE = re.compile(r"^\s*(with|select|insert|update|delete)\b", re.I)

# -------------------------
# APP
# -------------------------
app = FastAPI(title="QueryMax API", version="1.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# RAG engine instance
rag = RagEngine(db_path=DB_PATH, ollama_base=OLLAMA_BASE, embed_model=EMB_MODEL, top_k=TOP_K)

# -------------------------
# Pydantic models
# -------------------------
class QueryRequest(BaseModel):
    question: str = Field(..., example="Show all users from Hyderabad")

class SQLResult(BaseModel):
    columns: List[str]
    rows: List[List[Any]]

class QueryResponse(BaseModel):
    question: str
    sql: str
    result: SQLResult
    timing: Dict[str, float]

class ChatRequest(BaseModel):
    message: str
    mode: Literal["auto","nl","sql"] = "auto"  # detect or force one mode

class ChatResponse(BaseModel):
    mode: str
    sql: str | None = None
    result: SQLResult | None = None
    reply: str
    timing: Dict[str,float] | None = None

class ChatRequest(BaseModel):
    message: str
    mode: str | None = "auto"   # "auto" | "nl" | "sql"

class ChatResponse(BaseModel):
    reply: str
    sql: str | None = None
    result: SQLResult | None = None

def generate_sql_from_question(question: str) -> str:
    # alias to your existing NL->SQL
    return generate_sql(question)


def get_table_counts(include_internal: bool = False) -> dict:
    """
    Return {table_name: row_count}. By default hides internal tables.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    if not include_internal:
        tables = [
            t for t in tables
            if not t.startswith(INTERNAL_TABLE_PREFIXES)
        ]

    counts = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            counts[t] = cur.fetchone()[0]
        except Exception:
            counts[t] = None
    conn.close()
    return counts

def drop_database_file() -> bool:
    """
    Delete the SQLite DB file completely so every reset starts fresh.
    Returns True if deleted, False if it didn't exist.
    """
    import gc, time
    gc.collect()
    try:
        if os.path.exists(DB_PATH):
            # Give filesystem a moment to release file handles
            time.sleep(0.2)
            os.remove(DB_PATH)
            return True
    except PermissionError:
        # fallback: rename if delete is blocked
        os.rename(DB_PATH, DB_PATH + ".old")
    return False



# -------------------------
# DB init (demo users table)
# -------------------------
BUSINESS_TABLES = [
    "order_items", "orders", "payments", "inventory", "shipments",
    "reviews", "products", "suppliers", "employees", "departments", "users"
]

def init_demo_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    # USERS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT COLLATE NOCASE,
            city TEXT COLLATE NOCASE,
            age INTEGER,
            email TEXT COLLATE NOCASE,
            UNIQUE(name, city, age, email) ON CONFLICT IGNORE
        )
    """)

    # ORDERS (uniqueness by business values)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders(
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            order_date TEXT,
            total_amount REAL,
            FOREIGN KEY(user_id) REFERENCES users(id),
            UNIQUE(user_id, order_date, total_amount) ON CONFLICT IGNORE
        )
    """)

    # PRODUCTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products(
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT COLLATE NOCASE,
            category TEXT COLLATE NOCASE,
            price REAL,
            UNIQUE(name, category) ON CONFLICT IGNORE
        )
    """)

    # ORDER_ITEMS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS order_items(
            order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            FOREIGN KEY(order_id) REFERENCES orders(order_id),
            FOREIGN KEY(product_id) REFERENCES products(product_id),
            UNIQUE(order_id, product_id) ON CONFLICT IGNORE
        )
    """)

    # PAYMENTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments(
            payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            amount REAL,
            payment_method TEXT,
            payment_date TEXT,
            UNIQUE(order_id, amount, payment_date) ON CONFLICT IGNORE
        )
    """)

    # INVENTORY
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory(
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            stock_quantity INTEGER,
            last_updated TEXT,
            UNIQUE(product_id) ON CONFLICT IGNORE
        )
    """)

    # SUPPLIERS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS suppliers(
            supplier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT COLLATE NOCASE,
            contact_email TEXT COLLATE NOCASE,
            city TEXT COLLATE NOCASE,
            UNIQUE(name, contact_email) ON CONFLICT IGNORE
        )
    """)

    # SHIPMENTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shipments(
            shipment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            shipped_date TEXT,
            delivery_status TEXT,
            UNIQUE(order_id, shipped_date) ON CONFLICT IGNORE
        )
    """)

    # REVIEWS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reviews(
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            user_id INTEGER,
            rating INTEGER,
            comment TEXT,
            UNIQUE(product_id, user_id, rating, comment) ON CONFLICT IGNORE
        )
    """)

    # EMPLOYEES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employees(
            employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT COLLATE NOCASE,
            department TEXT COLLATE NOCASE,
            salary REAL,
            hire_date TEXT,
            UNIQUE(name, department, hire_date) ON CONFLICT IGNORE
        )
    """)

    # DEPARTMENTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS departments(
            dept_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dept_name TEXT COLLATE NOCASE,
            manager_id INTEGER,
            UNIQUE(dept_name) ON CONFLICT IGNORE
        )
    """)

    conn.commit()
    conn.close()


def seed_demo_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    # USERS
    cur.executemany(
        "INSERT OR IGNORE INTO users(name, city, age, email) VALUES (?,?,?,?)",
        [
            ("Karthikeya","Hyderabad",27,"karthikeya@example.com"),
            ("Ravi","Bangalore",30,"ravi@example.com"),
            ("Sneha","Hyderabad",25,"sneha@example.com"),
            ("John","Delhi",28,"john@example.com"),
            ("Aditi","Hyderabad",29,"aditi@example.com"),
        ],
    )

    # PRODUCTS
    cur.executemany(
        "INSERT OR IGNORE INTO products(name, category, price) VALUES (?,?,?)",
        [
            ("iPhone 15","Phones",1299.0),
            ("Pixel 9","Phones",999.0),
            ("MacBook Air","Laptops",1499.0),
            ("ThinkPad X1","Laptops",1799.0),
            ("Sony WH-1000XM5","Audio",399.0),
        ],
    )

    # SUPPLIERS
    cur.executemany(
        "INSERT OR IGNORE INTO suppliers(name, contact_email, city) VALUES (?,?,?)",
        [
            ("TechSource","contact@techsource.com","Hyderabad"),
            ("GadgetHub","hello@gadgethub.com","Bangalore"),
            ("AudioWorld","support@audioworld.com","Mumbai"),
        ],
    )

    # ORDERS
    cur.executemany(
        "INSERT OR IGNORE INTO orders(user_id, order_date, total_amount) VALUES (?,?,?)",
        [
            (1,"2025-10-01",1698.0),
            (2,"2025-10-02",399.0),
            (3,"2025-10-03",2298.0),
        ],
    )

    # ORDER_ITEMS
    cur.executemany(
        "INSERT OR IGNORE INTO order_items(order_id, product_id, quantity) VALUES (?,?,?)",
        [
            (1,1,1),
            (1,5,1),
            (2,5,1),
            (3,3,1),
            (3,2,1),
        ],
    )

    # PAYMENTS
    cur.executemany(
        "INSERT OR IGNORE INTO payments(order_id, amount, payment_method, payment_date) VALUES (?,?,?,?)",
        [
            (1,1698.0,"UPI","2025-10-01"),
            (2,399.0,"CARD","2025-10-02"),
            (3,2298.0,"UPI","2025-10-03"),
        ],
    )

    # INVENTORY
    cur.executemany(
        "INSERT OR IGNORE INTO inventory(product_id, stock_quantity, last_updated) VALUES (?,?,?)",
        [
            (1,10,"2025-10-01"),
            (2,15,"2025-10-04"),
            (3,5,"2025-10-01"),
            (4,3,"2025-10-02"),
            (5,20,"2025-10-03"),
        ],
    )

    # SHIPMENTS
    cur.executemany(
        "INSERT OR IGNORE INTO shipments(order_id, shipped_date, delivery_status) VALUES (?,?,?)",
        [
            (1,"2025-10-02","Delivered"),
            (2,"2025-10-03","In Transit"),
            (3,"2025-10-04","Processing"),
        ],
    )

    # REVIEWS
    cur.executemany(
        "INSERT OR IGNORE INTO reviews(product_id, user_id, rating, comment) VALUES (?,?,?,?)",
        [
            (1,1,5,"Loved it"),
            (5,2,4,"Great sound"),
            (3,3,5,"Perfect for travel"),
        ],
    )

    # EMPLOYEES
    cur.executemany(
        "INSERT OR IGNORE INTO employees(name, department, salary, hire_date) VALUES (?,?,?,?)",
        [
            ("Priya","Sales",1200000,"2024-06-01"),
            ("Nikhil","Ops",900000,"2025-01-10"),
        ],
    )

    # DEPARTMENTS
    cur.executemany(
        "INSERT OR IGNORE INTO departments(dept_name, manager_id) VALUES (?,?)",
        [
            ("Sales",1),
            ("Ops",2),
        ],
    )

    conn.commit()
    conn.close()


# -------------------------
# Startup
# -------------------------
@app.on_event("startup")
def _startup():
    init_demo_db()
    rag.ensure_tables()
    rag.startup()

# -------------------------
# Helpers
# -------------------------
def execute_sql(sql: str) -> SQLResult:
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        conn.close()
        return SQLResult(columns=cols, rows=rows)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"SQL execution error: {e}")

def generate_sql(question: str) -> str:
    schema_text, _, _ = rag.get_schema_context()
    notes = rag.retrieve(question, k=TOP_K)
    notes_block = "\n".join(f"- {n}" for n in notes) if notes else "- (none)"

    prompt = (
        "You are a SQL expert. Use ONLY the provided schema and relevant notes.\n"
        "Return **exactly one** valid SQLite SQL statement that answers the question.\n"
        "- Do not include explanations, comments, markdown, or multiple statements.\n"
        "- If multiple steps seem necessary, choose the single best query.\n"
        "- For text equality (like city/name/category), make sure it is case-insensitive and use comparisons via LOWER(column)=LOWER('value') or `COLLATE NOCASE`.\n"
        "- The output must start with SELECT/WITH/INSERT/UPDATE/DELETE and end with a semicolon.\n\n"
        f"Schema:\n{schema_text}\n\n"
        f"Relevant Notes:\n{notes_block}\n\n"
        f"Question: {question}\n"
        "Output: (only one SQL statement, nothing else)"
    )

    r = requests.post(
        f"{OLLAMA_BASE.rstrip('/')}/api/generate",
        json={
            "model": GEN_MODEL,
            "prompt": prompt,
            "stream": False,
            # ---- Deterministic decoding ----
            "options": {
                "temperature": 0.0,
                "top_p": 1.0,
                "repeat_penalty": 1.0,
                "num_predict": 512,
            },
        },
        timeout=60,
    )
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Ollama returned {r.status_code}: {r.text}")

    raw = (r.json().get("response") or "").strip()
    sql = _normalize_single_sql(raw)

    if not sql:
        raise HTTPException(status_code=502, detail=f"LLM did not return a single SQL statement. raw='{raw[:200]}'")

    # Ensure it's exactly one statement (hard gate)
    if len([s for s in sqlparse.split(sql) if s.strip()]) != 1:
        raise HTTPException(status_code=502, detail="Model returned more than one statement; rejecting.")

    return sql

def _normalize_single_sql(raw: str) -> str:
    """
    Keep exactly the first SQL statement. Remove code fences and comments.
    This tolerates CTEs (WITH ... SELECT ...).
    """
    if not raw:
        return ""
    raw = raw.replace("```sql", "").replace("```", "").strip()

    # Remove common leading explanations accidentally returned by LLM
    # Keep only first statement
    statements = [s.strip() for s in sqlparse.split(raw) if s and s.strip()]
    if not statements:
        return ""

    # Take the first statement only and ensure it ends with ';'
    sql = statements[0].strip()
    if not sql.endswith(";"):
        sql += ";"

    # Final safety check: it must start with one of the allowed SQL verbs
    if not SQL_PREFIX_RE.match(sql):
        return ""

    return sql

@app.post("/chat", response_model=ChatResponse, tags=["chatbot"])
def chat(req: ChatRequest):
    """
    Chatbot endpoint that can:
      - mode="nl":    interpret natural language and generate SQL then run it
      - mode="sql":   execute raw SQL directly
      - mode="auto":  detect: if input looks like SQL -> run SQL, else do NL->SQL

    Returns: text reply, generated SQL (if applicable), and SQL result.
    """
    text = (req.message or "").strip()
    if not text:
        raise HTTPException(status_code=422, detail="message cannot be empty")

    mode = (req.mode or "auto").lower()

    # simple detector for SQL-like input
    def _looks_like_sql(s: str) -> bool:
        return bool(SQL_PREFIX_RE.match(s.strip()))

    try:
        # --- SQL mode: execute exactly what the user sent
        if mode == "sql":
            sql = text
            result = execute_sql(sql)
            return ChatResponse(
                reply="Executed your SQL.",
                sql=sql,
                result=result
            )

        # --- NL mode: generate SQL then run
        if mode == "nl":
            sql = generate_sql_from_question(text)   # alias -> generate_sql(text)
            result = execute_sql(sql)
            return ChatResponse(
                reply="Here are the results for your question.",
                sql=sql,
                result=result
            )

        # --- auto mode: detect if it's SQL, else NL->SQL
        if mode == "auto":
            if _looks_like_sql(text):
                sql = text
                result = execute_sql(sql)
                return ChatResponse(
                    reply="I detected SQL and executed it.",
                    sql=sql,
                    result=result
                )
            else:
                sql = generate_sql_from_question(text)
                result = execute_sql(sql)
                return ChatResponse(
                    reply="Converted your question to SQL and executed it.",
                    sql=sql,
                    result=result
                )

        # If mode is unknown
        raise HTTPException(status_code=400, detail=f"Unknown mode '{mode}'. Use 'auto' | 'nl' | 'sql'.")

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Chatbot error: {e}")


# -------------------------
# Routes
# -------------------------
@app.get("/", tags=["system"])
def health():
    return {"message": "QueryMax API is running 🚀", "model": GEN_MODEL}

@app.get("/debug/dbinfo", tags=["debug"])
def debug_dbinfo(include_internal: bool = False):
    return {
        "db_path": DB_PATH,
        "table_counts": get_table_counts(include_internal=include_internal)
    }

@app.post("/admin/reset", tags=["admin"])
def admin_reset(mode: str = "safe"):
    """
    SAFE reset (default):
      - Ensures schema exists (no drop)
      - Seeds idempotently (INSERT OR IGNORE): no duplicates
      - Rebuilds RAG

    HARD reset (mode=hard):
      - Drops database.db
      - Recreates schema with UNIQUE constraints
      - Seeds once
      - Rebuilds RAG
    """
    t0 = time.time()

    if mode == "hard":
        # one-time migration to apply UNIQUE constraints cleanly
        deleted = drop_database_file()
        init_demo_db()
        seed_demo_data()
    else:
        # default safe: no drop, just ensure schema + idempotent seed
        deleted = False
        init_demo_db()
        seed_demo_data()

    rag.ensure_tables()
    rag.startup()
    docs, dim, ms = rag.rebuild_from_schema()

    return {
        "ok": True,
        "mode": mode,
        "deleted_db": deleted,
        "rag_rebuild": {"docs_indexed": docs, "embed_dim": dim, "duration_ms": ms},
        "table_counts": get_table_counts(include_internal=False),
        "duration_ms": round((time.time() - t0) * 1000, 2),
    }

    
# @app.post("/seed/demo", tags=["utils"])
# def seed_demo():
#     seed_demo_data()
#     return {"ok": True, "msg": "Seeded demo data"}

@app.post("/sql/run", tags=["utils"])
def run_sql(payload: dict):
    sql = payload.get("sql")
    if not sql:
        raise HTTPException(status_code=422, detail="Provide 'sql'")
    return {"result": execute_sql(sql)}

@app.get("/schema", tags=["rag"])
def schema_view():
    text, struct, h = rag.get_schema_context()
    return {"schema_text": text, "schema": struct, "hash": h}

@app.post("/rag/rebuild", tags=["rag"])
def rag_rebuild():
    docs, dim, ms = rag.rebuild_from_schema()
    return {"docs_indexed": docs, "embed_dim": dim, "duration_ms": ms}

@app.get("/rag/stats", tags=["rag"])
def rag_stats():
    return rag.stats()

@app.post("/query", response_model=QueryResponse, tags=["nl->sql"])
def query_db(req: QueryRequest):
    t0 = time.time()
    sql = generate_sql(req.question)
    t1 = time.time()
    result = execute_sql(sql)
    t2 = time.time()
    return QueryResponse(
        question=req.question,
        sql=sql,
        result=result,
        timing={
            "model_generation_ms": round((t1 - t0) * 1000, 2),
            "sql_execution_ms": round((t2 - t1) * 1000, 2),
            "total_request_ms": round((t2 - t0) * 1000, 2),
        },
    )
