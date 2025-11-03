# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Any, List, Dict
import os, sqlite3, requests, time, traceback, glob

from rag_engine import RagEngine  # <-- our separated RAG module

# -------------------------
# CONFIG
# -------------------------
DB_PATH     = os.getenv("QM_DB_PATH", "database.db")
OLLAMA_BASE = os.getenv("QM_OLLAMA_URL", "http://127.0.0.1:11434")
GEN_MODEL   = os.getenv("QM_MODEL", "qwen2.5-coder:3b-instruct-q4_K_M")
EMB_MODEL   = os.getenv("QM_EMBED_MODEL", "nomic-embed-text")
TOP_K       = int(os.getenv("QM_TOPK", "6"))
INTERNAL_TABLE_PREFIXES = ("sqlite_", "rag__", "rag_", "cache_", "_ai_")

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
    Delete the SQLite db file if present.
    Returns True if deleted, False if it didn't exist.
    """
    if os.path.exists(DB_PATH):
        # Ensure no open connection here; we use short-lived connections elsewhere
        os.remove(DB_PATH)
        return True
    return False

# -------------------------
# DB init (demo users table)
# -------------------------
def init_demo_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # USERS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, city TEXT, age INTEGER, email TEXT
        )
    """)
    # ORDERS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders(
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            order_date TEXT,
            total_amount REAL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)
    # PRODUCTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products(
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            category TEXT,
            price REAL
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
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        )
    """)
    # PAYMENTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments(
            payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            amount REAL,
            payment_method TEXT,
            payment_date TEXT
        )
    """)
    # INVENTORY
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory(
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            stock_quantity INTEGER,
            last_updated TEXT
        )
    """)
    # SUPPLIERS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS suppliers(
            supplier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            contact_email TEXT,
            city TEXT
        )
    """)
    # SHIPMENTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shipments(
            shipment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            shipped_date TEXT,
            delivery_status TEXT
        )
    """)
    # REVIEWS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reviews(
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            user_id INTEGER,
            rating INTEGER,
            comment TEXT
        )
    """)
    # EMPLOYEES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employees(
            employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            department TEXT,
            salary REAL,
            hire_date TEXT
        )
    """)
    # DEPARTMENTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS departments(
            dept_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dept_name TEXT,
            manager_id INTEGER
        )
    """)

    # seed minimal demo rows
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO users(name, city, age, email) VALUES (?,?,?,?)",
            [
                ("Karthikeya","Hyderabad",27,"karthikeya@example.com"),
                ("Ravi","Bangalore",30,"ravi@example.com"),
                ("Sneha","Hyderabad",25,"sneha@example.com"),
                ("John","Delhi",28,"john@example.com"),
                ("Aditi","Hyderabad",29,"aditi@example.com"),
            ],
        )

    conn.commit()
    conn.close()

def seed_demo_data():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # USERS
    cur.executemany(
        "INSERT INTO users(name, city, age, email) VALUES (?,?,?,?)",
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
        "INSERT INTO products(name, category, price) VALUES (?,?,?)",
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
        "INSERT INTO suppliers(name, contact_email, city) VALUES (?,?,?)",
        [
            ("TechSource","contact@techsource.com","Hyderabad"),
            ("GadgetHub","hello@gadgethub.com","Bangalore"),
            ("AudioWorld","support@audioworld.com","Mumbai"),
        ],
    )

    # ORDERS (users → orders)
    cur.executemany(
        "INSERT INTO orders(user_id, order_date, total_amount) VALUES (?,?,?)",
        [
            (1,"2025-10-01",1698.0),
            (2,"2025-10-02",399.0),
            (3,"2025-10-03",2298.0),
        ],
    )

    # ORDER_ITEMS (orders → products)
    cur.executemany(
        "INSERT INTO order_items(order_id, product_id, quantity) VALUES (?,?,?)",
        [
            (1,1,1),  # iPhone 15
            (1,5,1),  # Sony WH-1000XM5
            (2,5,1),
            (3,3,1),  # MacBook Air
            (3,2,1),  # Pixel 9
        ],
    )

    # PAYMENTS
    cur.executemany(
        "INSERT INTO payments(order_id, amount, payment_method, payment_date) VALUES (?,?,?,?)",
        [
            (1,1698.0,"UPI","2025-10-01"),
            (2,399.0,"CARD","2025-10-02"),
            (3,2298.0,"UPI","2025-10-03"),
        ],
    )

    # INVENTORY
    cur.executemany(
        "INSERT INTO inventory(product_id, stock_quantity, last_updated) VALUES (?,?,?)",
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
        "INSERT INTO shipments(order_id, shipped_date, delivery_status) VALUES (?,?,?)",
        [
            (1,"2025-10-02","Delivered"),
            (2,"2025-10-03","In Transit"),
            (3,"2025-10-04","Processing"),
        ],
    )

    # REVIEWS
    cur.executemany(
        "INSERT INTO reviews(product_id, user_id, rating, comment) VALUES (?,?,?,?)",
        [
            (1,1,5,"Loved it"),
            (5,2,4,"Great sound"),
            (3,3,5,"Perfect for travel"),
        ],
    )

    # EMPLOYEES
    cur.executemany(
        "INSERT INTO employees(name, department, salary, hire_date) VALUES (?,?,?,?)",
        [
            ("Priya","Sales",1200000,"2024-06-01"),
            ("Nikhil","Ops",900000,"2025-01-10"),
        ],
    )

    # DEPARTMENTS
    cur.executemany(
        "INSERT INTO departments(dept_name, manager_id) VALUES (?,?)",
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
        f"Schema: {schema_text}\n"
        f"Relevant Notes:\n{notes_block}\n"
        f"Question: {question}\n"
        "Return ONLY the SQL query, no explanations, no comments, no markdown."
    )
    r = requests.post(
        f"{OLLAMA_BASE.rstrip('/')}/api/generate",
        json={"model": GEN_MODEL, "prompt": prompt, "stream": False},
        timeout=60,
    )
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Ollama returned {r.status_code}: {r.text}")
    sql = (r.json().get("response") or "").replace("```sql", "").replace("```", "").strip()
    if not sql or not sql.lower().startswith(("select", "with", "insert", "update", "delete")):
        raise HTTPException(status_code=502, detail=f"LLM did not return SQL. raw='{sql}'")
    return sql

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
def admin_reset(rebuild_rag: bool = True, reseed: bool = True):
    """
    Resets the entire environment:
      1) Deletes database.db
      2) Recreates schema (init_demo_db)
      3) Optionally seeds demo rows (seed_demo_data)
      4) Rebuilds RAG index from live schema (rag.rebuild_from_schema)
    """
    t0 = time.time()

    deleted = drop_database_file()

    # recreate schema
    init_demo_db()

    seeded = False
    if reseed:
        seed_demo_data()
        seeded = True

    # RAG: ensure tables + rebuild
    rag.ensure_tables()
    rag.startup()

    rag_result = None
    if rebuild_rag:
        try:
            docs, dim, ms = rag.rebuild_from_schema()
            rag_result = {"docs_indexed": docs, "embed_dim": dim, "duration_ms": ms}
        except Exception as e:
            rag_result = {"error": str(e)}

    duration = round((time.time() - t0) * 1000, 2)

    return {
        "ok": True,
        "deleted_db": deleted,
        "reseeded": seeded,
        "rag_rebuild": rag_result,
        "table_counts": get_table_counts(include_internal=False),
        "duration_ms": duration,
    }

@app.post("/seed/demo", tags=["utils"])
def seed_demo():
    seed_demo_data()
    return {"ok": True, "msg": "Seeded demo data"}

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
