# rag_engine.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import sqlite3, os, json, hashlib, time, requests, traceback
import numpy as np

class RagEngine:
    """
    Schema-aware Vector RAG for SQLite + Ollama embeddings.
    - Builds small 'docs' from live schema (table/column notes)
    - Embeds & stores them in SQLite (rag_docs)
    - Loads vectors to memory for fast cosine retrieval
    """

    def __init__(
        self,
        db_path: str = "database.db",
        ollama_base: str = "http://127.0.0.1:11434",
        embed_model: str = "nomic-embed-text",
        top_k: int = 6,
    ):
        self.DB_PATH = db_path
        self.OLLAMA_GEN = f"{ollama_base.rstrip('/')}/api/generate"
        self.OLLAMA_EMB = f"{ollama_base.rstrip('/')}/api/embeddings"
        self.EMBED_MODEL = embed_model
        self.TOP_K = top_k

        # in-memory cache
        self._schema_cache: Dict[str, Any] = {}
        self._schema_hash: Optional[str] = None
        self._schema_text: str = ""

        self._rag_vecs: Optional[np.ndarray] = None  # (N, D)
        self._rag_texts: List[str] = []
        self._rag_meta_rows: List[Dict[str, Any]] = []
        self._rag_dim: int = 0

    # ---------- public lifecycle ----------

    def ensure_tables(self) -> None:
        """Create RAG storage tables if needed."""
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rag_docs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_type TEXT,            -- 'table' | 'column' | 'note'
                table_name TEXT,
                column_name TEXT,
                text TEXT NOT NULL,
                embedding TEXT NOT NULL   -- JSON list[float]
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rag_meta(
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

    def startup(self) -> None:
        """Call on app startup."""
        self.refresh_schema_cache(force=True)
        self.load_vector_cache()

    # ---------- schema handling ----------

    def refresh_schema_cache(self, force: bool = False) -> None:
        schema = self._introspect_schema(self.DB_PATH)
        text = self._schema_to_text(schema)
        h = self._hash(text)
        if force or h != self._schema_hash:
            self._schema_cache = schema
            self._schema_text = text
            self._schema_hash = h

    def get_schema_context(self) -> Tuple[str, Dict[str, Any], str]:
        """Return (schema_text, schema_struct, schema_hash)."""
        self.refresh_schema_cache(force=False)
        return self._schema_text, self._schema_cache, self._schema_hash or ""

    # ---------- vector index build/retrieval ----------

    def rebuild_from_schema(self) -> Tuple[int, int, float]:
        """
        Build docs from live schema, embed them, store in rag_docs,
        refresh in-memory cache. Returns (docs_count, dim, duration_ms).
        """
        t0 = time.time()
        text, struct, shash = self.get_schema_context()

        docs: List[Tuple[str, str, Optional[str], str]] = []
        # table docs
        for t, meta in struct["tables"].items():
            col_names = ", ".join([c["name"] for c in meta["columns"]])
            docs.append(("table", t, None, f"Table {t}: columns = [{col_names}]"))
            # column docs
            for c in meta["columns"]:
                cname, ctype = c["name"], c["type"]
                docs.append(("column", t, cname, f"Column {t}.{cname}: type={ctype}"))

        texts = [d[3] for d in docs]
        embs = self._embed(texts)  # (N, D)

        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM rag_docs")
        for (doc_type, table, col, t), vec in zip(docs, embs):
            cur.execute(
                "INSERT INTO rag_docs(doc_type, table_name, column_name, text, embedding) VALUES (?, ?, ?, ?, ?)",
                (doc_type, table, col, t, json.dumps(vec.tolist())),
            )
        self._save_meta(conn, "schema_hash", shash)
        conn.commit()
        conn.close()

        self.load_vector_cache()
        ms = round((time.time() - t0) * 1000, 2)
        return len(docs), (embs.shape[1] if embs.size else 0), ms

    def load_vector_cache(self) -> None:
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT doc_type, table_name, column_name, text, embedding FROM rag_docs")
        rows = cur.fetchall()
        conn.close()

        self._rag_texts = []
        self._rag_meta_rows = []
        vecs = []
        for doc_type, table, column, text, emb_json in rows:
            self._rag_texts.append(text)
            self._rag_meta_rows.append({"doc_type": doc_type, "table": table, "column": column})
            vecs.append(json.loads(emb_json))

        if vecs:
            mat = np.array(vecs, dtype=np.float32)
            self._rag_vecs = self._normalize(mat)
            self._rag_dim = self._rag_vecs.shape[1]
        else:
            self._rag_vecs = np.zeros((0, 0), dtype=np.float32)
            self._rag_dim = 0

    def retrieve(self, question: str, k: Optional[int] = None) -> List[str]:
        """Return top-k snippet texts for a question (cosine similarity)."""
        if self._rag_vecs is None or self._rag_vecs.shape[0] == 0:
            return []
        if k is None:
            k = self.TOP_K
        qv = self._embed([question])
        qv = self._normalize(qv)[0:1, :]
        sims = (self._rag_vecs @ qv.T).ravel()
        idx = np.argsort(-sims)[:k]
        return [self._rag_texts[i] for i in idx]

    def stats(self) -> Dict[str, Any]:
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM rag_docs")
        n = cur.fetchone()[0]
        conn.close()
        return {"docs": n, "embed_dim": self._rag_dim, "last_hash": self._get_meta("schema_hash")}

    # ---------- helpers (private) ----------

    def _introspect_schema(self, db_path: str) -> Dict[str, Any]:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [r[0] for r in cur.fetchall()]
        schema = {"tables": {}}
        for t in tables:
            cur.execute(f"PRAGMA table_info('{t}')")
            cols = [{"name": r[1], "type": r[2], "notnull": bool(r[3]), "pk": bool(r[5])} for r in cur.fetchall()]
            cur.execute(f"PRAGMA foreign_key_list('{t}')")
            fks = [{"table": r[2], "from": r[3], "to": r[4]} for r in cur.fetchall()]
            schema["tables"][t] = {"columns": cols, "foreign_keys": fks}
        conn.close()
        return schema

    def _schema_to_text(self, schema: Dict[str, Any]) -> str:
        parts = []
        for t, meta in schema["tables"].items():
            cols = ", ".join(f"{c['name']} {c['type']}" for c in meta["columns"])
            parts.append(f"{t}({cols})")
        return " ; ".join(parts)

    def _hash(self, s: str) -> str:
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def _embed(self, texts: List[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, self._rag_dim or 768), dtype=np.float32)
        r = requests.post(self.OLLAMA_EMB, json={"model": self.EMBED_MODEL, "input": texts}, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Embedding error {r.status_code}: {r.text}")
        data = r.json()
        embs = np.array(data.get("embeddings", []), dtype=np.float32)
        return embs

    def _normalize(self, mat: np.ndarray) -> np.ndarray:
        denom = np.linalg.norm(mat, axis=1, keepdims=True) + 1e-12
        return mat / denom

    def _save_meta(self, conn: sqlite3.Connection, k: str, v: str) -> None:
        cur = conn.cursor()
        cur.execute("INSERT INTO rag_meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

    def _get_meta(self, k: str) -> Optional[str]:
        conn = sqlite3.connect(self.DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT v FROM rag_meta WHERE k=?", (k,))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
