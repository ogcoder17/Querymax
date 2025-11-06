import React, { useEffect, useRef, useState } from "react";
// If you use react-router: import { Link, useNavigate } from "react-router-dom";

function SchemaModal({ open, onClose, schemaText }) {
  if (!open) return null;
  return (
    <div style={styles.modalOverlay} onClick={onClose}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 12 }}>
          <h3 style={{ margin: 0 }}>Database Schema</h3>
          <button onClick={onClose} style={styles.secondaryBtn}>Close</button>
        </div>
        <pre style={styles.pre}>{schemaText || "(no schema available)"}</pre>
      </div>
    </div>
  );
}

// --- Examples (ported from your second snippet) ---
const SAMPLES = [
  'Show all users from Hyderabad',
  'List orders with their items and total_amount',
  'Top 3 products by price',
  'Show shipments with status Delivered',
  'Employees in Sales',
];

export default function NLToSQL() {
  const [baseUrl, setBaseUrl] = useState(() => localStorage.getItem("qm_base") || "http://127.0.0.1:8000");
  const [question, setQuestion] = useState("");
  const [sql, setSql] = useState("");
  const [result, setResult] = useState({ columns: [], rows: [] });
  const [timing, setTiming] = useState(null);

  const [status, setStatus] = useState("");
  const [loading, setLoading] = useState(false);

  const [schemaOpen, setSchemaOpen] = useState(false);
  const [schemaText, setSchemaText] = useState("");

  const controllerRef = useRef(null);

  // Persist base URL
  const saveBase = () => {
    localStorage.setItem("qm_base", baseUrl.trim());
    setStatus("Saved API base");
    setTimeout(() => setStatus(""), 1000);
  };

  const ping = async () => {
    try {
      const res = await fetch(`${baseUrl}/`);
      const data = await res.json();
      setStatus(`Ping ok: ${data?.message || "online"}`);
    } catch (e) {
      setStatus("Ping failed");
    } finally {
      setTimeout(() => setStatus(""), 2000);
    }
  };

  const copySql = async () => {
    if (!sql) return;
    try {
      await navigator.clipboard.writeText(sql);
      setStatus("SQL copied");
    } catch {
      setStatus("Copy failed");
    } finally {
      setTimeout(() => setStatus(""), 1200);
    }
  };

  const run = async () => {
    if (!question.trim() || loading) return;
    // Cancel any pending request
    if (controllerRef.current) controllerRef.current.abort();
    const ac = new AbortController();
    controllerRef.current = ac;

    setLoading(true);
    setStatus("Running…");
    setSql("");
    setResult({ columns: [], rows: [] });
    setTiming(null);

    try {
      const res = await fetch(`${baseUrl}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question }),
        signal: ac.signal,
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setSql(data.sql || "");
      setResult(data.result || { columns: [], rows: [] });
      setTiming(data.timing || null);
      setStatus("Done");
    } catch (e) {
      if (e.name !== "AbortError") {
        console.error(e);
        setStatus(`Error: ${e.message?.slice(0, 160)}`);
      }
    } finally {
      setLoading(false);
      controllerRef.current = null;
    }
  };

  const seedDemo = async () => {
    if (loading) return;
    setLoading(true);
    setStatus("Seeding…");
    try {
      const res = await fetch(`${baseUrl}/seed/demo`, { method: "POST" });
      const data = await res.json();
      setStatus(data?.ok ? "Seeded" : "Seed error");
    } catch {
      setStatus("Seed error");
    } finally {
      setLoading(false);
      setTimeout(() => setStatus(""), 1200);
    }
  };

  const viewSchema = async () => {
    try {
      const res = await fetch(`${baseUrl}/schema`);
      const data = await res.json();
      setSchemaText(data?.schema_text || "");
      setSchemaOpen(true);
    } catch {
      setSchemaText("(failed to fetch schema)");
      setSchemaOpen(true);
    }
  };

  const rebuildRag = async () => {
    if (loading) return;
    setLoading(true);
    setStatus("Rebuilding RAG…");
    try {
      const res = await fetch(`${baseUrl}/rag/rebuild`, { method: "POST" });
      const data = await res.json();
      setStatus(`RAG rebuilt: ${data?.docs_indexed ?? 0} docs`);
    } catch {
      setStatus("RAG rebuild failed");
    } finally {
      setLoading(false);
      setTimeout(() => setStatus(""), 1500);
    }
  };

  const prettyJson = (obj) => JSON.stringify(obj, null, 2);

  return (
    <div style={styles.page}>

      {/* Body */}
      <div style={styles.card}>
        <h2 style={styles.h2}>Ask in Natural Language</h2>

        <textarea
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="e.g. Top 3 products by price"
          rows={4}
          style={styles.textarea}
        />

        <div style={{ display: "flex", gap: 10, alignItems: "center", marginTop: 8, flexWrap: "wrap" }}>
          <button onClick={run} disabled={loading || !question.trim()} style={styles.primaryBtn}>
            {loading ? "Running…" : "Run"}
          </button>
          {/* <button onClick={seedDemo} disabled={loading} style={styles.secondaryBtn}>Seed Demo Data</button> */}
          <button onClick={viewSchema} style={styles.secondaryBtn}>View Schema</button>
          <button onClick={rebuildRag} disabled={loading} style={styles.secondaryBtn}>Rebuild RAG</button>
          <span style={{ color: "#7dd3fc" }}>{status}</span>
        </div>

        {/* SQL */}
        <section style={{ marginTop: 18 }}>
          <h3 style={styles.h3}>Generated SQL</h3>
          <div style={styles.sqlBox}>
            <pre style={{ ...styles.pre, margin: 0 }}>{sql || "(run a question to generate SQL)"}</pre>
            <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 6 }}>
              <button onClick={copySql} disabled={!sql} style={styles.secondaryBtn}>Copy SQL</button>
            </div>
          </div>
        </section>

        {/* Result */}
        <section style={{ marginTop: 18 }}>
          <h3 style={styles.h3}>Result</h3>
          <div style={styles.tableWrap}>
            {result?.columns?.length ? (
              <table style={styles.table}>
                <thead>
                  <tr>
                    {result.columns.map((c) => (
                      <th key={c} style={styles.th}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.rows.map((row, idx) => (
                    <tr key={idx}>
                      {row.map((cell, j) => (
                        <td key={j} style={styles.td}>{String(cell)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div style={{ color: "#94a3b8" }}>(no rows)</div>
            )}
          </div>
        </section>

        {/* Timing */}
        <section style={{ marginTop: 18 }}>
          <h3 style={styles.h3}>Timing</h3>
          <pre style={styles.pre}>{timing ? prettyJson(timing) : "(no timing yet)"}</pre>
        </section>
      </div>

      {/* --- Examples card (new) --- */}
      <div style={styles.card}>
        <h4 style={{ marginTop: 0, marginBottom: 12 }}>Examples</h4>
        <div style={styles.samplesWrap}>
          {SAMPLES.map((s, i) => (
            <button
              key={i}
              style={styles.sampleBtn}
              onClick={() => setQuestion(s)}
              title={s}
            >
              {s}
            </button>
          ))}
        </div>
      </div>

      {/* Schema Modal */}
      <SchemaModal
        open={schemaOpen}
        onClose={() => setSchemaOpen(false)}
        schemaText={schemaText}
      />
    </div>
  );
}

/* -------------- styles -------------- */
const styles = {
  page: {
    minHeight: "100vh",
    background: "#0b1220",
    color: "#e2e8f0",
    paddingBottom: 40,
  },
  topbar: {
    display: "flex",
    justifyContent: "space-between",
    padding: "14px 18px",
    borderBottom: "1px solid #1f2937",
    position: "sticky",
    top: 0,
    background: "#0b1220",
    zIndex: 10,
  },
  brand: { fontWeight: 700, marginRight: 12 },
  tab: {
    color: "#cbd5e1",
    textDecoration: "none",
    padding: "6px 10px",
    borderRadius: 6,
  },
  tabActive: {
    color: "#111827",
    background: "#60a5fa",
    textDecoration: "none",
    padding: "6px 10px",
    borderRadius: 6,
    fontWeight: 600,
  },
  card: {
    maxWidth: 980,
    margin: "22px auto",
    background: "#0f172a",
    border: "1px solid #1f2937",
    borderRadius: 10,
    padding: 18,
  },
  h2: { marginTop: 0, marginBottom: 10 },
  h3: { margin: "10px 0" },
  textarea: {
    width: "100%",
    background: "#0b1220",
    color: "#e2e8f0",
    border: "1px solid #334155",
    borderRadius: 8,
    padding: 10,
    outline: "none",
  },
  input: {
    background: "#0b1220",
    color: "#e2e8f0",
    border: "1px solid #334155",
    borderRadius: 8,
    padding: "6px 10px",
    width: 240,
    outline: "none",
  },
  primaryBtn: {
    background: "#22c55e",
    color: "#0b1220",
    border: "none",
    padding: "8px 12px",
    borderRadius: 8,
    cursor: "pointer",
    fontWeight: 600,
  },
  secondaryBtn: {
    background: "#111827",
    color: "#e2e8f0",
    border: "1px solid #334155",
    padding: "8px 12px",
    borderRadius: 8,
    cursor: "pointer",
  },
  sqlBox: {
    background: "#0b1220",
    border: "1px solid #334155",
    borderRadius: 8,
    padding: 10,
  },
  tableWrap: {
    overflowX: "auto",
    border: "1px solid #334155",
    borderRadius: 8,
  },
  table: {
    width: "100%",
    borderCollapse: "collapse",
  },
  th: {
    textAlign: "left",
    padding: "10px 12px",
    borderBottom: "1px solid #334155",
    background: "#0b1220",
    color: "#93c5fd",
  },
  td: {
    padding: "10px 12px",
    borderBottom: "1px solid #1f2937",
    color: "#e2e8f0",
  },
  pre: {
    background: "transparent",
    color: "#a7f3d0",
    border: "1px solid #334155",
    borderRadius: 8,
    padding: 10,
    overflow: "auto",
  },
  modalOverlay: {
    position: "fixed",
    inset: 0,
    background: "rgba(0, 0, 0, 0.55)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 40,
  },
  modal: {
    width: "min(920px, 92vw)",
    maxHeight: "80vh",
    overflow: "auto",
    background: "#0f172a",
    border: "1px solid #334155",
    borderRadius: 10,
    padding: 14,
  },
  // --- samples styles ---
  samplesWrap: {
    display: "flex",
    gap: 10,
    flexWrap: "wrap",
  },
  sampleBtn: {
    background: "#111827",
    color: "#e2e8f0",
    border: "1px solid #334155",
    padding: "8px 12px",
    borderRadius: 999,
    cursor: "pointer",
    whiteSpace: "nowrap",
  },
};
