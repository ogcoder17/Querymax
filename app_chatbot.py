# app_chatbot.py
import os
import json
import time
import requests
import gradio as gr

# ---------------------------
# CONFIG
# ---------------------------
API_BASE = os.getenv("QM_BASE", "http://127.0.0.1:8000")

def _pretty_table(result: dict) -> str:
    """
    Render backend /sql/run or /query result as a simple text table for the chatbot.
    Expects a dict like {"columns": [...], "rows": [[...], ...]} or {"result": {"columns": [...], "rows": [...]}}
    """
    if not result:
        return "(empty result)"
    # result from /sql/run is {"result": {"columns":[], "rows":[]}}
    if "result" in result and isinstance(result["result"], dict):
        data = result["result"]
    else:
        data = result

    cols = data.get("columns", [])
    rows = data.get("rows", [])
    if not cols:
        return "(no columns)"

    # build a simple monospaced table
    col_line = " | ".join(str(c) for c in cols)
    sep = "-+-".join("-" * len(str(c)) for c in cols)
    body = []
    for r in rows:
        body.append(" | ".join(str(x) for x in r))
    table = f"{col_line}\n{sep}\n" + ("\n".join(body) if body else "(no rows)")
    return table

def backend_health(base):
    try:
        r = requests.get(f"{base}/", timeout=5)
        if r.ok:
            data = r.json()
            return f"✅ Backend OK — {data.get('message','online')} (model: {data.get('model','?')})"
        return f"⚠️ Backend responded: HTTP {r.status_code}"
    except Exception as e:
        return f"❌ Backend not reachable at {base} — {e}"

def call_nl_to_sql(base, question: str) -> str:
    """
    Calls FastAPI /query endpoint with {"question": "..."} and returns pretty text.
    """
    try:
        r = requests.post(f"{base}/query", json={"question": question}, timeout=120)
        if not r.ok:
            return f"❌ /query error: HTTP {r.status_code}\n{r.text[:400]}"
        data = r.json()
        sql = data.get("sql", "")
        result = data.get("result", {})
        timing = data.get("timing", {})

        out = []
        out.append("**Generated SQL**\n```sql\n" + (sql or "(empty)") + "\n```")
        out.append("**Result**\n" + _pretty_table(result))
        if timing:
            out.append("**Timing (ms)**\n" + json.dumps(timing, indent=2))
        return "\n\n".join(out)
    except Exception as e:
        return f"❌ Exception calling /query: {e}"

def call_sql_run(base, sql: str) -> str:
    """
    Calls FastAPI /sql/run endpoint with {"sql": "..."} and returns pretty text.
    """
    try:
        r = requests.post(f"{base}/sql/run", json={"sql": sql}, timeout=120)
        if not r.ok:
            return f"❌ /sql/run error: HTTP {r.status_code}\n{r.text[:400]}"
        data = r.json()
        # /sql/run returns {"result": {"columns": [...], "rows": [...]}}
        return "**Result**\n" + _pretty_table(data)
    except Exception as e:
        return f"❌ Exception calling /sql/run: {e}"

def router_chat(message: str, history: list, api_base: str) -> tuple[str, list]:
    """
    Unified router:
    - If the user message starts with 'SQL:' or 'sql:' then we treat it as raw SQL and call /sql/run
    - Otherwise we treat it as NL and call /query
    Returns the assistant reply and updated history.
    """
    message = message.strip()
    if not message:
        return "Please type something.", history

    if message.lower().startswith("sql:"):
        sql = message[4:].strip()
        if not sql.endswith(";"):
            sql += ";"
        reply = call_sql_run(api_base, sql)
    else:
        reply = call_nl_to_sql(api_base, message)

    history = history + [[message, reply]]
    return "", history

def ui():
    with gr.Blocks(css="footer {visibility: hidden}") as demo:
        gr.Markdown("## 🧠 QueryMax Chatbot (NL→SQL + Raw SQL)\nAsk in natural language or start with `SQL:` to run raw SQL.")

        with gr.Row():
            api_base = gr.Textbox(label="FastAPI Base URL", value=API_BASE, interactive=True)
            btn_ping = gr.Button("Ping Backend", variant="secondary")
            health_box = gr.Markdown("")

        with gr.Row():
            chatbot = gr.Chatbot(height=480)
        with gr.Row():
            msg = gr.Textbox(label="Message", placeholder="e.g., Show all users from Hyderabad OR SQL: select * from users;")
            send = gr.Button("Send", variant="primary")
            clear = gr.Button("Clear")

        state = gr.State([])

        def on_ping(base):
            return backend_health(base)

        btn_ping.click(fn=on_ping, inputs=[api_base], outputs=[health_box])

        def on_send(user_message, hist, base):
            # router returns cleared input and updated history
            empty, new_hist = router_chat(user_message, hist or [], base)
            return empty, new_hist

        send.click(fn=on_send, inputs=[msg, state, api_base], outputs=[msg, state]).then(
            lambda h: h, inputs=[state], outputs=[chatbot]
        )
        clear.click(lambda: [], outputs=[state]).then(lambda: [], outputs=[chatbot])

    return demo

if __name__ == "__main__":
    demo = ui()
    # If 5176 is busy, you can change the port below.
    demo.queue().launch(server_name="0.0.0.0", server_port=5180, share=False)
