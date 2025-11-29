
import json, requests, streamlit as st
import snowflake.connector

# --- Page settings ---
st.set_page_config(page_title="Dumsor AI — Talk to your data", layout="wide")
st.title("Dumsor AI (Snowflake Cortex Analyst)")
st.caption("Ask natural-language questions about outages, traffic, water, public response, or hospital waits. The semantic model returns accurate SQL you can inspect and (optionally) execute.")

# --- Secrets (never hardcode credentials) ---
SF_ACCOUNT  = st.secrets["snowflake"]["account"]                 # e.g., ZEQWJME-NV17394
SF_TOKEN    = st.secrets["snowflake"]["token"]                   # PAT / OAuth token
MODEL_FILE  = st.secrets["snowflake"]["semantic_model_file"]     # e.g., @DUMSOR.REPORT.STG_DUMSOR_SEMANTIC_LAYER/REPORTING_WORKSTREAM_SEMANTIC_LAYER.yaml

# Optional: to execute the returned SQL
SF_USER      = st.secrets["snowflake"].get("user")
SF_PASSWORD  = st.secrets["snowflake"].get("password")
SF_ROLE      = st.secrets["snowflake"].get("role")
SF_WAREHOUSE = st.secrets["snowflake"].get("warehouse")
SF_DATABASE  = st.secrets["snowflake"].get("database")
SF_SCHEMA    = st.secrets["snowflake"].get("schema")

API_URL = f"https://{SF_ACCOUNT}.snowflakecomputing.com/api/v2/cortex/analyst/message"

def ask_analyst(question: str) -> dict:
    headers = {
        "Authorization": f"Bearer {SF_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
        "semantic_model_file": MODEL_FILE
    }
    r = requests.post(API_URL, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def run_sql(sql: str):
    """Execute SQL only if connector creds exist in secrets."""
    if not (SF_USER and SF_PASSWORD and SF_ROLE and SF_WAREHOUSE and SF_DATABASE and SF_SCHEMA):
        return None, None
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        role=SF_ROLE, warehouse=SF_WAREHOUSE, database=SF_DATABASE, schema=SF_SCHEMA,
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    cur.close(); conn.close()
    return cols, rows

# --- Chat history ---
if "history" not in st.session_state:
    st.session_state.history = []

for role, text in st.session_state.history:
    with st.chat_message(role):
        st.markdown(text)

prompt = st.chat_input("Ask, e.g., 'Top 10 sidechicks that do not return call, verified only'")
if prompt:
    st.session_state.history.append(("user", prompt))
    with st.chat_message("user"):
        st.markdown(prompt)

    try:
        result = ask_analyst(prompt)

        # Parse Analyst response: 'text', 'sql', 'suggestions'
        answer_text = ""
        sql_blocks = []
        for msg in result.get("messages", []):
            if msg.get("role") == "assistant":
                for c in msg.get("content", []):
                    if c.get("type") == "text":
                        answer_text += c.get("text", "") + "\n"
                    elif c.get("type") == "sql":
                        sql_blocks.append(c.get("sql", ""))

        with st.chat_message("assistant"):
            st.markdown(answer_text or "No narrative text returned.")
            if sql_blocks:
                st.markdown("**Generated SQL**")
                st.code(sql_blocks[0], language="sql")

                # Execute & display (if connector creds present)
                cols, rows = run_sql(sql_blocks[0])
                if cols and rows is not None:
                    st.markdown("**Results**")
                    st.dataframe(rows, use_container_width=True)
                else:
                    st.info("SQL execution disabled (no connector creds in secrets).")

        st.session_state.history.append(("assistant", answer_text or "(no text)"))

    except requests.HTTPError as e:
        st.error(f"Cortex API error: {e.response.text}")
    except Exception as ex:
        st.error(f"Unexpected error: {ex}")

st.divider()
st.caption("This app calls Snowflake Cortex Analyst (API-first) with your staged semantic model; it returns text + SQL for reproducible analytics.")
