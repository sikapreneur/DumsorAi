
import json
import time
from typing import Dict, List, Optional

import requests
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

# --- Page settings ---
st.set_page_config(page_title="Dumsor AI", layout="wide")
st.title("Dumsor Ai — Talk to Your Data")
st.caption("Ask questions about Dumsor (power outages), Time wasted in traffic, Water supply disruption, Public agency response time, or Hospital waiting time in Ghana.")

# --- Secrets ---
SF_ACCOUNT = st.secrets["snowflake"]["account"]
SF_TOKEN = st.secrets["snowflake"]["token"]
MODEL_FILE = st.secrets["snowflake"]["semantic_model_file"]  # e.g., @DUMSOR.REPORT.STG_DUMSOR_SEMANTIC_LAYER/REPORTING_WORKSTREAM_SEMANTIC_LAYER.yaml

# Optional Snowflake creds (not used now, but kept for future)
SF_USER = st.secrets["snowflake"].get("user")
SF_PASSWORD = st.secrets["snowflake"].get("password")
SF_ROLE = st.secrets["snowflake"].get("role")
SF_WAREHOUSE = st.secrets["snowflake"].get("warehouse")
SF_DATABASE = st.secrets["snowflake"].get("database")
SF_SCHEMA = st.secrets["snowflake"].get("schema")

API_URL = f"https://{SF_ACCOUNT}.snowflakecomputing.com/api/v2/cortex/analyst/message"

connection_params = {
    "account": SF_ACCOUNT,
    "user": SF_USER,
    "password": SF_PASSWORD,
    "role": SF_ROLE,
    "warehouse": SF_WAREHOUSE,
    "database": SF_DATABASE,
    "schema": SF_SCHEMA
}

# Initialize Snowpark session (optional for future use)
session = Session.builder.configs(connection_params).create()

# --- Cortex Analyst API Call ---
def ask_analyst(messages: List[Dict]) -> dict:
    headers = {
        "Authorization": f"Bearer {SF_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messages": messages,  # ✅ Full conversation history
        "semantic_model_file": MODEL_FILE if MODEL_FILE.startswith("@") else f"@{MODEL_FILE}",
        "debug": True
    }
    resp = requests.post(API_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()

# --- Chat History ---
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display previous messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        for item in msg["content"]:
            if item["type"] == "text":
                st.markdown(item["text"])

# --- Chat Input ---
prompt = st.chat_input("Ask, e.g., 'How much time was wasted in traffic last month?'")
if prompt:
    user_msg = {"role": "user", "content": [{"type": "text", "text": prompt}]}
    st.session_state.messages.append(user_msg)
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):
            try:
                result = ask_analyst(st.session_state.messages)

                # ✅ Show debug info
                if "debug_info" in result:
                    st.markdown("### Debug Info")
                    st.json(result["debug_info"])

                # ✅ Show error if present
                if "error" in result:
                    st.error(f"Error: {result['error'].get('message', 'Unknown error')}")

                # ✅ Optional raw response
                if st.checkbox("Show raw API response"):
                    st.json(result)

                # ✅ Only parse narrative text
                answer_text = ""
                if "message" in result:
                    for item in result["message"]["content"]:
                        if item["type"] == "text":
                            answer_text += item["text"] + "\n"

                if answer_text.strip():
                    st.markdown(answer_text)
                else:
                    st.warning("⚠️ No narrative text returned. Check Debug Info above.")

                # Save analyst message
                analyst_msg = {
                    "role": "analyst",
                    "content": result.get("message", {}).get("content", []),
                    "request_id": result.get("request_id")
                }
                st.session_state.messages.append(analyst_msg)

            except requests.HTTPError as e:
                st.error(f"Cortex API error: {e.response.status_code} - {e.response.text}")
            except Exception as ex:
                st.error(f"Unexpected error: {str(ex)}")

st.divider()
st.caption("Kaunda | @2025 | kaunda@outlook.com | Dumsor.org | Facebook.com/KaundaAi")
# st.caption("This app uses Snowflake Cortex Analyst with your semantic model to return narrative text, SQL, and optional visualizations.")

# Belows script shows both narrative and sql code

# import json
# import time
# from typing import Dict, List, Optional, Tuple

# import requests
# import pandas as pd
# import streamlit as st
# import snowflake.connector
# from snowflake.snowpark.context import get_active_session
# from snowflake.snowpark.exceptions import SnowparkSQLException
# from snowflake.snowpark import Session



# # --- Page settings ---
# st.set_page_config(page_title="Dumsor AI", layout="wide")
# st.title("Dumsor Ai — Talk to Your Data")
# st.caption("Ask questions about Dumsor (power outages), Time wasted in traffic, Water supply disruption , Public agency response time, or Hospital waiting time in Ghana.")

# # --- Secrets ---
# SF_ACCOUNT = st.secrets["snowflake"]["account"]
# SF_TOKEN = st.secrets["snowflake"]["token"]
# MODEL_FILE = st.secrets["snowflake"]["semantic_model_file"]  # e.g., @DUMSOR.REPORT.STG_DUMSOR_SEMANTIC_LAYER/REPORTING_WORKSTREAM_SEMANTIC_LAYER.yaml

# # Optional Snowflake creds for SQL execution
# SF_USER = st.secrets["snowflake"].get("user")
# SF_PASSWORD = st.secrets["snowflake"].get("password")
# SF_ROLE = st.secrets["snowflake"].get("role")
# SF_WAREHOUSE = st.secrets["snowflake"].get("warehouse")
# SF_DATABASE = st.secrets["snowflake"].get("database")
# SF_SCHEMA = st.secrets["snowflake"].get("schema")

# API_URL = f"https://{SF_ACCOUNT}.snowflakecomputing.com/api/v2/cortex/analyst/message"

# connection_params = {
#     "account": SF_ACCOUNT,
#     "user": SF_USER,
#     "password": SF_PASSWORD,
#     "role": SF_ROLE,
#     "warehouse": SF_WAREHOUSE,
#     "database": SF_DATABASE,
#     "schema": SF_SCHEMA
# }

# session = Session.builder.configs(connection_params).create()

# # session = get_active_session()  # For Snowpark-based summarization

# # --- Cortex Analyst API Call ---
# def ask_analyst(messages: List[Dict]) -> dict:
#     headers = {
#         "Authorization": f"Bearer {SF_TOKEN}",
#         "Content-Type": "application/json"
#     }
#     payload = {
#         "messages": messages,  # ✅ Full conversation history
#         "semantic_model_file": MODEL_FILE if MODEL_FILE.startswith("@") else f"@{MODEL_FILE}",
#         "debug": True
#     }
#     resp = requests.post(API_URL, headers=headers, json=payload, timeout=60)
#     resp.raise_for_status()
#     return resp.json()

# # --- SQL Execution ---
# def run_sql(sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
#     if not (SF_USER and SF_PASSWORD and SF_ROLE and SF_WAREHOUSE and SF_DATABASE and SF_SCHEMA):
#         return None, "Missing Snowflake credentials"
#     try:
#         conn = snowflake.connector.connect(
#             account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
#             role=SF_ROLE, warehouse=SF_WAREHOUSE, database=SF_DATABASE, schema=SF_SCHEMA,
#         )
#         cur = conn.cursor()
#         cur.execute(sql)
#         rows = cur.fetchall()
#         cols = [c[0] for c in cur.description]
#         cur.close(); conn.close()
#         return pd.DataFrame(rows, columns=cols), None
#     except Exception as e:
#         return None, str(e)

# # --- Summarize Results using Cortex ---
# def summarize_results(sql: str) -> Optional[str]:
#     try:
#         summary_sql = f"""
#         SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
#             'Summarize results, show trends & itemize top insights from this JSON data in <150 words. Data: ' ||
#             (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))::STRING FROM ({sql.replace(";", "")}))
#         ) AS Insights
#         """
#         df, err = run_sql(summary_sql)
#         if df is not None and not df.empty:
#             return df.iloc[0, 0]
#     except Exception:
#         return None
#     return None

# # --- Chat History ---
# if "messages" not in st.session_state:
#     st.session_state.messages = []

# for msg in st.session_state.messages:
#     with st.chat_message(msg["role"]):
#         for item in msg["content"]:
#             if item["type"] == "text":
#                 st.markdown(item["text"])
#             elif item["type"] == "sql":
#                 st.code(item.get("statement", ""), language="sql")

# # --- Chat Input ---
# prompt = st.chat_input("Ask, e.g., 'How much time was wasted in traffic last month?'")
# if prompt:
#     user_msg = {"role": "user", "content": [{"type": "text", "text": prompt}]}
#     st.session_state.messages.append(user_msg)
#     with st.chat_message("user"):
#         st.markdown(prompt)

#     with st.chat_message("analyst"):
#         with st.spinner("Waiting for Analyst's response..."):
#             try:
#                 result = ask_analyst(st.session_state.messages)

#                 # ✅ Show debug info
#                 if "debug_info" in result:
#                     st.markdown("### Debug Info")
#                     st.json(result["debug_info"])

#                 # ✅ Show error if present
#                 if "error" in result:
#                     st.error(f"Error: {result['error'].get('message', 'Unknown error')}")

#                 # ✅ Optional raw response
#                 if st.checkbox("Show raw API response"):
#                     st.json(result)

#                 # Parse narrative text & SQL
#                 answer_text = ""
#                 sql_statement = None
#                 if "message" in result:
#                     for item in result["message"]["content"]:
#                         if item["type"] == "text":
#                             answer_text += item["text"] + "\n"
#                         elif item["type"] == "sql":
#                             sql_statement = item.get("statement")

#                 if answer_text.strip():
#                     st.markdown(answer_text)
#                 else:
#                     st.warning("⚠️ No narrative text returned. Check Debug Info above.")

#                 # Display SQL and execute
#                 if sql_statement:
#                     st.markdown("**Generated SQL**")
#                     st.code(sql_statement, language="sql")

#                     df, err = run_sql(sql_statement)
#                     if df is not None:
#                         # Summarize if small dataset
#                         if len(df) <= 20:
#                             summary = summarize_results(sql_statement)
#                             if summary:
#                                 st.markdown(f"### AI Summary\n{summary}")

#                         # Tabs for data & charts
#                         data_tab, chart_tab = st.tabs(["Data 📄", "Chart 📈"])
#                         with data_tab:
#                             st.dataframe(df, use_container_width=True)
#                         with chart_tab:
#                             if len(df.columns) >= 2:
#                                 x_col = st.selectbox("X axis", df.columns)
#                                 y_col = st.selectbox("Y axis", [c for c in df.columns if c != x_col])
#                                 chart_type = st.radio("Chart type", ["Line", "Bar"])
#                                 if chart_type == "Line":
#                                     st.line_chart(df.set_index(x_col)[y_col])
#                                 else:
#                                     st.bar_chart(df.set_index(x_col)[y_col])
#                             else:
#                                 st.write("Not enough columns for chart.")
#                     else:
#                         st.error(f"SQL execution failed: {err}")

#                 # Save analyst message
#                 analyst_msg = {
#                     "role": "analyst",
#                     "content": result.get("message", {}).get("content", []),
#                     "request_id": result.get("request_id")
#                 }
#                 st.session_state.messages.append(analyst_msg)

#             except requests.HTTPError as e:
#                 st.error(f"Cortex API error: {e.response.status_code} - {e.response.text}")
#             except Exception as ex:
#                 st.error(f"Unexpected error: {str(ex)}")

# st.divider()
# st.caption("Kaunda | @2025 | kaunda@outlook.com | Dumsor.org | Facebook.com/KaundaAi")
# # st.caption("This app uses Snowflake Cortex Analyst with your semantic model to return narrative text, SQL, and optional visualizations.")