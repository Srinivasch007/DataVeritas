"""
DataVeritas - A Streamlit dashboard app
"""
import io
import json
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

try:
    from shareplum import Site, Office365
    from shareplum.site import Version
    SHAREPOINT_AVAILABLE = True
except ImportError:
    SHAREPOINT_AVAILABLE = False

from recon import render as render_recon
from dmc import render as render_dmc
from data_explorer import render as render_data_explorer
from read_me import render as render_read_me
import streamlit.components.v1 as components

st.set_page_config(
    page_title="DataVeritas",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Outfit:wght@300;500;700&display=swap');
    
    .main {
        background: linear-gradient(135deg, #0f0f23 0%, #1a1a2e 50%, #16213e 100%);
        font-family: 'Outfit', sans-serif;
    }
    
    [data-testid="stSidebar"] {
        min-width: 260px;
        background: #ECF0F1;
        border-right: 1px solid rgba(0,0,0,0.1);
    }
    
    [data-testid="stSidebar"] *, [data-testid="stSidebar"] label, [data-testid="stSidebar"] p, [data-testid="stSidebar"] input {
        color: #000000 !important;
    }
    
    [data-testid="stSidebar"] input::placeholder {
        color: #333333 !important;
    }
    
    [data-testid="stSidebar"] .stSelectbox, [data-testid="stSidebar"] .stTextInput {
        padding: 0;
        max-width: 180px !important;
    }
    [data-testid="stSidebar"] [data-testid="stSelectbox"] > div {
        max-width: 180px !important;
    }
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {
        padding-top: 0.1rem !important;
        padding-bottom: 0.1rem !important;
    }
    [data-testid="stSidebar"] [data-testid="stHorizontalBlock"] {
        gap: 0.1rem !important;
        align-items: center !important;
    }
    [data-testid="stSidebar"] .stExpander {
        margin: 0.15rem 0 !important;
    }
    [data-testid="stSidebar"] .stButton > button {
        padding: 2px 30px 2px 12px !important;
        font-size: 1rem !important;
        font-weight: 400 !important;
        min-height: 30px !important;
        width: 100% !important;
        white-space: nowrap !important;
        text-align: left !important;
        display: flex !important;
        justify-content: flex-start !important;
        align-items: center !important;
        background-color: #87cefa !important;
        color: #000000 !important;
        margin-left: 12px !important;
    }
    [data-testid="stSidebar"] button {
        background-color: #87cefa !important;
        color: #000000 !important;
        border: 1px solid #6fbfe6 !important;
        font-weight: 400 !important;
    }
    [data-testid="stSidebar"] button:hover {
        background-color: #87cefa !important;
        color: #000000 !important;
    }
    [data-testid="stSidebar"] .stButton > button p {
        font-weight: 400 !important;
        text-align: left !important;
        color: #000000 !important;
    }
    
    .stExpander {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 8px;
        margin: 0.25rem 0;
    }
    
    h1 {
        font-family: 'Outfit', sans-serif !important;
        font-weight: 700 !important;
        background: linear-gradient(90deg, #00d4ff, #7c3aed);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    .stMetric { background: rgba(255, 255, 255, 0.03); padding: 1rem; border-radius: 10px; border: 1px solid rgba(255, 255, 255, 0.08); }
    .stat-card {
        background: #1f1f1f;
        border: 1px solid #2a2a2a;
        border-radius: 12px;
        padding: 1rem 1.1rem;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2);
        min-height: 110px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        text-align: center;
    }
    .stat-label { font-size: 0.95rem; font-weight: 700; color: #f5a623; }
    .stat-value { font-size: 2.1rem; font-weight: 700; color: #ffffff; margin-top: 0.35rem; }
    
    .main .stButton > button {
        padding: 10px 20px !important;
        border-radius: 8px !important;
        font-weight: 700 !important;
        width: 70% !important;
        margin: 0 auto !important;
    }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2 {
        font-size: 1.9rem !important;
        font-weight: 700 !important;
        color: #000000 !important;
    }
    [data-testid="stSidebar"] h1 span,
    [data-testid="stSidebar"] h2 span,
    [data-testid="stSidebar"] h1 a,
    [data-testid="stSidebar"] h2 a,
    [data-testid="stSidebar"] .stMarkdown h1,
    [data-testid="stSidebar"] .stMarkdown h2 {
        color: #000000 !important;
    }
    @media print {
        [data-testid="stSidebar"] { display: none !important; }
        .main { max-width: 100% !important; }
    }
</style>
""", unsafe_allow_html=True)

# Initialize page in session state
if "page" not in st.session_state:
    st.session_state["page"] = "Orchestrator"


def _run_recon_single(source_db, source_table, target_db, target_table, config):
    """Run source/target comparison for one row. Returns result dict or error string."""
    from db_connector import connect_db, get_table_columns, fetch_table_data
    src_conn, err = connect_db(source_db, config)
    if err:
        return {"error": f"Source DB: {err}"}
    tgt_conn, err = connect_db(target_db, config)
    if err:
        try:
            src_conn.close()
        except Exception:
            pass
        return {"error": f"Target DB: {err}"}
    try:
        src_cols, err = get_table_columns(src_conn, source_db, source_table)
        if err:
            return {"error": f"Source table columns: {err}"}
        tgt_cols, err = get_table_columns(tgt_conn, target_db, target_table)
        if err:
            return {"error": f"Target table columns: {err}"}
        tgt_lower = {c.lower(): c for c in tgt_cols}
        matching = []
        not_in_target = []
        for c in src_cols:
            if c.lower() in tgt_lower:
                matching.append(tgt_lower[c.lower()])
            else:
                not_in_target.append(c)
        src_df, tgt_df = pd.DataFrame(), pd.DataFrame()
        if matching:
            matching_src_cols = [c for c in src_cols if c.lower() in tgt_lower]
            src_df, err = fetch_table_data(src_conn, source_db, source_table, matching_src_cols, limit=100)
            if err:
                return {"error": f"Fetch source data: {err}"}
            src_df = src_df if src_df is not None else pd.DataFrame()
            tgt_df, err = fetch_table_data(tgt_conn, target_db, target_table, matching, limit=100)
            if err:
                return {"error": f"Fetch target data: {err}"}
            tgt_df = tgt_df if tgt_df is not None else pd.DataFrame()
        return {
            "source_db": source_db, "source_table": source_table,
            "target_db": target_db, "target_table": target_table,
            "matching_columns": matching, "columns_not_in_target": not_in_target,
            "source_df": src_df, "target_df": tgt_df,
        }
    finally:
        try:
            src_conn.close()
        except Exception:
            pass
        try:
            tgt_conn.close()
        except Exception:
            pass


def _run_recon():
    """Run source/target comparison. If Excel uploaded, run for each row; else run for manual entry."""
    st.session_state.pop("recon_error", None)
    st.session_state.pop("recon_results", None)
    st.session_state.pop("recon_source_df", None)
    st.session_state.pop("recon_target_df", None)
    st.session_state.pop("recon_matching_columns", None)
    st.session_state.pop("recon_columns_not_in_target", None)
    CONFIG_PATH = Path(__file__).parent / "config.json"
    if not CONFIG_PATH.exists():
        st.session_state["recon_error"] = "config.json not found. Add config in Orchestrator or project folder."
        return
    try:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        st.session_state["recon_error"] = f"Invalid config: {e}"
        return
    recon_df = st.session_state.get("recon_excel_df")
    if recon_df is not None and not recon_df.empty:
        col_map = {str(c).strip().lower().replace(" ", "_"): c for c in recon_df.columns}
        sno_col = col_map.get("sno") or next((c for c in recon_df.columns if "sno" in str(c).lower()), None)
        src_db_col = col_map.get("source_database") or next((c for c in recon_df.columns if "source" in str(c).lower() and "database" in str(c).lower()), None)
        src_tbl_col = col_map.get("source_table_nm") or next((c for c in recon_df.columns if "source" in str(c).lower() and "table" in str(c).lower()), None)
        tgt_db_col = col_map.get("target_database") or next((c for c in recon_df.columns if "target" in str(c).lower() and "database" in str(c).lower()), None)
        tgt_tbl_col = col_map.get("target_table_nm") or next((c for c in recon_df.columns if "target" in str(c).lower() and "table" in str(c).lower()), None)
        if not all([sno_col, src_db_col, src_tbl_col, tgt_db_col, tgt_tbl_col]):
            st.session_state["recon_error"] = "Excel missing columns: SNO, Source_database, source_table_nm, target_database, target_table_nm"
            return
        results = []
        for idx, row in recon_df.iterrows():
            sno = int(row[sno_col]) if pd.notna(row[sno_col]) else idx + 1
            source_db = str(row[src_db_col]).strip() if pd.notna(row[src_db_col]) else "Netezza"
            source_table = str(row[src_tbl_col]).strip() if pd.notna(row[src_tbl_col]) else ""
            target_db = str(row[tgt_db_col]).strip() if pd.notna(row[tgt_db_col]) else "Netezza"
            target_table = str(row[tgt_tbl_col]).strip() if pd.notna(row[tgt_tbl_col]) else ""
            if not source_table or not target_table:
                results.append({"sno": sno, "error": "Missing source or target table name"})
                continue
            r = _run_recon_single(source_db, source_table, target_db, target_table, config)
            r["sno"] = sno
            results.append(r)
        st.session_state["recon_results"] = results
    else:
        source_db = st.session_state.get("recon_source_db", "Netezza")
        source_table = st.session_state.get("recon_source_table", "").strip()
        target_db = st.session_state.get("recon_target_db", "Netezza")
        target_table = st.session_state.get("recon_target_table", "").strip()
        if not source_table or not target_table:
            st.session_state["recon_error"] = "Enter both source and target table names."
            return
        r = _run_recon_single(source_db, source_table, target_db, target_table, config)
        if "error" in r:
            st.session_state["recon_error"] = r["error"]
        else:
            r["sno"] = 1
            st.session_state["recon_results"] = [r]


def _get_dmc_config_key(db_type):
    """Map database type to config key for db_connector."""
    m = {"Netezza": "netezza", "Snowflake": "snowflake", "SQL Server": "sql_server",
         "PostgreSQL": "postgresql", "MySQL": "mysql", "Oracle": "oracle"}
    return m.get(db_type, db_type.lower().replace(" ", "_"))


def _find_dmc_col(df, patterns):
    """Find column matching any of the patterns (case-insensitive)."""
    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    for p in patterns:
        pk = p.lower().replace(" ", "_")
        if pk in cols_lower:
            return cols_lower[pk]
        for c in df.columns:
            if pk in str(c).lower().replace(" ", "_"):
                return c
    return None


def _run_dmc():
    """Build UNION ALL count queries grouped by Hop Name from uploaded DMC Excel."""
    st.session_state.pop("dmc_error", None)
    st.session_state.pop("dmc_queries", None)
    st.session_state.pop("dmc_results", None)
    st.session_state.pop("dmc_final_df", None)
    df = st.session_state.get("dmc_excel_df")
    if df is None or df.empty:
        st.session_state["dmc_error"] = "Upload an Excel file first."
        return
    hop_col = _find_dmc_col(df, ["Hop Name", "HopName", "Hop"])
    tbl_col = _find_dmc_col(df, ["Table Name", "TableName", "Table"])
    schema_col = _find_dmc_col(df, ["Schema Name", "SchemaName", "Schema"])
    filter_col = _find_dmc_col(df, ["Filter Col 1", "Filter Col1", "Filter_Col_1"])
    filter_val_col = _find_dmc_col(df, ["Filter Col 1 Val", "Filter Col1 Val", "Filter_Col_1_Val"])
    if not all([hop_col, tbl_col, schema_col]):
        st.session_state["dmc_error"] = "Excel must have columns: Hop Name, Table Name, Schema Name"
        return

    def _quote_sql_val(val):
        """Quote value for SQL: numeric unquoted, else single-quoted with escape."""
        if pd.isna(val) or val == "":
            return None
        s = str(val).strip()
        try:
            float(s)
            return s
        except (ValueError, TypeError):
            return "'" + s.replace("'", "''") + "'"

    queries = {}
    for hop_name, grp in df.groupby(hop_col):
        hop_val = str(hop_name).strip() if pd.notna(hop_name) else "Unknown"
        parts = []
        for _, row in grp.iterrows():
            schema = str(row[schema_col]).strip() if pd.notna(row[schema_col]) else ""
            table = str(row[tbl_col]).strip() if pd.notna(row[tbl_col]) else ""
            if schema and table:
                full_name = f'"{schema}"."{table}"'
                base = f"SELECT '{table}' AS tablename, COUNT(*) FROM {full_name}"
                if filter_col and filter_val_col:
                    fcol = str(row[filter_col]).strip() if pd.notna(row[filter_col]) else ""
                    fval = _quote_sql_val(row[filter_val_col])
                    if fcol and fval is not None:
                        base += f' WHERE "{fcol}" = {fval}'
                parts.append(base)
        if parts:
            queries[hop_val] = "\nUNION ALL\n".join(parts)
    st.session_state["dmc_queries"] = queries

    hop_config = st.session_state.get("dmc_hop_config", {})
    results = {}
    if hop_config:
        from db_connector import connect_db, run_query
        for hop_name, sql in queries.items():
            cfg = hop_config.get(hop_name)
            if not cfg:
                results[hop_name] = {"sql": sql, "df": None, "error": f"No database config for '{hop_name}'"}
                continue
            db_type = cfg.get("database_type", "Netezza")
            db_config = {"databases": {db_type.lower().replace(" ", "_"): cfg}}
            conn, err = connect_db(db_type, {"databases": {_get_dmc_config_key(db_type): cfg}})
            if err:
                results[hop_name] = {"sql": sql, "df": None, "error": err}
                continue
            try:
                qdf = run_query(conn, db_type, sql)
                if qdf is not None and not qdf.empty:
                    tbl_col_name = next((c for c in qdf.columns if "table" in str(c).lower()), qdf.columns[0])
                    cnt_col_name = next((c for c in qdf.columns if c != tbl_col_name), qdf.columns[1] if len(qdf.columns) > 1 else None)
                    qdf = qdf.copy()
                    qdf = qdf.rename(columns={tbl_col_name: "tablename"})
                    if cnt_col_name and cnt_col_name in qdf.columns:
                        qdf = qdf.rename(columns={cnt_col_name: "count"})
                    elif len(qdf.columns) >= 2:
                        qdf = qdf.rename(columns={qdf.columns[1]: "count"})
                    qdf["Grouping"] = qdf["tablename"].apply(lambda t: table_to_grouping.get((hop_name, str(t).strip()), "Other"))
                    cols = ["Grouping", "tablename"] + (["count"] if "count" in qdf.columns else [])
                    qdf = qdf[[c for c in cols if c in qdf.columns]]
                results[hop_name] = {"sql": sql, "df": qdf, "error": None}
            except Exception as e:
                results[hop_name] = {"sql": sql, "df": None, "error": str(e)}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
    else:
        for hop_name, sql in queries.items():
            results[hop_name] = {"sql": sql, "df": None, "error": "Upload config (dmc_config) to execute queries."}

    all_rows = []
    for hop_name, r in results.items():
        if r.get("df") is not None and not r["df"].empty:
            df_part = r["df"].copy()
            if "tablename" in df_part.columns and "count" in df_part.columns:
                all_rows.append(df_part[["Grouping", "tablename", "count"]])
            elif "Grouping" in df_part.columns:
                all_rows.append(df_part)
    dmc_final = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(columns=["Grouping", "tablename", "count"])
    st.session_state["dmc_results"] = results
    st.session_state["dmc_final_df"] = dmc_final


def _render_stat_card(label, value, icon):
    """Render a small stat card with icon."""
    st.markdown(
        f"""
        <div class="stat-card">
            <div class="stat-label">{icon} {label}</div>
            <div class="stat-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# Sidebar - Title and Orchestrator controls
with st.sidebar:
    st.markdown("# DataVeritas")
    st.markdown("---")
    
    if st.session_state.get("page") == "Orchestrator":
        with st.expander("Config", expanded=False):
            config_mode = st.radio("Config", ["Default", "Upload"], label_visibility="collapsed", key="config_mode", horizontal=True)
            CONFIG_PATH = Path(__file__).parent / "config.json"

            if config_mode == "Default":
                st.caption("Reads config.json from project folder")
                try:
                    if not CONFIG_PATH.exists():
                        st.session_state["orc_config_error"] = "config.json not found in project folder"
                    else:
                        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
                        st.session_state["orc_config"] = config
                        cfg = config
                        if "network" in cfg and "folder_path" in cfg["network"]:
                            st.session_state["orc_network_path"] = cfg["network"]["folder_path"]
                        elif "network_locations" in cfg and cfg["network_locations"]:
                            st.session_state["orc_network_path"] = cfg["network_locations"][0]
                        if "sharepoint" in cfg:
                            sp = cfg["sharepoint"]
                            for k, v in [("site_url", "sp_site"), ("library", "sp_lib"), ("username", "sp_user"), ("password", "sp_pwd")]:
                                if k in sp:
                                    st.session_state[v] = sp[k]
                        st.session_state.pop("orc_config_error", None)
                except json.JSONDecodeError as e:
                    st.session_state["orc_config_error"] = str(e)
                except Exception as e:
                    st.session_state["orc_config_error"] = str(e)
            else:
                config_upload = st.file_uploader("Upload config", type=["json"], key="config_upload", label_visibility="collapsed")
                if config_upload:
                    try:
                        config = json.load(config_upload)
                        st.session_state["orc_config"] = config
                        cfg = config
                        if "network" in cfg and "folder_path" in cfg["network"]:
                            st.session_state["orc_network_path"] = cfg["network"]["folder_path"]
                        elif "network_locations" in cfg and cfg["network_locations"]:
                            st.session_state["orc_network_path"] = cfg["network_locations"][0]
                        if "sharepoint" in cfg:
                            sp = cfg["sharepoint"]
                            for k, v in [("site_url", "sp_site"), ("library", "sp_lib"), ("username", "sp_user"), ("password", "sp_pwd")]:
                                if k in sp:
                                    st.session_state[v] = sp[k]
                        st.session_state.pop("orc_config_error", None)
                        st.success("Loaded")
                    except json.JSONDecodeError as e:
                        st.error(f"Invalid JSON: {e}")
                    except Exception as e:
                        st.error(str(e))
            if "orc_config_error" in st.session_state:
                st.error(st.session_state["orc_config_error"])
            if "orc_config" in st.session_state:
                st.caption("Config ready")

        with st.expander("Database", expanded=False):
            col_db, col_btn = st.columns([3, 1], gap="small")
            with col_db:
                orchestrator_database = st.selectbox(
                    "Database",
                    ["Netezza", "Snowflake", "SQL Server", "PostgreSQL", "MySQL", "Oracle"],
                    label_visibility="visible",
                    key="orchestrator_database",
                    index=0
                )
            with col_btn:
                db_connect_clicked = st.button("Connect", key="db_connect")
            if db_connect_clicked:
                if "orc_config" not in st.session_state:
                    st.session_state["orc_db_error"] = "Upload config first."
                else:
                    try:
                        from db_connector import connect_db
                        conn, err = connect_db(orchestrator_database, st.session_state["orc_config"])
                        if err:
                            st.session_state["orc_db_error"] = err
                            st.session_state["orc_db_conn"] = None
                        else:
                            st.session_state["orc_db_conn"] = conn
                            st.session_state.pop("orc_db_error", None)
                            st.success("Connected")
                    except Exception as e:
                        st.session_state["orc_db_error"] = str(e)
                        st.session_state["orc_db_conn"] = None
            if "orc_db_error" in st.session_state:
                st.error(st.session_state["orc_db_error"])
            elif "orc_db_conn" in st.session_state and st.session_state["orc_db_conn"]:
                st.caption("Connected")

        st.toggle(
            "Proceed after Row Count fail",
            value=True,
            key="orc_proceed_on_row_count_fail",
            help="If off, stop processing remaining test cases after a row count validation fails.",
        )

        with st.expander("Source", expanded=False):
            orchestrator_source = st.selectbox(
                "Source",
                ["Network Folder", "SharePoint", "Upload"],
                label_visibility="visible",
                key="orchestrator_source"
            )
            source = orchestrator_source

            if source == "Network Folder":
                col_path, col_list = st.columns([3, 1], gap="small")
                with col_path:
                    folder_path = st.text_input("Folder", placeholder=r"\\server\share\reports", label_visibility="collapsed", key="orc_network_path")
                with col_list:
                    list_clicked = st.button("List", key="list_network")
                if list_clicked and folder_path and folder_path.strip():
                    try:
                        p = Path(folder_path.strip())
                        if not p.is_dir():
                            st.session_state["orc_excel_error"] = "Not a valid folder."
                            st.session_state.pop("orc_network_files", None)
                        else:
                            xlsx_files = sorted(p.glob("*.xlsx"))
                            if not xlsx_files:
                                st.session_state["orc_excel_error"] = "No .xlsx files."
                                st.session_state.pop("orc_network_files", None)
                            else:
                                st.session_state["orc_network_files"] = [str(f) for f in xlsx_files]
                                st.session_state.pop("orc_excel_error", None)
                    except Exception as e:
                        st.session_state["orc_excel_error"] = str(e)
                        st.session_state.pop("orc_network_files", None)
                if "orc_network_files" in st.session_state:
                    file_paths = st.session_state["orc_network_files"]
                    file_names = [Path(p).name for p in file_paths]
                    col_sel, col_load = st.columns([2.5, 1], gap="small")
                    with col_sel:
                        selected_name = st.selectbox("File", file_names, key="orc_network_file_select", label_visibility="collapsed")
                    selected_file = next((p for p in file_paths if Path(p).name == selected_name), None) if selected_name else None
                    with col_load:
                        load_clicked = st.button("Load", key="load_network")
                else:
                    selected_file = None
                    load_clicked = False

            elif source == "SharePoint":
                if not SHAREPOINT_AVAILABLE:
                    st.warning("pip install SharePlum")
                else:
                    with st.form("sharepoint_form"):
                        site_url = st.text_input("Site URL", placeholder="https://...sharepoint.com/sites/...", key="sp_site")
                        library = st.text_input("Library", value="Shared Documents", key="sp_lib")
                        sp_file_path = st.text_input("File path", placeholder="Folder/file.xlsx", key="sp_path")
                        username = st.text_input("Username", placeholder="user@domain.com", key="sp_user")
                        password = st.text_input("Password", type="password", key="sp_pwd")
                        sp_submitted = st.form_submit_button("Load")
                    if sp_submitted and site_url and sp_file_path and username and password:
                        try:
                            authcookie = Office365(site_url.split("/sites/")[0], username=username, password=password).GetCookies()
                            site = Site(site_url, version=Version.v2016, authcookie=authcookie)
                            folder = site.Folder(library)
                            file_bytes = folder.get_file(sp_file_path)
                            st.session_state["orc_excel_data"] = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
                            st.session_state["orc_excel_filename"] = Path(sp_file_path).stem
                            st.session_state.pop("orc_execute_clicked", None)
                            st.session_state.pop("orc_excel_error", None)
                        except Exception as e:
                            st.session_state["orc_excel_error"] = str(e)
                    elif sp_submitted:
                        st.session_state["orc_excel_error"] = "Fill all fields."

            else:
                uploaded = st.file_uploader("File", type=["xlsx", "xls"], key="orc_upload", label_visibility="collapsed")
                if uploaded:
                    try:
                        st.session_state["orc_excel_data"] = pd.read_excel(uploaded, sheet_name=None)
                        st.session_state["orc_excel_filename"] = Path(uploaded.name).stem
                        st.session_state.pop("orc_execute_clicked", None)
                        st.session_state.pop("orc_excel_error", None)
                    except Exception as e:
                        st.session_state["orc_excel_error"] = str(e)

            if source == "Network Folder" and load_clicked and selected_file:
                try:
                    st.session_state["orc_excel_data"] = pd.read_excel(selected_file, sheet_name=None)
                    st.session_state["orc_excel_filename"] = Path(selected_file).stem
                    st.session_state.pop("orc_execute_clicked", None)
                    st.session_state.pop("orc_excel_error", None)
                except Exception as e:
                    st.session_state["orc_excel_error"] = str(e)

            if "orc_excel_data" in st.session_state:
                df_all = st.session_state["orc_excel_data"]
                if isinstance(df_all, dict):
                    sheet_list = list(df_all.keys())
                    sheet_options = (["ALL"] + sheet_list) if len(sheet_list) > 1 else sheet_list
                    st.session_state["orc_selected_sheet"] = st.selectbox("Select sheet", sheet_options, key="orc_sheet_select", label_visibility="visible")
                st.markdown("---")
                if st.button("Execute", key="orc_execute_tests", type="primary", use_container_width=True):
                    st.session_state["orc_execute_clicked"] = True
                    st.rerun()
        components.html(
            '''<button onclick="try{window.top.print()}catch(e){window.print()}" style="padding:0.4rem 0.8rem;background:#87cefa;color:#000000;
            border:1px solid #6fbfe6;border-radius:6px;cursor:pointer;font-weight:400;font-size:0.85rem;width:100%;">Download PDF</button>''',
            height=40
        )

    elif st.session_state.get("page") == "Recon":
        db_options = ["Netezza", "Snowflake", "SQL Server", "PostgreSQL", "MySQL", "Oracle"]
        recon_mode = st.radio("Input", ["Manual", "Upload Excel"], key="recon_mode", horizontal=True, label_visibility="collapsed")
        if recon_mode == "Upload Excel":
            template_path = Path(__file__).parent / "recon_template.xlsx"
            if template_path.exists():
                with open(template_path, "rb") as f:
                    st.download_button("Download template", data=f.read(), file_name="recon_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="recon_dl_template")
            recon_file = st.file_uploader("Upload recon file", type=["xlsx", "xls"], key="recon_upload", label_visibility="collapsed")
            if recon_file:
                try:
                    recon_df = pd.read_excel(recon_file, sheet_name=0)
                    col_map = {str(c).strip().lower().replace(" ", "_"): c for c in recon_df.columns}
                    sno_col = col_map.get("sno") or next((c for c in recon_df.columns if "sno" in str(c).lower()), None)
                    src_db_col = col_map.get("source_database") or next((c for c in recon_df.columns if "source" in str(c).lower() and "database" in str(c).lower()), None)
                    src_tbl_col = col_map.get("source_table_nm") or next((c for c in recon_df.columns if "source" in str(c).lower() and "table" in str(c).lower()), None)
                    tgt_db_col = col_map.get("target_database") or next((c for c in recon_df.columns if "target" in str(c).lower() and "database" in str(c).lower()), None)
                    tgt_tbl_col = col_map.get("target_table_nm") or next((c for c in recon_df.columns if "target" in str(c).lower() and "table" in str(c).lower()), None)
                    if not all([sno_col, src_db_col, src_tbl_col, tgt_db_col, tgt_tbl_col]):
                        st.warning("Expected columns: SNO, Source_database, source_table_nm, target_database, target_table_nm")
                    else:
                        st.session_state["recon_excel_df"] = recon_df
                        st.session_state["recon_excel_cols"] = {"sno": sno_col, "src_db": src_db_col, "src_tbl": src_tbl_col, "tgt_db": tgt_db_col, "tgt_tbl": tgt_tbl_col}
                except Exception as e:
                    st.error(f"Invalid file: {e}")
            else:
                st.session_state.pop("recon_excel_df", None)
        else:
            st.session_state.pop("recon_excel_df", None)
            st.selectbox("Source Database", db_options, key="recon_source_db")
            st.text_input("Source Table Name", placeholder="Enter source table...", key="recon_source_table")
            st.selectbox("Target Database", db_options, key="recon_target_db")
            st.text_input("Target Table Name", placeholder="Enter target table...", key="recon_target_table")
        execute_clicked = st.button("Execute", key="recon_execute", type="primary", use_container_width=True)
        if execute_clicked:
            _run_recon()
        if "recon_error" in st.session_state:
            st.error(st.session_state["recon_error"])
        components.html(
            '''<button onclick="try{window.top.print()}catch(e){window.print()}" style="padding:0.4rem 0.8rem;background:#87cefa;color:#000000;
            border:1px solid #6fbfe6;border-radius:6px;cursor:pointer;font-weight:400;font-size:0.85rem;width:100%;">Download PDF</button>''',
            height=40
        )

    elif st.session_state.get("page") == "DMC":
        with st.expander("Config", expanded=True):
            dmc_config_mode = st.radio("Config", ["Default", "Upload"], label_visibility="collapsed", key="dmc_config_mode", horizontal=True)
            DMC_CONFIG_PATH = Path(__file__).parent / "dmc_config.json"
            if dmc_config_mode == "Default":
                st.caption("Reads dmc_config.json from project folder")
                try:
                    if not DMC_CONFIG_PATH.exists():
                        st.session_state["dmc_config_error"] = "dmc_config.json not found in project folder"
                    else:
                        cfg = json.loads(DMC_CONFIG_PATH.read_text(encoding="utf-8"))
                        st.session_state["dmc_hop_config"] = cfg.get("hop_databases", {})
                        st.session_state.pop("dmc_config_error", None)
                        st.caption("Config loaded")
                except json.JSONDecodeError as e:
                    st.session_state["dmc_config_error"] = str(e)
                except Exception as e:
                    st.session_state["dmc_config_error"] = str(e)
                if "dmc_config_error" in st.session_state:
                    st.error(st.session_state["dmc_config_error"])
            else:
                dmc_config_file = st.file_uploader("Upload config (JSON)", type=["json"], key="dmc_config_upload")
                if dmc_config_file:
                    try:
                        dmc_config = json.load(dmc_config_file)
                        if "hop_databases" in dmc_config:
                            st.session_state["dmc_hop_config"] = dmc_config["hop_databases"]
                            st.session_state.pop("dmc_config_error", None)
                            st.caption("Config loaded")
                        else:
                            st.warning("Expected 'hop_databases' in JSON")
                    except json.JSONDecodeError as e:
                        st.error(f"Invalid JSON: {e}")
                    except Exception as e:
                        st.error(str(e))
        with st.expander("Data Copy", expanded=True):
            dmc_file = st.file_uploader("Upload Excel file", type=["xlsx", "xls"], key="dmc_upload")
            if dmc_file:
                try:
                    dmc_df = pd.read_excel(dmc_file, sheet_name=0)
                    st.session_state["dmc_excel_df"] = dmc_df
                except Exception as e:
                    st.error(f"Invalid file: {e}")
            else:
                st.session_state.pop("dmc_excel_df", None)
        dmc_execute_clicked = st.button("Execute", key="dmc_execute", type="primary", use_container_width=True)
        if dmc_execute_clicked:
            _run_dmc()
        if "dmc_error" in st.session_state:
            st.error(st.session_state["dmc_error"])
        components.html(
            '''<button onclick="try{window.top.print()}catch(e){window.print()}" style="padding:0.4rem 0.8rem;background:#87cefa;color:#000000;
            border:1px solid #6fbfe6;border-radius:6px;cursor:pointer;font-weight:400;font-size:0.85rem;width:100%;">Download PDF</button>''',
            height=40
        )


    elif st.session_state.get("page") == "Read me":
        st.markdown("**📥 Click to Download Test Case Template**")
        template_path = Path(__file__).parent / "orchestrator_template.xlsx"
        if template_path.exists():
            with open(template_path, "rb") as f:
                st.download_button("Orchestrator", data=f.read(), file_name="orchestrator_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="readme_dl_orchestrator_template")
        recon_template_path = Path(__file__).parent / "recon_template.xlsx"
        if recon_template_path.exists():
            with open(recon_template_path, "rb") as f:
                st.download_button("Recon", data=f.read(), file_name="recon_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="readme_dl_recon_template")
        dmc_template_path = Path(__file__).parent / "dmc_template.xlsx"
        if dmc_template_path.exists():
            with open(dmc_template_path, "rb") as f:
                st.download_button("DMC", data=f.read(), file_name="dmc_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="readme_dl_dmc_template")
        st.markdown("**📥 Click to download Database Config Template**")
        config_sample_path = Path(__file__).parent / "config_sample.json"
        if config_sample_path.exists():
            with open(config_sample_path, "rb") as f:
                st.download_button("Orchestrator", data=f.read(), file_name="config_sample.json", mime="application/json", key="readme_dl_config")
        dmc_config_path = Path(__file__).parent / "dmc_config_sample.json"
        if dmc_config_path.exists():
            with open(dmc_config_path, "rb") as f:
                st.download_button("DMC", data=f.read(), file_name="dmc_config_sample.json", mime="application/json", key="readme_dl_dmc_config")

    st.markdown("---")
    st.caption("DataVeritas")

# Navigation buttons
page = st.session_state.get("page", "Orchestrator")
tabs = ["Orchestrator", "Recon", "DMC", "Data Explorer", "Read me"]
nav_cols = st.columns(len(tabs))
for i, tab in enumerate(tabs):
    with nav_cols[i]:
        is_active = page == tab
        if st.button(tab, key=f"nav_{tab}", use_container_width=True, type="primary" if is_active else "secondary"):
            st.session_state["page"] = tab
            st.rerun()
st.markdown("---")

# Initialize session state for the list
if "items" not in st.session_state:
    st.session_state.items = [
        {"id": 1, "name": "Sample Task", "status": "Active", "created": datetime.now().strftime("%Y-%m-%d")},
        {"id": 2, "name": "Another Item", "status": "Completed", "created": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")},
    ]

if page == "Recon":
    render_recon()

elif page == "Data Explorer":
    render_data_explorer()

elif page == "Orchestrator":
    if "orc_excel_error" in st.session_state:
        st.error(st.session_state["orc_excel_error"])

    col_data = st.container()
    with col_data:
        if "orc_excel_data" in st.session_state:
            df_all = st.session_state["orc_excel_data"]
            if isinstance(df_all, dict):
                sel = st.session_state.get("orc_selected_sheet", list(df_all.keys())[0])
                sheets_to_show = list(df_all.items()) if sel == "ALL" else [(sel, df_all[sel])]
            else:
                sheets_to_show = [("Sheet", df_all)]

            excel_name = st.session_state.get("orc_excel_filename", "Sheet")

            for sheet_name, df in sheets_to_show:
                st.markdown(f'<p style="color: #0066cc; font-size: 1.5rem; font-weight: 600; margin: 0.5rem 0;">{excel_name} - {sheet_name}</p>', unsafe_allow_html=True)
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.markdown("---")

                col_res = None
                col_skip_reg = None
                cnt_success = 0
                cnt_fail = 0
                if st.session_state.get("orc_execute_clicked"):
                    st.markdown('<p style="color: #0066cc; font-size: 1.5rem; font-weight: 600; margin: 0.5rem 0;">Execution of the Test Cases is Started......</p>', unsafe_allow_html=True)
                if not df.empty and st.session_state.get("orc_execute_clicked"):
                    def find_col(df, names):
                        cols_lower = {str(c).strip().lower(): c for c in df.columns}
                        for n in names:
                            nlo = n.lower().replace(" ", "_")
                            if nlo in cols_lower:
                                return cols_lower[nlo]
                            for c in df.columns:
                                if nlo in str(c).lower().replace(" ", "_"):
                                    return c
                        return None

                    col_sno = find_col(df, ["S_No", "SNo", "Test Case", "TestCase"])
                    col_val = find_col(df, ["Validation_Type", "Validation Type", "ValidationType"])
                    col_cols = find_col(df, ["Columns", "Column"])
                    col_sql = find_col(df, ["SQL Query", "SQLQuery", "SQL", "Query"])
                    col_res = find_col(df, ["Results", "Result"])
                    col_skip_reg = find_col(df, ["Skip_Regression_Testing", "Skip Regression Testing", "SkipRegressionTesting"])
                    if col_skip_reg is None:
                        for c in df.columns:
                            if "skip" in str(c).lower() and "regression" in str(c).lower():
                                col_skip_reg = c
                                break

                    conn = st.session_state.get("orc_db_conn")
                    db_type = st.session_state.get("orchestrator_database", "Netezza")
                    for idx, row in df.iterrows():
                        validation_executed = False
                        v = lambda r, c: r.get(c, "-") if c is not None else "-"
                        st.markdown(f"**Test Case:** {v(row, col_sno)}")
                        st.markdown(f"**Validation Type:** {v(row, col_val)}")
                        st.markdown(f"**Columns:** {v(row, col_cols)}")
                        sql_val = v(row, col_sql)
                        validation_type_raw = str(v(row, col_val))
                        validation_type = " ".join(validation_type_raw.strip().lower().split())
                        computed_status = None
                        not_matching_msg = "Data is not Matching between the hop."
                        st.markdown("**SQL Query:**")
                        if sql_val and str(sql_val).strip() not in ("-", "nan", ""):
                            sql_str = str(sql_val).strip()
                            st.code(sql_str, language="sql")
                            if conn:
                                try:
                                    from db_connector import run_query
                                    qdf = run_query(conn, db_type, sql_str)
                                    validation_executed = True
                                    if qdf is not None and not qdf.empty:
                                        display_df = qdf.head(5)
                                        st.markdown("**Query Results:**")
                                        st.dataframe(display_df, use_container_width=True, hide_index=True)
                                        if len(qdf) > 5:
                                            st.caption(f"Showing first 5 of {len(qdf)} rows")
                                        if validation_type in ("count", "row count"):
                                            if len(qdf) == 1:
                                                computed_status = not_matching_msg
                                            elif len(qdf) >= 2:
                                                if qdf.shape[1] >= 2:
                                                    first_val = qdf.iloc[0, 1]
                                                    second_val = qdf.iloc[1, 1]
                                                    if pd.isna(first_val) or pd.isna(second_val):
                                                        computed_status = not_matching_msg
                                                    elif str(first_val).strip() == str(second_val).strip():
                                                        computed_status = "Success"
                                                    else:
                                                        computed_status = not_matching_msg
                                                else:
                                                    st.warning("Count validation expects at least 2 columns.")
                                    elif validation_type in ("dnp", "etl fields", "direct map"):
                                        if len(qdf) > 1:
                                            computed_status = "Data not matching"
                                    elif validation_type == "etl":
                                        if len(qdf) < 6:
                                            computed_status = "Data matching as expected"
                                    elif validation_type in ("business logic", "business_logic", "businesslogic"):
                                        if qdf is None or qdf.empty:
                                            computed_status = "Success"
                                        else:
                                            computed_status = "Data not matching"
                                    elif validation_type in ("default values", "default"):
                                        if len(qdf) == 1:
                                            computed_status = "Data is loaded as expected"
                                    elif qdf is not None:
                                        if validation_type in ("dnp", "etl fields", "direct map"):
                                            computed_status = "Data is Matching as expected"
                                            st.caption("Data is Matching as expected")
                                        elif validation_type == "etl":
                                            computed_status = "Data matching as expected"
                                            st.caption("Data matching as expected")
                                        elif validation_type in ("business logic", "business_logic", "businesslogic"):
                                            computed_status = "Success"
                                            st.caption("Success")
                                        else:
                                            st.caption("Query returned no rows.")
                                except Exception as ex:
                                    st.error(f"Query error: {ex}")
                            else:
                                st.caption("Connect to database to execute query.")
                        else:
                            st.markdown("-")
                        if not conn and not validation_executed:
                            res_val = "Connect to database to run validation"
                        else:
                            res_val = computed_status or str(v(row, col_res)).strip()
                        res_lower = res_val.lower()
                        if res_lower == "success":
                            cnt_success += 1
                        elif res_lower == "fail" or res_lower == "data not matching" or res_lower == "data is not matching between the hop.":
                            cnt_fail += 1
                        if res_lower == "success":
                            res_style = "color: #008000; font-weight: bold;"
                        elif res_lower == "fail":
                            res_style = "color: #cc0000; font-weight: bold;"
                        elif res_lower == "data is loaded as expected":
                            res_style = "color: #1e90ff; font-weight: bold;"
                        else:
                            res_style = "color: #000000;"
                        st.markdown(f"**Results:** <span style='{res_style}'>{res_val}</span>", unsafe_allow_html=True)
                        st.markdown("---")

                        if (
                            validation_type in ("count", "row count")
                            and computed_status == not_matching_msg
                            and not st.session_state.get("orc_proceed_on_row_count_fail", True)
                        ):
                            st.warning("Row count validation failed; stopping further execution.")
                            break

                if st.session_state.get("orc_execute_clicked") and col_res is not None:
                    if col_skip_reg is not None:
                        skip_series = df[col_skip_reg].fillna("").astype(str).str.strip().str.upper()
                        cnt_skipped = (skip_series == "Y").sum()
                    else:
                        cnt_skipped = 0
                    st.markdown("---")
                    cnt_total = cnt_success + cnt_fail + cnt_skipped
                    c1, c2, c3, c4 = st.columns(4)
                    with c1:
                        _render_stat_card("# Total Test Cases", cnt_total, "🧾")
                    with c2:
                        _render_stat_card("# Test Case - Successful", cnt_success, "✅")
                    with c3:
                        _render_stat_card("# Test Case - Failed", cnt_fail, "❌")
                    with c4:
                        _render_stat_card("# Test Case - Skipped", cnt_skipped, "⏭️")
                    st.markdown(
                        "<hr style='border: 0; border-top: 4px double #ffffff; margin: 1rem 0;' />",
                        unsafe_allow_html=True,
                    )
        else:
            st.info("Configure source in the left panel and load data.")

    # Removed the right-side "Run query" panel.

elif page == "DMC":
    render_dmc()

elif page == "Read me":
    st.title("Read me")
    readme_path = Path(__file__).parent / "README.md"
    if readme_path.exists():
        st.markdown(readme_path.read_text(encoding="utf-8"))
    else:
        st.info("README.md not found.")
