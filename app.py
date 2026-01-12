import streamlit as st
import requests
import json
import snowflake.connector
from typing import Any, Dict, List, Tuple, Optional
import re
import time
from datetime import datetime, timedelta
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import uuid

# =============================================================================
# CONFIGURATION
# =============================================================================

APP_NAME = "Dataflow Backfill Manager"
BACKFILL_TABLE = "MAKO_DATA_LAKE.PUBLIC.BACKFILL_RUNS"

# =============================================================================
# STYLING
# =============================================================================

def apply_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    :root {
        --primary: #4a5568;
        --primary-dark: #2d3748;
        --accent: #5a67d8;
        --accent-light: #7f9cf5;
        --success: #48bb78;
        --success-bg: #f0fff4;
        --success-border: #9ae6b4;
        --warning: #ed8936;
        --warning-bg: #fffaf0;
        --warning-border: #fbd38d;
        --error: #e53e3e;
        --error-bg: #fff5f5;
        --error-border: #feb2b2;
        --info: #4299e1;
        --info-bg: #ebf8ff;
        --info-border: #90cdf4;
        --bg-primary: #f7fafc;
        --bg-secondary: #ffffff;
        --bg-tertiary: #edf2f7;
        --text-primary: #1a202c;
        --text-secondary: #718096;
        --text-muted: #a0aec0;
        --border-color: #e2e8f0;
        --border-dark: #cbd5e0;
    }
    
    .stApp {
        background: var(--bg-primary);
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Header */
    .app-header {
        background: linear-gradient(135deg, var(--primary-dark) 0%, var(--primary) 100%);
        padding: 2rem 2.5rem;
        border-radius: 8px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    
    .app-title {
        color: #ffffff !important;
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0;
        letter-spacing: -0.025em;
    }
    
    .app-subtitle {
        color: rgba(255,255,255,0.85) !important;
        font-size: 0.875rem;
        font-weight: 400;
        margin-top: 0.5rem;
    }
    
    /* Metric boxes */
    .metric-box {
        background: var(--bg-secondary);
        border-radius: 6px;
        padding: 1rem 1.25rem;
        border: 1px solid var(--border-color);
        text-align: center;
    }
    
    .metric-value {
        font-size: 1.125rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 0.25rem;
    }
    
    .metric-label {
        font-size: 0.75rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    /* Status indicators */
    .status-indicator {
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.75rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.025em;
    }
    
    .status-ready {
        background: var(--success-bg);
        color: #276749;
        border: 1px solid var(--success-border);
    }
    
    .status-warning {
        background: var(--warning-bg);
        color: #c05621;
        border: 1px solid var(--warning-border);
    }
    
    .status-error {
        background: var(--error-bg);
        color: #c53030;
        border: 1px solid var(--error-border);
    }
    
    .status-info {
        background: var(--info-bg);
        color: #2b6cb0;
        border: 1px solid var(--info-border);
    }
    
    .status-neutral {
        background: var(--bg-tertiary);
        color: var(--text-secondary);
        border: 1px solid var(--border-color);
    }
    
    .status-running {
        background: var(--info-bg);
        color: #2b6cb0;
        border: 1px solid var(--info-border);
    }
    
    .status-success {
        background: var(--success-bg);
        color: #276749;
        border: 1px solid var(--success-border);
    }
    
    .status-failed {
        background: var(--error-bg);
        color: #c53030;
        border: 1px solid var(--error-border);
    }
    
    .status-paused {
        background: var(--warning-bg);
        color: #c05621;
        border: 1px solid var(--warning-border);
    }
    
    /* Alert boxes */
    .alert {
        border-radius: 6px;
        padding: 1rem;
        margin: 0.75rem 0;
        font-size: 0.875rem;
    }
    
    .alert-info {
        background: var(--info-bg);
        border: 1px solid var(--info-border);
        color: #2c5282;
    }
    
    .alert-warning {
        background: var(--warning-bg);
        border: 1px solid var(--warning-border);
        color: #744210;
    }
    
    .alert-success {
        background: var(--success-bg);
        border: 1px solid var(--success-border);
        color: #22543d;
    }
    
    .alert-error {
        background: var(--error-bg);
        border: 1px solid var(--error-border);
        color: #742a2a;
    }
    
    .alert-title {
        font-weight: 600;
        margin-bottom: 0.25rem;
    }
    
    /* Progress section */
    .progress-container {
        background: var(--bg-secondary);
        border-radius: 8px;
        padding: 1.5rem;
        border: 1px solid var(--border-color);
        margin-top: 1rem;
    }
    
    .progress-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    .progress-title {
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--text-primary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .progress-percentage {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--accent);
    }
    
    /* View list items */
    .view-item {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.5rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    
    .view-name {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        color: var(--text-primary);
    }
    
    /* Log entries */
    .log-container {
        background: var(--primary-dark);
        border-radius: 6px;
        padding: 1rem;
        max-height: 300px;
        overflow-y: auto;
    }
    
    .log-entry {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        padding: 0.375rem 0.5rem;
        border-radius: 3px;
        margin: 0.25rem 0;
        display: flex;
        align-items: flex-start;
        gap: 0.5rem;
    }
    
    .log-timestamp {
        color: var(--text-muted);
        flex-shrink: 0;
    }
    
    .log-message {
        color: #e2e8f0;
    }
    
    .log-entry.success .log-message {
        color: #68d391;
    }
    
    .log-entry.error .log-message {
        color: #fc8181;
    }
    
    .log-entry.warning .log-message {
        color: #f6ad55;
    }
    
    /* Section titles */
    .section-title {
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--text-primary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid var(--border-color);
    }
    
    /* Buttons */
    .stButton > button {
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        border-radius: 6px;
        padding: 0.5rem 1.5rem;
        transition: all 0.15s ease;
        text-transform: none;
        letter-spacing: 0;
    }
    
    .stButton > button[kind="primary"] {
        background: var(--accent);
        border: none;
        color: white;
    }
    
    .stButton > button[kind="primary"]:hover {
        background: var(--primary-dark);
    }
    
    /* Resume card */
    .resume-card {
        background: var(--warning-bg);
        border: 2px solid var(--warning-border);
        border-radius: 8px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }
    
    .resume-card-title {
        font-size: 1rem;
        font-weight: 600;
        color: #744210;
        margin-bottom: 0.75rem;
    }
    
    .resume-card-details {
        font-size: 0.875rem;
        color: #975a16;
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background: var(--bg-secondary);
        border-right: 1px solid var(--border-color);
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Divider */
    .divider {
        height: 1px;
        background: var(--border-color);
        margin: 1.5rem 0;
    }

        /* Input styling */
    .stTextInput > div > div > input {
        font-family: 'Inter', sans-serif;
        border-radius: 6px;
        border: 1px solid #cbd5e0 !important;
        background-color: #ffffff !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #5a67d8 !important;
        box-shadow: 0 0 0 2px rgba(90, 103, 216, 0.2) !important;
    }
    
    .stSelectbox > div > div {
        border-radius: 6px;
        border: 1px solid #cbd5e0 !important;
        background-color: #ffffff !important;
    }
    
    .stDateInput > div > div > input {
        font-family: 'Inter', sans-serif;
        border-radius: 6px;
        border: 1px solid #cbd5e0 !important;
        background-color: #ffffff !important;
    }
    
    .stNumberInput > div > div > input {
        font-family: 'Inter', sans-serif;
        border-radius: 6px;
        border: 1px solid #cbd5e0 !important;
        background-color: #ffffff !important;
    }
    
    /* Sidebar section labels */
    section[data-testid="stSidebar"] .stMarkdown p strong {
        color: #2d3748;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    /* Sidebar dividers */
    section[data-testid="stSidebar"] .divider {
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# DOMO FUNCTIONS
# =============================================================================

def get_domo_headers():
    return {
        'Content-Type': 'application/json',
        'X-DOMO-Developer-Token': st.secrets["domo"]["developer_token"]
    }

def get_domo_base_url():
    return f"https://{st.secrets['domo']['instance']}.domo.com"

@st.cache_data(ttl=300)
def list_dataflows() -> List[Dict]:
    url = f"{get_domo_base_url()}/api/dataprocessing/v1/dataflows"
    response = requests.get(url, headers=get_domo_headers(), timeout=60)
    response.raise_for_status()
    return response.json()

def get_dataflow(df_id: int) -> Dict:
    url = f"{get_domo_base_url()}/api/dataprocessing/v1/dataflows/{df_id}"
    response = requests.get(url, headers=get_domo_headers(), timeout=60)
    response.raise_for_status()
    return response.json()

def trigger_dataflow(df_id: int) -> Tuple[int, Dict]:
    url = f"{get_domo_base_url()}/api/dataprocessing/v1/dataflows/{df_id}/executions"
    response = requests.post(url, headers=get_domo_headers(), timeout=60)
    return response.status_code, response.json() if response.text else {}

def get_execution_status(df_id: int, execution_id: int) -> Tuple[str, int, str]:
    dataflow = get_dataflow(df_id)
    last_exec = dataflow.get('lastExecution', {})
    last_exec_id = last_exec.get('id')
    last_exec_status = last_exec.get('state', 'UNKNOWN')
    
    if last_exec_id == execution_id:
        return last_exec_status, last_exec_id, last_exec_status
    else:
        return 'PENDING', last_exec_id, last_exec_status

def extract_input_names(dataflow: Dict) -> List[str]:
    inputs = []
    for action in dataflow.get('actions', []) or []:
        if action.get('type') == 'LoadFromVault':
            name = action.get('name')
            if name:
                inputs.append(name)
    
    seen = set()
    out = []
    for x in inputs:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def extract_update_method(dataflow: Dict) -> Dict:
    best = None
    best_score = -1
    
    for action in dataflow.get('actions', []) or []:
        has_any = any(k in action for k in ('partitioned', 'partitionIdColumns', 'versionChainType'))
        if not has_any:
            continue
        
        partitioned = action.get('partitioned')
        cols = action.get('partitionIdColumns') or []
        vct = action.get('versionChainType')
        
        score = 0
        if partitioned is True:
            score += 100
        if isinstance(cols, list) and len(cols) > 0:
            score += 50
        if vct is not None:
            score += 10
        
        if score > best_score:
            best_score = score
            best = action
    
    if best is None:
        return {
            'partitioned': False,
            'partition_columns': [],
            'update_method': None
        }
    
    return {
        'partitioned': bool(best.get('partitioned')),
        'partition_columns': best.get('partitionIdColumns') or [],
        'update_method': best.get('versionChainType')
    }


# =============================================================================
# SNOWFLAKE FUNCTIONS
# =============================================================================

@st.cache_resource
def get_snowflake_connection():
    private_key_pem = st.secrets["snowflake"]["private_key"]
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=None,
        backend=default_backend()
    )
    
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        private_key=private_key_bytes,
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"]
    )

def is_fully_qualified(name: str) -> bool:
    pattern = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
    return bool(pattern.match(name))

def get_object_type(cur, fqn: str) -> Tuple[str, Optional[str]]:
    db, schema, obj = fqn.split(".")
    cur.execute(f"""
        SELECT TABLE_TYPE, TABLE_NAME
        FROM {db}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema.upper()}' 
        AND UPPER(TABLE_NAME) = '{obj.upper()}'
    """)
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return "NOT_FOUND", None

def get_view_ddl(cur, fqn: str, actual_name: str = None) -> str:
    db, schema, obj = fqn.split(".")
    if actual_name:
        obj = actual_name
    cur.execute(f'SELECT GET_DDL(\'VIEW\', \'{db}.{schema}."{obj}"\')')
    return cur.fetchone()[0]

def check_date_filter_markers(ddl: str) -> Dict[str, bool]:
    return {
        'has_normal_marker': '--{normal_date_filter}' in ddl,
        'has_temp_marker': '--{temp_date_filter}' in ddl,
        'ready_for_backfill': '--{normal_date_filter}' in ddl and '--{temp_date_filter}' in ddl
    }

def apply_date_filter(ddl: str, from_date: str, to_date: str) -> str:
    lines = ddl.split('\n')
    new_lines = []
    
    for line in lines:
        if '--{normal_date_filter}' in line:
            new_lines.append(line.replace('WHERE', '--WHERE').replace('where', '--where'))
        elif '--{temp_date_filter}' in line:
            new_lines.append(
                line.replace('--WHERE', 'WHERE')
                    .replace('--where', 'where')
                    .replace('{from_date}', from_date)
                    .replace('{to_date}', to_date)
            )
        else:
            new_lines.append(line)
    
    return '\n'.join(new_lines)


# =============================================================================
# STATE PERSISTENCE FUNCTIONS
# =============================================================================

def create_backfill_run(cur, dataflow_id: int, dataflow_name: str, start_date: datetime, 
                        end_date: datetime, batch_days: int, total_batches: int,
                        original_ddls: Dict, ready_views: List) -> str:
    """Create a new backfill run record"""
    run_id = str(uuid.uuid4())[:8]
    
    cur.execute(f"""
        INSERT INTO {BACKFILL_TABLE} 
        (run_id, dataflow_id, dataflow_name, start_date, end_date, batch_days, 
         current_batch, total_batches, status, original_ddls, ready_views, logs)
        SELECT 
            %(run_id)s,
            %(dataflow_id)s,
            %(dataflow_name)s,
            %(start_date)s,
            %(end_date)s,
            %(batch_days)s,
            0,
            %(total_batches)s,
            'RUNNING',
            PARSE_JSON(%(original_ddls)s),
            PARSE_JSON(%(ready_views)s),
            PARSE_JSON('[]')
    """, {
        'run_id': run_id,
        'dataflow_id': dataflow_id,
        'dataflow_name': dataflow_name,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'batch_days': batch_days,
        'total_batches': total_batches,
        'original_ddls': json.dumps(original_ddls),
        'ready_views': json.dumps(ready_views)
    })
    
    return run_id

def update_backfill_progress(cur, run_id: str, current_batch: int, status: str, logs: List[Dict]):
    """Update backfill run progress"""
    cur.execute(f"""
        UPDATE {BACKFILL_TABLE}
        SET current_batch = %(current_batch)s,
            status = %(status)s,
            logs = PARSE_JSON(%(logs)s),
            updated_at = CURRENT_TIMESTAMP()
        WHERE run_id = %(run_id)s
    """, {
        'run_id': run_id,
        'current_batch': current_batch,
        'status': status,
        'logs': json.dumps(logs)
    })

def get_incomplete_runs(cur) -> List[Dict]:
    """Get all incomplete backfill runs"""
    cur.execute(f"""
        SELECT run_id, dataflow_id, dataflow_name, start_date, end_date, batch_days,
               current_batch, total_batches, status, original_ddls, ready_views, logs,
               created_at, updated_at
        FROM {BACKFILL_TABLE}
        WHERE status IN ('RUNNING', 'PAUSED')
        ORDER BY created_at DESC
    """)
    
    rows = cur.fetchall()
    columns = ['run_id', 'dataflow_id', 'dataflow_name', 'start_date', 'end_date', 
               'batch_days', 'current_batch', 'total_batches', 'status', 
               'original_ddls', 'ready_views', 'logs', 'created_at', 'updated_at']
    
    return [dict(zip(columns, row)) for row in rows]

def get_run_by_id(cur, run_id: str) -> Optional[Dict]:
    """Get a specific backfill run"""
    cur.execute(f"""
        SELECT run_id, dataflow_id, dataflow_name, start_date, end_date, batch_days,
               current_batch, total_batches, status, original_ddls, ready_views, logs,
               created_at, updated_at
        FROM {BACKFILL_TABLE}
        WHERE run_id = %(run_id)s
    """, {'run_id': run_id})
    
    row = cur.fetchone()
    if not row:
        return None
    
    columns = ['run_id', 'dataflow_id', 'dataflow_name', 'start_date', 'end_date', 
               'batch_days', 'current_batch', 'total_batches', 'status', 
               'original_ddls', 'ready_views', 'logs', 'created_at', 'updated_at']
    
    return dict(zip(columns, row))

def revert_views_from_run(cur, run: Dict):
    """Revert views to original state using stored DDLs"""
    original_ddls = run.get('original_ddls', {})
    if isinstance(original_ddls, str):
        original_ddls = json.loads(original_ddls)
    
    for fqn, ddl in original_ddls.items():
        try:
            cur.execute(ddl)
        except Exception as e:
            st.warning(f"Failed to revert {fqn}: {e}")

def cancel_run(cur, run_id: str):
    """Cancel a run and revert views"""
    run = get_run_by_id(cur, run_id)
    if run:
        revert_views_from_run(cur, run)
        cur.execute(f"""
            UPDATE {BACKFILL_TABLE}
            SET status = 'CANCELLED', updated_at = CURRENT_TIMESTAMP()
            WHERE run_id = %(run_id)s
        """, {'run_id': run_id})


# =============================================================================
# UI COMPONENTS
# =============================================================================

def render_header():
    st.markdown("""
    <div class="app-header">
        <h1 class="app-title">Dataflow Backfill Manager</h1>
        <p class="app-subtitle">Execute historical data backfills through automated batch processing</p>
    </div>
    """, unsafe_allow_html=True)

def render_dataflow_info(df_info: Dict, update_info: Dict):
    partitioned = update_info.get('partitioned', False)
    partition_cols = update_info.get('partition_columns', [])
    update_method = update_info.get('update_method', 'Unknown')
    
    st.markdown('<div class="section-title">Dataflow Configuration</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{df_info.get('name', 'Unknown')[:30]}</div>
            <div class="metric-label">Dataflow Name</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        status_class = "status-ready" if partitioned else "status-warning"
        status_text = "Enabled" if partitioned else "Disabled"
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value"><span class="status-indicator {status_class}">{status_text}</span></div>
            <div class="metric-label">Partition Update</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{update_method or 'N/A'}</div>
            <div class="metric-label">Update Method</div>
        </div>
        """, unsafe_allow_html=True)
    
    if partition_cols:
        st.markdown(f"""
        <div class="alert alert-info">
            <span class="alert-title">Partition Configuration</span><br/>
            Partition columns: <strong>{', '.join(partition_cols)}</strong>
        </div>
        """, unsafe_allow_html=True)
    
    if not partitioned:
        st.markdown("""
        <div class="alert alert-warning">
            <span class="alert-title">Warning</span><br/>
            This dataflow does not use partition updates. Each batch execution may overwrite previous data.
        </div>
        """, unsafe_allow_html=True)

def render_view_status(ready_views: List, missing_views: List, skipped: List):
    st.markdown('<div class="section-title">Input Sources Analysis</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{len(ready_views)}</div>
            <div class="metric-label">Ready</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{len(missing_views)}</div>
            <div class="metric-label">Missing Markers</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{len(skipped)}</div>
            <div class="metric-label">Skipped</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br/>", unsafe_allow_html=True)
    
    if ready_views:
        with st.expander(f"Ready for backfill ({len(ready_views)})", expanded=True):
            for fqn, _ in ready_views:
                st.markdown(f"""
                <div class="view-item">
                    <span class="view-name">{fqn}</span>
                    <span class="status-indicator status-ready">Ready</span>
                </div>
                """, unsafe_allow_html=True)
    
    if missing_views:
        with st.expander(f"Missing date filter markers ({len(missing_views)})"):
            for fqn, markers in missing_views:
                st.markdown(f"""
                <div class="view-item">
                    <span class="view-name">{fqn}</span>
                    <span class="status-indicator status-warning">Missing Markers</span>
                </div>
                """, unsafe_allow_html=True)
    
    if skipped:
        with st.expander(f"Skipped sources ({len(skipped)})"):
            for fqn, obj_type in skipped:
                st.markdown(f"""
                <div class="view-item">
                    <span class="view-name">{fqn}</span>
                    <span class="status-indicator status-neutral">{obj_type}</span>
                </div>
                """, unsafe_allow_html=True)

def render_progress(current_batch: int, total_batches: int, current_dates: Tuple[str, str], 
                    status: str, logs: List[Dict], run_id: str = None):
    percentage = (current_batch / total_batches * 100) if total_batches > 0 else 0
    
    run_info = f" (Run ID: {run_id})" if run_id else ""
    
    st.markdown(f"""
    <div class="progress-container">
        <div class="progress-header">
            <span class="progress-title">Execution Progress{run_info}</span>
            <span class="progress-percentage">{percentage:.0f}%</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.progress(percentage / 100)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Batch", f"{current_batch} / {total_batches}")
    with col2:
        date_display = f"{current_dates[0]} to {current_dates[1]}" if current_dates[0] else "—"
        st.metric("Current Range", date_display)
    with col3:
        status_class = {
            'RUNNING': 'status-running',
            'SUCCESS': 'status-success',
            'FAILED': 'status-failed',
            'PENDING': 'status-info',
            'PREPARING': 'status-info',
            'PAUSED': 'status-paused',
            'COMPLETED': 'status-success'
        }.get(status, 'status-info')
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value"><span class="status-indicator {status_class}">{status}</span></div>
            <div class="metric-label">Status</div>
        </div>
        """, unsafe_allow_html=True)
    
if logs:
    st.markdown("<br/>", unsafe_allow_html=True)
    
    with st.expander("Execution Log", expanded=True):
        log_html = '<div class="log-container">'
        for log in logs[-25:]:
            log_class = log.get('type', 'info')
            timestamp = log.get('timestamp', '')
            message = log.get('message', '')
            log_html += f'''<div class="log-entry {log_class}"><span class="log-timestamp">{timestamp}</span><span class="log-message">{message}</span></div>'''
        log_html += '</div>'
        st.markdown(log_html, unsafe_allow_html=True)

def render_incomplete_run(run: Dict):
    """Render an incomplete run card"""
    progress = (run['current_batch'] / run['total_batches'] * 100) if run['total_batches'] > 0 else 0
    
    st.markdown(f"""
    <div class="resume-card">
        <div class="resume-card-title">Incomplete Backfill Detected</div>
        <div class="resume-card-details">
            <strong>Dataflow:</strong> {run['dataflow_name']}<br/>
            <strong>Run ID:</strong> {run['run_id']}<br/>
            <strong>Progress:</strong> {run['current_batch']} / {run['total_batches']} batches ({progress:.0f}%)<br/>
            <strong>Date Range:</strong> {run['start_date']} to {run['end_date']}<br/>
            <strong>Status:</strong> {run['status']}<br/>
            <strong>Last Updated:</strong> {run['updated_at']}
        </div>
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# BACKFILL LOGIC
# =============================================================================

def calculate_batches(start_date: datetime, end_date: datetime, batch_days: int) -> List[Tuple[datetime, datetime]]:
    batches = []
    current = start_date
    while current <= end_date:
        batch_end = min(current + timedelta(days=batch_days - 1), end_date)
        batches.append((current, batch_end))
        current = batch_end + timedelta(days=1)
    return batches

def add_log(logs: List[Dict], message: str, log_type: str = 'info'):
    logs.append({
        'timestamp': datetime.now().strftime('%H:%M:%S'),
        'message': message,
        'type': log_type
    })

def run_backfill(run_id: str, df_id: int, ready_views: List, start_date: datetime, 
                 end_date: datetime, batch_days: int, poll_interval: int, 
                 start_batch: int, original_ddls: Dict, progress_placeholder):
    """Run or resume a backfill process"""
    
    batches = calculate_batches(start_date, end_date, batch_days)
    total_batches = len(batches)
    logs = []
    
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    try:
        for batch_idx in range(start_batch, total_batches):
            batch_start, batch_end = batches[batch_idx]
            start_str = batch_start.strftime('%Y-%m-%d')
            end_str = batch_end.strftime('%Y-%m-%d')
            
            add_log(logs, f"Starting batch {batch_idx + 1}/{total_batches}: {start_str} to {end_str}", 'info')
            
            # Update state to RUNNING
            update_backfill_progress(cur, run_id, batch_idx, 'RUNNING', logs)
            conn.commit()
            
            with progress_placeholder.container():
                render_progress(batch_idx, total_batches, (start_str, end_str), "PREPARING", logs, run_id)
            
            # Apply date filters
            add_log(logs, f"Applying date filters to {len(ready_views)} view(s)", 'info')
            for fqn, actual_name in ready_views:
                modified_ddl = apply_date_filter(original_ddls[fqn], start_str, end_str)
                cur.execute(modified_ddl)
            
            # Trigger dataflow
            add_log(logs, "Triggering dataflow execution", 'info')
            status_code, response = trigger_dataflow(df_id)
            
            if status_code != 200:
                add_log(logs, f"Failed to trigger dataflow: {response}", 'error')
                update_backfill_progress(cur, run_id, batch_idx, 'FAILED', logs)
                conn.commit()
                raise Exception(f"Trigger failed: {response}")
            
            execution_id = response.get('id')
            add_log(logs, f"Execution started (ID: {execution_id})", 'success')
            
            # Wait for completion
            while True:
                status, _, _ = get_execution_status(df_id, execution_id)
                
                with progress_placeholder.container():
                    render_progress(batch_idx, total_batches, (start_str, end_str), status, logs, run_id)
                
                if status == 'SUCCESS':
                    add_log(logs, f"Batch {batch_idx + 1} completed successfully", 'success')
                    
                    # Update progress after successful batch
                    update_backfill_progress(cur, run_id, batch_idx + 1, 'RUNNING', logs)
                    conn.commit()
                    break
                    
                elif status in ['FAILED', 'KILLED', 'CANCELLED']:
                    add_log(logs, f"Batch {batch_idx + 1} failed: {status}", 'error')
                    update_backfill_progress(cur, run_id, batch_idx, 'FAILED', logs)
                    conn.commit()
                    raise Exception(f"Dataflow execution failed: {status}")
                
                time.sleep(poll_interval)
            
            # Check if stop requested
            if st.session_state.get('stop_backfill', False):
                add_log(logs, "Backfill paused by user", 'warning')
                update_backfill_progress(cur, run_id, batch_idx + 1, 'PAUSED', logs)
                conn.commit()
                
                # Revert views before pausing
                add_log(logs, "Reverting views to original state", 'info')
                for fqn in original_ddls:
                    cur.execute(original_ddls[fqn])
                add_log(logs, "Views reverted - safe to close", 'success')
                
                with progress_placeholder.container():
                    render_progress(batch_idx + 1, total_batches, ("", ""), "PAUSED", logs, run_id)
                
                return False, logs
        
        # All batches completed
        add_log(logs, "Backfill completed successfully", 'success')
        
        # Revert views
        add_log(logs, "Reverting views to original state", 'info')
        for fqn in original_ddls:
            cur.execute(original_ddls[fqn])
        add_log(logs, "View reversion complete", 'success')
        
        update_backfill_progress(cur, run_id, total_batches, 'COMPLETED', logs)
        conn.commit()
        
        with progress_placeholder.container():
            render_progress(total_batches, total_batches, ("", ""), "COMPLETED", logs, run_id)
        
        return True, logs
        
    except Exception as e:
        add_log(logs, f"Error: {str(e)}", 'error')
        
        # Revert views on error
        add_log(logs, "Reverting views due to error", 'warning')
        for fqn in original_ddls:
            try:
                cur.execute(original_ddls[fqn])
            except:
                pass
        
        update_backfill_progress(cur, run_id, batch_idx if 'batch_idx' in locals() else 0, 'FAILED', logs)
        conn.commit()
        
        with progress_placeholder.container():
            render_progress(batch_idx if 'batch_idx' in locals() else 0, total_batches, ("", ""), "FAILED", logs, run_id)
        
        return False, logs


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    st.set_page_config(
        page_title=APP_NAME,
        page_icon="◇",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    apply_custom_css()
    render_header()
    
    # Initialize session state
    if 'backfill_running' not in st.session_state:
        st.session_state.backfill_running = False
    if 'stop_backfill' not in st.session_state:
        st.session_state.stop_backfill = False
    if 'active_run_id' not in st.session_state:
        st.session_state.active_run_id = None
    
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    # ==========================================================================
    # CHECK FOR INCOMPLETE RUNS
    # ==========================================================================
    
    incomplete_runs = get_incomplete_runs(cur)
    
    if incomplete_runs and not st.session_state.backfill_running:
        st.markdown('<div class="section-title">Incomplete Runs</div>', unsafe_allow_html=True)
        
        for run in incomplete_runs:
            render_incomplete_run(run)
            
            col1, col2, col3 = st.columns([2, 2, 6])
            with col1:
                if st.button(f"Resume", key=f"resume_{run['run_id']}", type="primary", use_container_width=True):
                    st.session_state.active_run_id = run['run_id']
                    st.session_state.backfill_running = True
                    st.session_state.stop_backfill = False
                    st.rerun()
            with col2:
                if st.button(f"Cancel", key=f"cancel_{run['run_id']}", use_container_width=True):
                    cancel_run(cur, run['run_id'])
                    conn.commit()
                    st.success(f"Run {run['run_id']} cancelled and views reverted")
                    st.rerun()
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    # ==========================================================================
    # RESUME ACTIVE RUN
    # ==========================================================================
    
    if st.session_state.backfill_running and st.session_state.active_run_id:
        run = get_run_by_id(cur, st.session_state.active_run_id)
        
        if run and run['status'] in ('RUNNING', 'PAUSED'):
            st.markdown('<div class="section-title">Resuming Backfill</div>', unsafe_allow_html=True)
            
            # Parse stored data
            original_ddls = run['original_ddls']
            if isinstance(original_ddls, str):
                original_ddls = json.loads(original_ddls)
            
            ready_views_data = run['ready_views']
            if isinstance(ready_views_data, str):
                ready_views_data = json.loads(ready_views_data)
            ready_views = [(v['fqn'], v['actual_name']) for v in ready_views_data]
            
            # Stop button
            if st.button("Pause Backfill", use_container_width=False):
                st.session_state.stop_backfill = True
            
            progress_placeholder = st.empty()
            
            start_date = datetime.strptime(str(run['start_date']), '%Y-%m-%d')
            end_date = datetime.strptime(str(run['end_date']), '%Y-%m-%d')
            
            success, logs = run_backfill(
                run_id=run['run_id'],
                df_id=run['dataflow_id'],
                ready_views=ready_views,
                start_date=start_date,
                end_date=end_date,
                batch_days=run['batch_days'],
                poll_interval=10,
                start_batch=run['current_batch'],
                original_ddls=original_ddls,
                progress_placeholder=progress_placeholder
            )
            
            st.session_state.backfill_running = False
            st.session_state.active_run_id = None
            
            if success:
                st.markdown("""
                <div class="alert alert-success">
                    <span class="alert-title">Backfill Complete</span><br/>
                    All batches have been processed successfully.
                </div>
                """, unsafe_allow_html=True)
        else:
            st.session_state.backfill_running = False
            st.session_state.active_run_id = None
            st.rerun()
        
        return
    
    # ==========================================================================
    # SIDEBAR - NEW BACKFILL CONFIGURATION
    # ==========================================================================
    with st.sidebar:
        st.markdown("### Configuration")
        
        try:
            dataflows = list_dataflows()
            dataflow_options = {f"{df['id']} | {df['name']}": df['id'] for df in dataflows}
        except Exception as e:
            st.error(f"Failed to load dataflows: {e}")
            return
        
        st.markdown("**Select Dataflow**")
        search_term = st.text_input("Search", "", placeholder="Filter by name or ID")
        filtered_options = [opt for opt in dataflow_options.keys() if search_term.lower() in opt.lower()]
        
        if filtered_options:
            selected = st.selectbox("Dataflow", filtered_options, label_visibility="collapsed")
            selected_df_id = dataflow_options[selected]
        else:
            st.warning("No matching dataflows found")
            return
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        st.markdown("**Date Range**")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start", value=datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End", value=datetime.now() - timedelta(days=1))
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        st.markdown("**Batch Settings**")
        batch_days = st.number_input("Batch size (days)", min_value=1, max_value=30, value=3)
        poll_interval = st.number_input("Poll interval (seconds)", min_value=5, max_value=120, value=10)
        
        batches = calculate_batches(
            datetime.combine(start_date, datetime.min.time()),
            datetime.combine(end_date, datetime.min.time()),
            batch_days
        )
        
        st.markdown(f"""
        <div class="alert alert-info">
            <span class="alert-title">Execution Plan</span><br/>
            Total batches: <strong>{len(batches)}</strong>
        </div>
        """, unsafe_allow_html=True)
    
    # ==========================================================================
    # MAIN CONTENT - NEW BACKFILL
    # ==========================================================================
    
    try:
        dataflow = get_dataflow(selected_df_id)
        update_info = extract_update_method(dataflow)
        input_names = extract_input_names(dataflow)
    except Exception as e:
        st.error(f"Failed to load dataflow details: {e}")
        return
    
    render_dataflow_info({'name': dataflow.get('name')}, update_info)
    
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    snowflake_objects = [name for name in input_names if is_fully_qualified(name)]
    
    if not snowflake_objects:
        st.markdown("""
        <div class="alert alert-warning">
            <span class="alert-title">No Snowflake Views Found</span><br/>
            This dataflow does not contain any Snowflake view inputs.
        </div>
        """, unsafe_allow_html=True)
        return
    
    ready_views = []
    missing_views = []
    skipped = []
    
    for fqn in snowflake_objects:
        try:
            obj_type, actual_name = get_object_type(cur, fqn)
            
            if obj_type == "VIEW":
                ddl = get_view_ddl(cur, fqn, actual_name)
                markers = check_date_filter_markers(ddl)
                
                if markers['ready_for_backfill']:
                    ready_views.append((fqn, actual_name))
                else:
                    missing_views.append((fqn, markers))
            else:
                skipped.append((fqn, obj_type))
        except Exception as e:
            skipped.append((fqn, f"Error: {e}"))
    
    render_view_status(ready_views, missing_views, skipped)
    
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    # ==========================================================================
    # EXECUTION CONTROLS
    # ==========================================================================
    
    if not ready_views:
        st.markdown("""
        <div class="alert alert-error">
            <span class="alert-title">Cannot Proceed</span><br/>
            No views are configured for backfill. Add the required date filter markers to your Snowflake views.
        </div>
        """, unsafe_allow_html=True)
        
        with st.expander("View marker configuration instructions"):
            st.code("""
-- Add these markers to your Snowflake view WHERE clause:

WHERE date >= '2025-01-01' --{normal_date_filter}
--WHERE date BETWEEN '{from_date}' AND '{to_date}' --{temp_date_filter}
            """, language="sql")
        return
    
    st.markdown('<div class="section-title">Execution Controls</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([2, 2, 6])
    
    with col1:
        start_btn = st.button(
            "Start Backfill", 
            type="primary", 
            use_container_width=True,
            disabled=st.session_state.backfill_running
        )
    
    with col2:
        stop_btn = st.button(
            "Pause", 
            use_container_width=True,
            disabled=not st.session_state.backfill_running
        )
    
    if stop_btn:
        st.session_state.stop_backfill = True
    
    progress_placeholder = st.empty()
    
    if start_btn and not st.session_state.backfill_running:
        st.session_state.backfill_running = True
        st.session_state.stop_backfill = False
        
        # Collect original DDLs
        original_ddls = {}
        for fqn, actual_name in ready_views:
            original_ddls[fqn] = get_view_ddl(cur, fqn, actual_name)
        
        # Prepare ready_views for storage
        ready_views_data = [{'fqn': fqn, 'actual_name': actual_name} for fqn, actual_name in ready_views]
        
        # Create run record
        run_id = create_backfill_run(
            cur=cur,
            dataflow_id=selected_df_id,
            dataflow_name=dataflow.get('name'),
            start_date=datetime.combine(start_date, datetime.min.time()),
            end_date=datetime.combine(end_date, datetime.min.time()),
            batch_days=batch_days,
            total_batches=len(batches),
            original_ddls=original_ddls,
            ready_views=ready_views_data
        )
        conn.commit()
        
        st.session_state.active_run_id = run_id
        
        success, logs = run_backfill(
            run_id=run_id,
            df_id=selected_df_id,
            ready_views=ready_views,
            start_date=datetime.combine(start_date, datetime.min.time()),
            end_date=datetime.combine(end_date, datetime.min.time()),
            batch_days=batch_days,
            poll_interval=poll_interval,
            start_batch=0,
            original_ddls=original_ddls,
            progress_placeholder=progress_placeholder
        )
        
        st.session_state.backfill_running = False
        st.session_state.active_run_id = None
        
        if success:
            st.markdown("""
            <div class="alert alert-success">
                <span class="alert-title">Backfill Complete</span><br/>
                All batches have been processed successfully.
            </div>
            """, unsafe_allow_html=True)
        elif st.session_state.stop_backfill:
            st.markdown("""
            <div class="alert alert-warning">
                <span class="alert-title">Backfill Paused</span><br/>
                The backfill has been paused. You can resume it later from the incomplete runs section.
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="alert alert-error">
                <span class="alert-title">Backfill Failed</span><br/>
                An error occurred during execution. Review the log for details.
            </div>
            """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
