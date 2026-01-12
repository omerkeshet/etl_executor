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
from streamlit_autorefresh import st_autorefresh

# =============================================================================
# CONFIGURATION
# =============================================================================

APP_NAME = "Dataflow Backfill Manager"
BACKFILL_TABLE = "MAKO_DATA_LAKE.PUBLIC.BACKFILL_RUNS"
AUTO_REFRESH_SECONDS = 5

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
    
    .status-pending {
        background: var(--bg-tertiary);
        color: var(--text-secondary);
        border: 1px solid var(--border-color);
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
    
    /* Run card */
    .run-card {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 1.25rem;
        margin-bottom: 1rem;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
    }
    
    .run-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    .run-card-title {
        font-size: 1rem;
        font-weight: 600;
        color: var(--text-primary);
    }
    
    .run-card-id {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        color: var(--text-muted);
    }
    
    .run-card-details {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 1rem;
        margin-bottom: 1rem;
    }
    
    .run-card-detail {
        text-align: center;
    }
    
    .run-card-detail-value {
        font-size: 1rem;
        font-weight: 600;
        color: var(--text-primary);
    }
    
    .run-card-detail-label {
        font-size: 0.7rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .run-card-progress {
        margin-top: 0.75rem;
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
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background: var(--bg-secondary);
        border-right: 1px solid var(--border-color);
    }
    
    section[data-testid="stSidebar"] .stMarkdown p strong {
        color: #2d3748;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
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
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background: var(--bg-tertiary);
        padding: 4px;
        border-radius: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px;
        padding: 0.5rem 1rem;
        font-weight: 500;
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

def get_execution_status(df_id: int, execution_id: int) -> str:
    dataflow = get_dataflow(df_id)
    last_exec = dataflow.get('lastExecution', {})
    last_exec_id = last_exec.get('id')
    last_exec_status = last_exec.get('state', 'UNKNOWN')
    
    if last_exec_id == execution_id:
        return last_exec_status
    else:
        return 'PENDING'

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

def revert_view(cur, fqn: str, original_ddl: str):
    try:
        cur.execute(original_ddl)
        return True
    except Exception as e:
        return False


# =============================================================================
# STATE PERSISTENCE FUNCTIONS
# =============================================================================

def create_backfill_run(cur, dataflow_id: int, dataflow_name: str, start_date: datetime, 
                        end_date: datetime, batch_days: int, total_batches: int,
                        original_ddls: Dict, ready_views: List, poll_interval: int) -> str:
    run_id = str(uuid.uuid4())[:8]
    
    cur.execute(f"""
        INSERT INTO {BACKFILL_TABLE} 
        (run_id, dataflow_id, dataflow_name, start_date, end_date, batch_days, 
         current_batch, total_batches, status, original_ddls, ready_views, logs,
         current_execution_id, poll_interval)
        SELECT 
            %(run_id)s,
            %(dataflow_id)s,
            %(dataflow_name)s,
            %(start_date)s,
            %(end_date)s,
            %(batch_days)s,
            0,
            %(total_batches)s,
            'PENDING',
            PARSE_JSON(%(original_ddls)s),
            PARSE_JSON(%(ready_views)s),
            PARSE_JSON('[]'),
            NULL,
            %(poll_interval)s
    """, {
        'run_id': run_id,
        'dataflow_id': dataflow_id,
        'dataflow_name': dataflow_name,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'batch_days': batch_days,
        'total_batches': total_batches,
        'original_ddls': json.dumps(original_ddls),
        'ready_views': json.dumps(ready_views),
        'poll_interval': poll_interval
    })
    
    return run_id

def update_run_status(cur, run_id: str, status: str, current_batch: int = None, 
                      execution_id: int = None, logs: List[Dict] = None):
    updates = ["status = %(status)s", "updated_at = CURRENT_TIMESTAMP()"]
    params = {'run_id': run_id, 'status': status}
    
    if current_batch is not None:
        updates.append("current_batch = %(current_batch)s")
        params['current_batch'] = current_batch
    
    if execution_id is not None:
        updates.append("current_execution_id = %(execution_id)s")
        params['execution_id'] = execution_id
    elif execution_id == 0:
        updates.append("current_execution_id = NULL")
    
    if logs is not None:
        updates.append("logs = PARSE_JSON(%(logs)s)")
        params['logs'] = json.dumps(logs)
    
    cur.execute(f"""
        UPDATE {BACKFILL_TABLE}
        SET {', '.join(updates)}
        WHERE run_id = %(run_id)s
    """, params)

def get_active_runs(cur) -> List[Dict]:
    cur.execute(f"""
        SELECT run_id, dataflow_id, dataflow_name, start_date, end_date, batch_days,
               current_batch, total_batches, status, original_ddls, ready_views, logs,
               created_at, updated_at, current_execution_id, poll_interval
        FROM {BACKFILL_TABLE}
        WHERE status IN ('PENDING', 'RUNNING', 'WAITING')
        ORDER BY created_at ASC
    """)
    
    rows = cur.fetchall()
    columns = ['run_id', 'dataflow_id', 'dataflow_name', 'start_date', 'end_date', 
               'batch_days', 'current_batch', 'total_batches', 'status', 
               'original_ddls', 'ready_views', 'logs', 'created_at', 'updated_at',
               'current_execution_id', 'poll_interval']
    
    return [dict(zip(columns, row)) for row in rows]

def get_all_runs(cur, limit: int = 20) -> List[Dict]:
    cur.execute(f"""
        SELECT run_id, dataflow_id, dataflow_name, start_date, end_date, batch_days,
               current_batch, total_batches, status, original_ddls, ready_views, logs,
               created_at, updated_at, current_execution_id, poll_interval
        FROM {BACKFILL_TABLE}
        ORDER BY created_at DESC
        LIMIT {limit}
    """)
    
    rows = cur.fetchall()
    columns = ['run_id', 'dataflow_id', 'dataflow_name', 'start_date', 'end_date', 
               'batch_days', 'current_batch', 'total_batches', 'status', 
               'original_ddls', 'ready_views', 'logs', 'created_at', 'updated_at',
               'current_execution_id', 'poll_interval']
    
    return [dict(zip(columns, row)) for row in rows]

def get_run_by_id(cur, run_id: str) -> Optional[Dict]:
    cur.execute(f"""
        SELECT run_id, dataflow_id, dataflow_name, start_date, end_date, batch_days,
               current_batch, total_batches, status, original_ddls, ready_views, logs,
               created_at, updated_at, current_execution_id, poll_interval
        FROM {BACKFILL_TABLE}
        WHERE run_id = %(run_id)s
    """, {'run_id': run_id})
    
    row = cur.fetchone()
    if not row:
        return None
    
    columns = ['run_id', 'dataflow_id', 'dataflow_name', 'start_date', 'end_date', 
               'batch_days', 'current_batch', 'total_batches', 'status', 
               'original_ddls', 'ready_views', 'logs', 'created_at', 'updated_at',
               'current_execution_id', 'poll_interval']
    
    return dict(zip(columns, row))


# =============================================================================
# BATCH EXECUTION LOGIC
# =============================================================================

def calculate_batch_dates(start_date: datetime, end_date: datetime, batch_days: int, batch_index: int) -> Tuple[datetime, datetime]:
    batch_start = start_date + timedelta(days=batch_index * batch_days)
    batch_end = min(batch_start + timedelta(days=batch_days - 1), end_date)
    return batch_start, batch_end

def calculate_total_batches(start_date: datetime, end_date: datetime, batch_days: int) -> int:
    total_days = (end_date - start_date).days + 1
    return (total_days + batch_days - 1) // batch_days

def add_log(logs: List[Dict], message: str, log_type: str = 'info') -> List[Dict]:
    logs.append({
        'timestamp': datetime.now().strftime('%H:%M:%S'),
        'message': message,
        'type': log_type
    })
    return logs

def parse_json_field(value) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value

def process_single_run(cur, run: Dict) -> bool:
    run_id = run['run_id']
    status = run['status']
    dataflow_id = run['dataflow_id']
    current_batch = run['current_batch']
    total_batches = run['total_batches']
    
    original_ddls = parse_json_field(run['original_ddls'])
    ready_views = parse_json_field(run['ready_views'])
    logs = parse_json_field(run['logs']) or []
    
    start_date = datetime.strptime(str(run['start_date']), '%Y-%m-%d')
    end_date = datetime.strptime(str(run['end_date']), '%Y-%m-%d')
    batch_days = run['batch_days']
    
    if status == 'PENDING':
        if current_batch >= total_batches:
            logs = add_log(logs, "Backfill completed", 'success')
            update_run_status(cur, run_id, 'COMPLETED', logs=logs)
            return True
        
        batch_start, batch_end = calculate_batch_dates(start_date, end_date, batch_days, current_batch)
        start_str = batch_start.strftime('%Y-%m-%d')
        end_str = batch_end.strftime('%Y-%m-%d')
        
        logs = add_log(logs, f"Starting batch {current_batch + 1}/{total_batches}: {start_str} to {end_str}")
        logs = add_log(logs, f"Applying date filters to {len(ready_views)} view(s)")
        
        for view_info in ready_views:
            fqn = view_info['fqn']
            modified_ddl = apply_date_filter(original_ddls[fqn], start_str, end_str)
            cur.execute(modified_ddl)
        
        logs = add_log(logs, "Triggering dataflow execution")
        status_code, response = trigger_dataflow(dataflow_id)
        
        if status_code != 200:
            logs = add_log(logs, f"Failed to trigger dataflow: {response}", 'error')
            for view_info in ready_views:
                revert_view(cur, view_info['fqn'], original_ddls[view_info['fqn']])
            update_run_status(cur, run_id, 'FAILED', logs=logs)
            return True
        
        execution_id = response.get('id')
        logs = add_log(logs, f"Execution started (ID: {execution_id})", 'success')
        
        update_run_status(cur, run_id, 'WAITING', execution_id=execution_id, logs=logs)
        return True
    
    elif status == 'WAITING':
        execution_id = run['current_execution_id']
        
        if not execution_id:
            logs = add_log(logs, "No execution ID found, restarting batch", 'warning')
            update_run_status(cur, run_id, 'PENDING', logs=logs)
            return True
        
        exec_status = get_execution_status(dataflow_id, execution_id)
        
        if exec_status == 'SUCCESS':
            logs = add_log(logs, f"Batch {current_batch + 1} completed successfully", 'success')
            
            next_batch = current_batch + 1
            
            if next_batch >= total_batches:
                logs = add_log(logs, "All batches completed, reverting views")
                for view_info in ready_views:
                    revert_view(cur, view_info['fqn'], original_ddls[view_info['fqn']])
                logs = add_log(logs, "Backfill completed successfully", 'success')
                update_run_status(cur, run_id, 'COMPLETED', current_batch=next_batch, logs=logs)
            else:
                update_run_status(cur, run_id, 'RUNNING', current_batch=next_batch, execution_id=0, logs=logs)
            
            return True
        
        elif exec_status in ['FAILED', 'KILLED', 'CANCELLED']:
            logs = add_log(logs, f"Batch {current_batch + 1} failed: {exec_status}", 'error')
            for view_info in ready_views:
                revert_view(cur, view_info['fqn'], original_ddls[view_info['fqn']])
            update_run_status(cur, run_id, 'FAILED', logs=logs)
            return True
        
        else:
            return False
    
    elif status == 'RUNNING':
        if current_batch >= total_batches:
            logs = add_log(logs, "Backfill completed", 'success')
            for view_info in ready_views:
                revert_view(cur, view_info['fqn'], original_ddls[view_info['fqn']])
            update_run_status(cur, run_id, 'COMPLETED', logs=logs)
            return True
        
        batch_start, batch_end = calculate_batch_dates(start_date, end_date, batch_days, current_batch)
        start_str = batch_start.strftime('%Y-%m-%d')
        end_str = batch_end.strftime('%Y-%m-%d')
        
        logs = add_log(logs, f"Starting batch {current_batch + 1}/{total_batches}: {start_str} to {end_str}")
        logs = add_log(logs, f"Applying date filters to {len(ready_views)} view(s)")
        
        for view_info in ready_views:
            fqn = view_info['fqn']
            modified_ddl = apply_date_filter(original_ddls[fqn], start_str, end_str)
            cur.execute(modified_ddl)
        
        logs = add_log(logs, "Triggering dataflow execution")
        status_code, response = trigger_dataflow(dataflow_id)
        
        if status_code != 200:
            logs = add_log(logs, f"Failed to trigger dataflow: {response}", 'error')
            for view_info in ready_views:
                revert_view(cur, view_info['fqn'], original_ddls[view_info['fqn']])
            update_run_status(cur, run_id, 'FAILED', logs=logs)
            return True
        
        execution_id = response.get('id')
        logs = add_log(logs, f"Execution started (ID: {execution_id})", 'success')
        
        update_run_status(cur, run_id, 'WAITING', execution_id=execution_id, logs=logs)
        return True
    
    return False

def process_all_active_runs(cur) -> int:
    active_runs = get_active_runs(cur)
    processed = 0
    
    for run in active_runs:
        try:
            if process_single_run(cur, run):
                processed += 1
        except Exception as e:
            logs = parse_json_field(run['logs']) or []
            logs = add_log(logs, f"Error processing run: {str(e)}", 'error')
            
            try:
                original_ddls = parse_json_field(run['original_ddls'])
                ready_views = parse_json_field(run['ready_views'])
                for view_info in ready_views:
                    revert_view(cur, view_info['fqn'], original_ddls[view_info['fqn']])
            except:
                pass
            
            update_run_status(cur, run['run_id'], 'FAILED', logs=logs)
            processed += 1
    
    return processed


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

def render_run_card(run: Dict, show_actions: bool = True):
    status = run['status']
    progress = (run['current_batch'] / run['total_batches'] * 100) if run['total_batches'] > 0 else 0
    
    status_class = {
        'PENDING': 'status-pending',
        'RUNNING': 'status-running',
        'WAITING': 'status-running',
        'COMPLETED': 'status-success',
        'FAILED': 'status-failed',
        'PAUSED': 'status-paused',
        'CANCELLED': 'status-error'
    }.get(status, 'status-neutral')
    
    st.markdown(f"""
    <div class="run-card">
        <div class="run-card-header">
            <div>
                <div class="run-card-title">{run['dataflow_name']}</div>
                <div class="run-card-id">Run ID: {run['run_id']}</div>
            </div>
            <span class="status-indicator {status_class}">{status}</span>
        </div>
        <div class="run-card-details">
            <div class="run-card-detail">
                <div class="run-card-detail-value">{run['current_batch']} / {run['total_batches']}</div>
                <div class="run-card-detail-label">Batches</div>
            </div>
            <div class="run-card-detail">
                <div class="run-card-detail-value">{progress:.0f}%</div>
                <div class="run-card-detail-label">Progress</div>
            </div>
            <div class="run-card-detail">
                <div class="run-card-detail-value">{run['start_date']} to {run['end_date']}</div>
                <div class="run-card-detail-label">Date Range</div>
            </div>
            <div class="run-card-detail">
                <div class="run-card-detail-value">{run['batch_days']} days</div>
                <div class="run-card-detail-label">Batch Size</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.progress(progress / 100)
    
    if show_actions:
        col1, col2, col3 = st.columns([1, 1, 4])
        
        with col1:
            if status in ('PENDING', 'RUNNING', 'WAITING'):
                if st.button("Pause", key=f"pause_{run['run_id']}", use_container_width=True):
                    return ('pause', run['run_id'])
            elif status == 'PAUSED':
                if st.button("Resume", key=f"resume_{run['run_id']}", type="primary", use_container_width=True):
                    return ('resume', run['run_id'])
        
        with col2:
            if status in ('PENDING', 'RUNNING', 'WAITING', 'PAUSED'):
                if st.button("Cancel", key=f"cancel_{run['run_id']}", use_container_width=True):
                    return ('cancel', run['run_id'])
        
        logs = parse_json_field(run['logs']) or []
        if logs:
            with st.expander("Execution Log", expanded=False):
                log_html = '<div class="log-container">'
                for log in logs[-30:]:
                    log_class = log.get('type', 'info')
                    timestamp = log.get('timestamp', '')
                    message = log.get('message', '')
                    log_html += f'<div class="log-entry {log_class}"><span class="log-timestamp">{timestamp}</span><span class="log-message">{message}</span></div>'
                log_html += '</div>'
                st.markdown(log_html, unsafe_allow_html=True)
    
    return None

def render_dataflow_info(df_info: Dict, update_info: Dict):
    partitioned = update_info.get('partitioned', False)
    partition_cols = update_info.get('partition_columns', [])
    update_method = update_info.get('update_method', 'Unknown')
    
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
    
    if not partitioned:
        st.markdown("""
        <div class="alert alert-warning">
            <span class="alert-title">Warning</span><br/>
            This dataflow does not use partition updates. Each batch execution may overwrite previous data.
        </div>
        """, unsafe_allow_html=True)

def render_view_status(ready_views: List, missing_views: List, skipped: List):
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
    
    if ready_views:
        with st.expander(f"Ready for backfill ({len(ready_views)})", expanded=False):
            for fqn, _ in ready_views:
                st.markdown(f"""
                <div class="view-item">
                    <span class="view-name">{fqn}</span>
                    <span class="status-indicator status-ready">Ready</span>
                </div>
                """, unsafe_allow_html=True)
    
    if missing_views:
        with st.expander(f"Missing markers ({len(missing_views)})"):
            for fqn, markers in missing_views:
                st.markdown(f"""
                <div class="view-item">
                    <span class="view-name">{fqn}</span>
                    <span class="status-indicator status-warning">Missing</span>
                </div>
                """, unsafe_allow_html=True)


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    st.set_page_config(
        page_title=APP_NAME,
        page_icon="◇",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    apply_custom_css()
    render_header()
    
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    # Process any active runs
    process_all_active_runs(cur)
    conn.commit()
    
    # Get all runs for display
    all_runs = get_all_runs(cur)
    active_runs = [r for r in all_runs if r['status'] in ('PENDING', 'RUNNING', 'WAITING')]
    
    # ==========================================================================
    # TABS
    # ==========================================================================
    
    tab1, tab2 = st.tabs(["Active Runs", "New Backfill"])
    
    # ==========================================================================
    # TAB 1: ACTIVE RUNS
    # ==========================================================================
    
    with tab1:
        if active_runs:
            st.markdown(f'<div class="section-title">Active Backfills ({len(active_runs)})</div>', unsafe_allow_html=True)
            
            for run in active_runs:
                action = render_run_card(run)
                
                if action:
                    action_type, run_id = action
                    run_data = get_run_by_id(cur, run_id)
                    
                    if action_type == 'pause':
                        logs = parse_json_field(run_data['logs']) or []
                        logs = add_log(logs, "Backfill paused by user", 'warning')
                        
                        original_ddls = parse_json_field(run_data['original_ddls'])
                        ready_views = parse_json_field(run_data['ready_views'])
                        for view_info in ready_views:
                            revert_view(cur, view_info['fqn'], original_ddls[view_info['fqn']])
                        
                        update_run_status(cur, run_id, 'PAUSED', logs=logs)
                        conn.commit()
                        st.rerun()
                    
                    elif action_type == 'resume':
                        logs = parse_json_field(run_data['logs']) or []
                        logs = add_log(logs, "Backfill resumed by user", 'info')
                        update_run_status(cur, run_id, 'RUNNING', logs=logs)
                        conn.commit()
                        st.rerun()
                    
                    elif action_type == 'cancel':
                        logs = parse_json_field(run_data['logs']) or []
                        logs = add_log(logs, "Backfill cancelled by user", 'warning')
                        
                        original_ddls = parse_json_field(run_data['original_ddls'])
                        ready_views = parse_json_field(run_data['ready_views'])
                        for view_info in ready_views:
                            revert_view(cur, view_info['fqn'], original_ddls[view_info['fqn']])
                        
                        update_run_status(cur, run_id, 'CANCELLED', logs=logs)
                        conn.commit()
                        st.rerun()
                
                st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
            
            st.markdown(f"""
            <div class="alert alert-info">
                <span class="alert-title">Auto-Refresh Active</span><br/>
                Progress updates every {AUTO_REFRESH_SECONDS} seconds. Switch to "New Backfill" tab to start additional backfills.
            </div>
            """, unsafe_allow_html=True)
        
        else:
            st.markdown("""
            <div class="alert alert-info">
                <span class="alert-title">No Active Backfills</span><br/>
                There are no backfills currently running. Go to "New Backfill" tab to start one.
            </div>
            """, unsafe_allow_html=True)
        
        # Show recent completed/failed runs
        completed_runs = [r for r in all_runs if r['status'] in ('COMPLETED', 'FAILED', 'CANCELLED', 'PAUSED')]
        if completed_runs:
            st.markdown('<div class="section-title">Recent Runs</div>', unsafe_allow_html=True)
            
            for run in completed_runs[:5]:
                render_run_card(run, show_actions=(run['status'] == 'PAUSED'))
                st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    # ==========================================================================
    # TAB 2: NEW BACKFILL
    # ==========================================================================
    
    with tab2:
        col_config, col_preview = st.columns([1, 2])
        
        with col_config:
            st.markdown('<div class="section-title">Configuration</div>', unsafe_allow_html=True)
            
            try:
                dataflows = list_dataflows()
                dataflow_options = {f"{df['id']} | {df['name']}": df['id'] for df in dataflows}
            except Exception as e:
                st.error(f"Failed to load dataflows: {e}")
                return
            
            st.markdown("**Select Dataflow**")
            search_term = st.text_input("Search", "", placeholder="Filter by name or ID", key="df_search")
            filtered_options = [opt for opt in dataflow_options.keys() if search_term.lower() in opt.lower()]
            
            if filtered_options:
                selected = st.selectbox("Dataflow", filtered_options, label_visibility="collapsed", key="df_select")
                selected_df_id = dataflow_options[selected]
            else:
                st.warning("No matching dataflows found")
                return
            
            st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
            
            st.markdown("**Date Range**")
            date_col1, date_col2 = st.columns(2)
            with date_col1:
                start_date = st.date_input("Start", value=datetime.now() - timedelta(days=30), key="start_date")
            with date_col2:
                end_date = st.date_input("End", value=datetime.now() - timedelta(days=1), key="end_date")
            
            st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
            
            st.markdown("**Batch Settings**")
            batch_days = st.number_input("Batch size (days)", min_value=1, max_value=30, value=3, key="batch_days")
            poll_interval = st.number_input("Poll interval (seconds)", min_value=5, max_value=120, value=10, key="poll_interval")
            
            total_batches = calculate_total_batches(
                datetime.combine(start_date, datetime.min.time()),
                datetime.combine(end_date, datetime.min.time()),
                batch_days
            )
            
            st.markdown(f"""
            <div class="alert alert-info">
                <span class="alert-title">Execution Plan</span><br/>
                Total batches: <strong>{total_batches}</strong>
            </div>
            """, unsafe_allow_html=True)
        
        with col_preview:
            st.markdown('<div class="section-title">Dataflow Preview</div>', unsafe_allow_html=True)
            
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
            else:
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
                
                st.markdown('<div class="section-title">Input Sources</div>', unsafe_allow_html=True)
                render_view_status(ready_views, missing_views, skipped)
                
                st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
                
                # Check for conflicts with active runs
                conflicting_views = []
                for run in active_runs:
                    run_views = parse_json_field(run['ready_views']) or []
                    run_view_fqns = [v['fqn'] for v in run_views]
                    for fqn, _ in ready_views:
                        if fqn in run_view_fqns:
                            conflicting_views.append((fqn, run['run_id'], run['dataflow_name']))
                
                if conflicting_views:
                    st.markdown("""
                    <div class="alert alert-error">
                        <span class="alert-title">View Conflict Detected</span><br/>
                        The following views are already being used by active backfills:
                    </div>
                    """, unsafe_allow_html=True)
                    for fqn, run_id, df_name in conflicting_views:
                        st.markdown(f"- `{fqn}` (Run {run_id}: {df_name})")
                    
                    st.warning("Cannot start backfill while views are in use by another run.")
                
                elif not ready_views:
                    st.markdown("""
                    <div class="alert alert-error">
                        <span class="alert-title">Cannot Proceed</span><br/>
                        No views are configured for backfill. Add the required date filter markers.
                    </div>
                    """, unsafe_allow_html=True)
                    
                    with st.expander("View marker configuration instructions"):
                        st.code("""
-- Add these markers to your Snowflake view WHERE clause:

WHERE date >= '2025-01-01' --{normal_date_filter}
--WHERE date BETWEEN '{from_date}' AND '{to_date}' --{temp_date_filter}
                        """, language="sql")
                
                else:
                    st.markdown('<div class="section-title">Start Backfill</div>', unsafe_allow_html=True)
                    
                    if st.button("Start Backfill", type="primary", use_container_width=True):
                        original_ddls = {}
                        for fqn, actual_name in ready_views:
                            original_ddls[fqn] = get_view_ddl(cur, fqn, actual_name)
                        
                        ready_views_data = [{'fqn': fqn, 'actual_name': actual_name} for fqn, actual_name in ready_views]
                        
                        run_id = create_backfill_run(
                            cur=cur,
                            dataflow_id=selected_df_id,
                            dataflow_name=dataflow.get('name'),
                            start_date=datetime.combine(start_date, datetime.min.time()),
                            end_date=datetime.combine(end_date, datetime.min.time()),
                            batch_days=batch_days,
                            total_batches=total_batches,
                            original_ddls=original_ddls,
                            ready_views=ready_views_data,
                            poll_interval=poll_interval
                        )
                        conn.commit()
                        
                        st.success(f"Backfill started with Run ID: {run_id}")
                        time.sleep(1)
                        st.rerun()
    
# ==========================================================================
# AUTO-REFRESH
# ==========================================================================

if active_runs:
    waiting_runs = [r for r in active_runs if r['status'] == 'WAITING']
    
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("Refresh", use_container_width=True):
            st.rerun()
    
    if waiting_runs:
        # Only auto-refresh when waiting for Domo execution
        st_autorefresh(interval=10 * 1000, key="datarefresh")


if __name__ == "__main__":
    main()
