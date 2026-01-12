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

# =============================================================================
# CONFIGURATION
# =============================================================================

APP_NAME = "Domo Dataflow Backfill Tool"

# =============================================================================
# STYLING
# =============================================================================

def apply_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    :root {
        --keshet-blue: #6b7fa3;
        --keshet-purple: #9b8ec4;
        --keshet-pink: #c9a9bc;
        --keshet-orange: #d4a574;
        --keshet-yellow: #d4c874;
        --keshet-green: #7eb89a;
        --keshet-cyan: #7eb8c4;
        --bg-primary: #f8f9fb;
        --bg-secondary: #ffffff;
        --text-primary: #3d4a5c;
        --text-secondary: #8895a7;
        --border-color: #e8ecf1;
        --success: #7eb89a;
        --warning: #d4b574;
        --error: #c98a8a;
    }
    
    .stApp {
        background: #f5f7fa;
        font-family: 'Plus Jakarta Sans', sans-serif;
    }
    
    /* Header */
    .app-header {
        background: linear-gradient(135deg, #8b9dc3 0%, #a89bc4 50%, #c4a9b8 100%);
        padding: 2rem 2.5rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 20px rgba(139, 157, 195, 0.15);
    }
    
    .app-title {
        color: white;
        font-size: 1.75rem;
        font-weight: 600;
        margin: 0;
        letter-spacing: -0.5px;
    }
    
    .app-subtitle {
        color: rgba(255,255,255,0.8);
        font-size: 0.95rem;
        font-weight: 400;
        margin-top: 0.5rem;
    }
    
    /* Cards */
    .card {
        background: var(--bg-secondary);
        border-radius: 10px;
        padding: 1.5rem;
        border: 1px solid var(--border-color);
        box-shadow: 0 1px 3px rgba(0,0,0,0.03);
        margin-bottom: 1rem;
    }
    
    /* Stats */
    .stat-box {
        background: var(--bg-secondary);
        border-radius: 8px;
        padding: 1rem 1.25rem;
        border: 1px solid var(--border-color);
        text-align: center;
    }
    
    .stat-value {
        font-size: 1.4rem;
        font-weight: 600;
        color: #6b7fa3;
    }
    
    .stat-label {
        font-size: 0.75rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Status badges */
    .status-ready {
        background: #e8f5e9;
        color: #2e7d32;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        display: inline-block;
    }
    
    .status-missing {
        background: #fff3e0;
        color: #ef6c00;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        display: inline-block;
    }
    
    .status-skipped {
        background: #f5f5f5;
        color: #757575;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        display: inline-block;
    }
    
    .status-running {
        background: #e3f2fd;
        color: #1565c0;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        display: inline-block;
    }
    
    .status-success {
        background: #e8f5e9;
        color: #2e7d32;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        display: inline-block;
    }
    
    .status-failed {
        background: #ffebee;
        color: #c62828;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        display: inline-block;
    }
    
    /* Info boxes */
    .info-box {
        background: #f0f5ff;
        border: 1px solid #c8d8f0;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    
    .warning-box {
        background: #fff8e1;
        border: 1px solid #ffe082;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    
    .success-box {
        background: #e8f5e9;
        border: 1px solid #a5d6a7;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    
    .error-box {
        background: #ffebee;
        border: 1px solid #ef9a9a;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    
    /* Progress section */
    .progress-container {
        background: var(--bg-secondary);
        border-radius: 10px;
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
        font-size: 1rem;
        font-weight: 600;
        color: var(--text-primary);
    }
    
    .progress-percentage {
        font-size: 1.5rem;
        font-weight: 700;
        color: #6b7fa3;
    }
    
    /* Log entries */
    .log-entry {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        padding: 0.5rem;
        border-radius: 4px;
        margin: 0.25rem 0;
    }
    
    .log-info {
        background: #f8f9fb;
        color: #5a6578;
    }
    
    .log-success {
        background: #e8f5e9;
        color: #2e7d32;
    }
    
    .log-error {
        background: #ffebee;
        color: #c62828;
    }
    
    .log-warning {
        background: #fff8e1;
        color: #f57c00;
    }
    
    /* Buttons */
    .stButton > button {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-weight: 500;
        border-radius: 6px;
        padding: 0.5rem 1.5rem;
        transition: all 0.2s ease;
    }
    
    /* View list */
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
        font-size: 0.85rem;
        color: var(--text-primary);
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
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
    """Fetch all dataflows from Domo"""
    url = f"{get_domo_base_url()}/api/dataprocessing/v1/dataflows"
    response = requests.get(url, headers=get_domo_headers(), timeout=60)
    response.raise_for_status()
    return response.json()

def get_dataflow(df_id: int) -> Dict:
    """Get dataflow details"""
    url = f"{get_domo_base_url()}/api/dataprocessing/v1/dataflows/{df_id}"
    response = requests.get(url, headers=get_domo_headers(), timeout=60)
    response.raise_for_status()
    return response.json()

def trigger_dataflow(df_id: int) -> Tuple[int, Dict]:
    """Trigger a dataflow execution"""
    url = f"{get_domo_base_url()}/api/dataprocessing/v1/dataflows/{df_id}/executions"
    response = requests.post(url, headers=get_domo_headers(), timeout=60)
    return response.status_code, response.json() if response.text else {}

def get_execution_status(df_id: int, execution_id: int) -> Tuple[str, int, str]:
    """Get status of a specific execution"""
    dataflow = get_dataflow(df_id)
    last_exec = dataflow.get('lastExecution', {})
    last_exec_id = last_exec.get('id')
    last_exec_status = last_exec.get('state', 'UNKNOWN')
    
    if last_exec_id == execution_id:
        return last_exec_status, last_exec_id, last_exec_status
    else:
        return 'PENDING', last_exec_id, last_exec_status

def extract_input_names(dataflow: Dict) -> List[str]:
    """Extract input dataset names from dataflow"""
    inputs = []
    for action in dataflow.get('actions', []) or []:
        if action.get('type') == 'LoadFromVault':
            name = action.get('name')
            if name:
                inputs.append(name)
    
    # Dedupe, preserve order
    seen = set()
    out = []
    for x in inputs:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def extract_update_method(dataflow: Dict) -> Dict:
    """Extract partition/update method info from dataflow"""
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
    """Create Snowflake connection using key-pair auth"""
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
    """Check if name is DATABASE.SCHEMA.OBJECT format"""
    pattern = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
    return bool(pattern.match(name))

def get_object_type(cur, fqn: str) -> Tuple[str, Optional[str]]:
    """Check if object exists and get its type"""
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
    """Get view DDL from Snowflake"""
    db, schema, obj = fqn.split(".")
    if actual_name:
        obj = actual_name
    cur.execute(f'SELECT GET_DDL(\'VIEW\', \'{db}.{schema}."{obj}"\')')
    return cur.fetchone()[0]

def check_date_filter_markers(ddl: str) -> Dict[str, bool]:
    """Check if view has date filter markers"""
    return {
        'has_normal_marker': '--{normal_date_filter}' in ddl,
        'has_temp_marker': '--{temp_date_filter}' in ddl,
        'ready_for_backfill': '--{normal_date_filter}' in ddl and '--{temp_date_filter}' in ddl
    }

def apply_date_filter(ddl: str, from_date: str, to_date: str) -> str:
    """Transform view DDL to use temporary date filter"""
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
# UI COMPONENTS
# =============================================================================

def render_header():
    """Render app header"""
    st.markdown("""
    <div class="app-header">
        <h1 class="app-title">🔄 Domo Dataflow Backfill Tool</h1>
        <p class="app-subtitle">Backfill historical data by running dataflows in date-range batches</p>
    </div>
    """, unsafe_allow_html=True)

def render_dataflow_info(df_info: Dict, update_info: Dict):
    """Render dataflow information card"""
    partitioned = update_info.get('partitioned', False)
    partition_cols = update_info.get('partition_columns', [])
    update_method = update_info.get('update_method', 'Unknown')
    
    st.markdown("##### Dataflow Information")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="stat-box">
            <div class="stat-value" style="font-size: 1rem;">{df_info.get('name', 'Unknown')}</div>
            <div class="stat-label">Dataflow Name</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        status_class = "status-ready" if partitioned else "status-missing"
        status_text = "Yes" if partitioned else "No"
        st.markdown(f"""
        <div class="stat-box">
            <div class="stat-value"><span class="{status_class}">{status_text}</span></div>
            <div class="stat-label">Partitioned</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="stat-box">
            <div class="stat-value" style="font-size: 1rem;">{update_method or 'N/A'}</div>
            <div class="stat-label">Update Method</div>
        </div>
        """, unsafe_allow_html=True)
    
    if partition_cols:
        st.markdown(f"""
        <div class="info-box">
            <strong>Partition Columns:</strong> {', '.join(partition_cols)}
        </div>
        """, unsafe_allow_html=True)
    
    if not partitioned:
        st.markdown("""
        <div class="warning-box">
            ⚠️ <strong>Warning:</strong> This dataflow is not partitioned. Each batch may overwrite previous data instead of appending.
        </div>
        """, unsafe_allow_html=True)

def render_view_status(ready_views: List, missing_views: List, skipped: List):
    """Render view status section"""
    st.markdown("##### Input Views Status")
    
    if ready_views:
        st.markdown(f"**✅ Ready for backfill ({len(ready_views)})**")
        for fqn, _ in ready_views:
            st.markdown(f"""
            <div class="view-item">
                <span class="view-name">{fqn}</span>
                <span class="status-ready">Ready</span>
            </div>
            """, unsafe_allow_html=True)
    
    if missing_views:
        st.markdown(f"**⚠️ Missing markers ({len(missing_views)})**")
        for fqn, markers in missing_views:
            st.markdown(f"""
            <div class="view-item">
                <span class="view-name">{fqn}</span>
                <span class="status-missing">Missing Markers</span>
            </div>
            """, unsafe_allow_html=True)
    
    if skipped:
        with st.expander(f"⏭️ Skipped - not views ({len(skipped)})"):
            for fqn, obj_type in skipped:
                st.markdown(f"""
                <div class="view-item">
                    <span class="view-name">{fqn}</span>
                    <span class="status-skipped">{obj_type}</span>
                </div>
                """, unsafe_allow_html=True)

def render_progress(current_batch: int, total_batches: int, current_dates: Tuple[str, str], status: str, logs: List[str]):
    """Render progress section"""
    percentage = (current_batch / total_batches * 100) if total_batches > 0 else 0
    
    st.markdown(f"""
    <div class="progress-container">
        <div class="progress-header">
            <span class="progress-title">Backfill Progress</span>
            <span class="progress-percentage">{percentage:.0f}%</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.progress(percentage / 100)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Current Batch", f"{current_batch} / {total_batches}")
    with col2:
        st.metric("Date Range", f"{current_dates[0]} → {current_dates[1]}" if current_dates[0] else "—")
    with col3:
        status_class = {
            'RUNNING': 'status-running',
            'SUCCESS': 'status-success',
            'FAILED': 'status-failed',
            'PENDING': 'status-running'
        }.get(status, 'status-running')
        st.markdown(f'<span class="{status_class}">{status}</span>', unsafe_allow_html=True)
    
    # Logs
    if logs:
        with st.expander("Execution Log", expanded=True):
            for log in logs[-20:]:  # Show last 20 logs
                log_class = "log-success" if "✅" in log else "log-error" if "❌" in log else "log-info"
                st.markdown(f'<div class="log-entry {log_class}">{log}</div>', unsafe_allow_html=True)


# =============================================================================
# BACKFILL LOGIC
# =============================================================================

def calculate_batches(start_date: datetime, end_date: datetime, batch_days: int) -> List[Tuple[datetime, datetime]]:
    """Generate list of (batch_start, batch_end) tuples"""
    batches = []
    current = start_date
    while current <= end_date:
        batch_end = min(current + timedelta(days=batch_days - 1), end_date)
        batches.append((current, batch_end))
        current = batch_end + timedelta(days=1)
    return batches

def run_backfill(df_id: int, ready_views: List, start_date: datetime, end_date: datetime, 
                 batch_days: int, poll_interval: int, progress_placeholder, log_placeholder):
    """Run the backfill process"""
    
    batches = calculate_batches(start_date, end_date, batch_days)
    total_batches = len(batches)
    logs = []
    
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
    # Store original DDLs
    original_ddls = {}
    for fqn, actual_name in ready_views:
        original_ddls[fqn] = get_view_ddl(cur, fqn, actual_name)
    
    try:
        for batch_idx, (batch_start, batch_end) in enumerate(batches):
            start_str = batch_start.strftime('%Y-%m-%d')
            end_str = batch_end.strftime('%Y-%m-%d')
            
            logs.append(f"📅 Starting batch {batch_idx + 1}/{total_batches}: {start_str} → {end_str}")
            
            # Update progress
            with progress_placeholder.container():
                render_progress(batch_idx, total_batches, (start_str, end_str), "PREPARING", logs)
            
            # Apply date filters to views
            logs.append(f"🔧 Modifying {len(ready_views)} views...")
            for fqn, actual_name in ready_views:
                modified_ddl = apply_date_filter(original_ddls[fqn], start_str, end_str)
                cur.execute(modified_ddl)
            
            # Trigger dataflow
            logs.append(f"🚀 Triggering dataflow...")
            status_code, response = trigger_dataflow(df_id)
            
            if status_code != 200:
                logs.append(f"❌ Failed to trigger dataflow: {response}")
                raise Exception(f"Trigger failed: {response}")
            
            execution_id = response.get('id')
            logs.append(f"✅ Triggered execution ID: {execution_id}")
            
            # Wait for completion
            while True:
                status, _, _ = get_execution_status(df_id, execution_id)
                
                with progress_placeholder.container():
                    render_progress(batch_idx, total_batches, (start_str, end_str), status, logs)
                
                if status == 'SUCCESS':
                    logs.append(f"✅ Batch {batch_idx + 1} completed successfully")
                    break
                elif status in ['FAILED', 'KILLED', 'CANCELLED']:
                    logs.append(f"❌ Batch {batch_idx + 1} failed with status: {status}")
                    raise Exception(f"Dataflow failed: {status}")
                
                time.sleep(poll_interval)
            
            # Check if stop requested
            if st.session_state.get('stop_backfill', False):
                logs.append("⏹️ Backfill stopped by user")
                break
        
        logs.append("🎉 Backfill completed successfully!")
        
        with progress_placeholder.container():
            render_progress(total_batches, total_batches, ("", ""), "SUCCESS", logs)
        
        return True, logs
        
    except Exception as e:
        logs.append(f"❌ Error: {str(e)}")
        with progress_placeholder.container():
            render_progress(batch_idx if 'batch_idx' in locals() else 0, total_batches, ("", ""), "FAILED", logs)
        return False, logs
        
    finally:
        # Always revert views
        logs.append("🔄 Reverting views to original state...")
        for fqn in original_ddls:
            try:
                cur.execute(original_ddls[fqn])
            except Exception as e:
                logs.append(f"⚠️ Failed to revert {fqn}: {e}")
        logs.append("✅ Views reverted")
        cur.close()


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    st.set_page_config(
        page_title=APP_NAME,
        page_icon="🔄",
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
    
    # ==========================================================================
    # SIDEBAR - Configuration
    # ==========================================================================
    with st.sidebar:
        st.markdown("### ⚙️ Configuration")
        
        # Load dataflows
        try:
            dataflows = list_dataflows()
            dataflow_options = {f"{df['id']} | {df['name']}": df['id'] for df in dataflows}
        except Exception as e:
            st.error(f"Failed to load dataflows: {e}")
            return
        
        # Search and select dataflow
        search_term = st.text_input("🔍 Search dataflows", "")
        filtered_options = [opt for opt in dataflow_options.keys() if search_term.lower() in opt.lower()]
        
        if filtered_options:
            selected = st.selectbox("Select dataflow", filtered_options)
            selected_df_id = dataflow_options[selected]
        else:
            st.warning("No dataflows match your search")
            return
        
        st.markdown("---")
        
        # Date range
        st.markdown("### 📅 Date Range")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now() - timedelta(days=1))
        
        # Batch size
        batch_days = st.number_input("Batch Size (days)", min_value=1, max_value=30, value=3)
        
        # Poll interval
        poll_interval = st.number_input("Poll Interval (seconds)", min_value=5, max_value=120, value=10)
        
        # Calculate batches
        batches = calculate_batches(
            datetime.combine(start_date, datetime.min.time()),
            datetime.combine(end_date, datetime.min.time()),
            batch_days
        )
        
        st.markdown(f"""
        <div class="info-box">
            📊 This will run <strong>{len(batches)} batches</strong>
        </div>
        """, unsafe_allow_html=True)
    
    # ==========================================================================
    # MAIN CONTENT
    # ==========================================================================
    
    # Get dataflow info
    try:
        dataflow = get_dataflow(selected_df_id)
        update_info = extract_update_method(dataflow)
        input_names = extract_input_names(dataflow)
    except Exception as e:
        st.error(f"Failed to load dataflow details: {e}")
        return
    
    # Render dataflow info
    render_dataflow_info({'name': dataflow.get('name')}, update_info)
    
    st.markdown("---")
    
    # Analyze views
    snowflake_objects = [name for name in input_names if is_fully_qualified(name)]
    
    if not snowflake_objects:
        st.warning("No Snowflake views found in this dataflow's inputs.")
        return
    
    # Check view status
    conn = get_snowflake_connection()
    cur = conn.cursor()
    
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
    
    cur.close()
    
    # Render view status
    render_view_status(ready_views, missing_views, skipped)
    
    st.markdown("---")
    
    # ==========================================================================
    # BACKFILL CONTROLS
    # ==========================================================================
    
    if not ready_views:
        st.error("❌ No views are ready for backfill. Please add the required date filter markers to your views.")
        st.markdown("""
        **Required markers in your Snowflake view:**
```sql
        where date >= '2025-01-01' --{normal_date_filter}
        --WHERE date BETWEEN '{from_date}' AND '{to_date}' --{temp_date_filter}
```
        """)
        return
    
    # Start/Stop buttons
    col1, col2, col3 = st.columns([2, 2, 4])
    
    with col1:
        start_btn = st.button(
            "▶️ Start Backfill", 
            type="primary", 
            use_container_width=True,
            disabled=st.session_state.backfill_running
        )
    
    with col2:
        stop_btn = st.button(
            "⏹️ Stop", 
            use_container_width=True,
            disabled=not st.session_state.backfill_running
        )
    
    if stop_btn:
        st.session_state.stop_backfill = True
    
    # Progress placeholder
    progress_placeholder = st.empty()
    log_placeholder = st.empty()
    
    # Run backfill
    if start_btn and not st.session_state.backfill_running:
        st.session_state.backfill_running = True
        st.session_state.stop_backfill = False
        
        success, logs = run_backfill(
            df_id=selected_df_id,
            ready_views=ready_views,
            start_date=datetime.combine(start_date, datetime.min.time()),
            end_date=datetime.combine(end_date, datetime.min.time()),
            batch_days=batch_days,
            poll_interval=poll_interval,
            progress_placeholder=progress_placeholder,
            log_placeholder=log_placeholder
        )
        
        st.session_state.backfill_running = False
        
        if success:
            st.balloons()
            st.success("🎉 Backfill completed successfully!")
        else:
            st.error("❌ Backfill failed. Check the logs for details.")


if __name__ == "__main__":
    main()
