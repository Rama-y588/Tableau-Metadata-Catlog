import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import os
import re

# Set page config with a modern theme
st.set_page_config(
    page_title="Tableau Metadata Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for navigation
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'main'
if 'selected_workbook' not in st.session_state:
    st.session_state.selected_workbook = None

st.markdown("""
    <style>
    /* General page styling */
    .main {
        background-color: #f7f9fc;
        padding: 2rem;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        color: #1e1e2f; /* Dark text for readability */
    }

  .hr-thin-white {
    border: none;
    height: 1px;
    background-color: rgba(255, 255, 255, 0.3); /* Subtle white */
    margin: 1.5rem 0;
}


  

    /* Metric card styling */
    .metric-card {
        background: linear-gradient(145deg, #ffffff, #f0f2f8);
        padding: 1.25rem;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
        text-align: center;
        margin-bottom: 1.5rem;
        border-left: 5px solid #4e73df;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        color: #111111; /* Strong dark text */
    }

    .metric-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.12);
    }

    .metric-card h3 {
        color: #2e59d9;
        font-size: 1.25rem;
        margin-bottom: 0.5rem;
        font-weight: 600;
    }

    .metric-card p {
        color: #4a4a4a;
        font-size: 1.05rem;
        margin: 0;
        font-weight: 500;
    }

    /* Sidebar styling */
    .css-1d391kg {
        background-color: #1e2a44;
        color: #ffffff;
        padding: 1.5rem;
    }

    .css-1d391kg h1,
    .css-1d391kg h2,
    .css-1d391kg h3 {
        color: #ffffff;
    }

    .css-1d391kg h1:hover,
    .css-1d391kg h2:hover,
    .css-1d391kg h3:hover {
        color: #a0c4ff;
        text-decoration: underline;
        font-weight: bold;
    }

    /* Button styling */
    .stButton>button {
        background-color: #4e73df;
        color: white;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        transition: background-color 0.2s ease;
        font-weight: 600;
        font-size: 0.95rem;
    }

    .stButton>button:hover {
        background-color: #2e59d9;
    }

    /* Header styling */
    h1, h2, h3 {
        color: #1e2a44;
        font-weight: 600;
        transition: color 0.2s ease, text-decoration 0.2s ease;
        cursor: pointer;
    }

    h1:hover, h2:hover, h3:hover {
        color: #2e59d9;
        text-decoration: underline;
        font-weight: bold;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab"] {
        background-color: #ffffff;
        border-radius: 8px 8px 0 0;
        padding: 0.75rem 1.5rem;
        font-weight: 500;
        color: #2f2f2f;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background-color: #f0f4ff;
        color: #1e2a44;
    }

    /* Ensure metrics are visible */
    .stMetric {
        background-color: transparent !important;
        padding: 0 !important;
        margin: 0 !important;
        font-size: 1.1rem !important;
        font-weight: 600 !important;
        color: #1e1e2f !important;
    }

    </style>
""", unsafe_allow_html=True)


def load_data() -> Dict[str, pd.DataFrame]:
    """Load all CSV files into DataFrames with error handling"""
    data_dir = Path(os.getenv('DATA_DIR', Path(__file__).parent.parent.parent / 'data'))
    
    try:
        data = {
            'workbooks': pd.read_csv(data_dir / 'workbooks.csv'),
            'views': pd.read_csv(data_dir / 'views.csv'),
            'datasources': pd.read_csv(data_dir / 'datasources.csv'),
            'connections': pd.read_csv(data_dir / 'connections.csv'),
            'workbook_tags': pd.read_csv(data_dir / 'workbook_tags.csv'),
            'workbook_datasources': pd.read_csv(data_dir / 'workbook_datasources.csv')
        }
        
        # Validate workbook_datasources has required columns
        required_columns = ['workbook_id', 'datasource_id', 'is_embedded']
        if not all(col in data['workbook_datasources'].columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in data['workbook_datasources'].columns]
            st.error(f"Missing required columns in workbook_datasources.csv: {missing_cols}")
            return {}
        
        # Convert is_embedded to boolean
        if 'is_embedded' in data['workbook_datasources'].columns:
            try:
                data['workbook_datasources']['is_embedded'] = data['workbook_datasources']['is_embedded'].map(
                    {'true': True, 'false': False, True: True, False: False}
                ).astype(bool)
            except Exception as e:
                st.error(f"Error converting 'is_embedded' column to boolean: {e}")
                return {}
        
        return data
    except FileNotFoundError as e:
        st.error(f"Error loading data: {e}. Please ensure the data directory and files exist.")
        return {}
    except Exception as e:
        st.error(f"Unexpected error loading data: {e}")
        return {}

def get_workbook_details(data: Dict[str, pd.DataFrame], workbook_id: str) -> Dict[str, Any]:
    """Get detailed information for a specific workbook"""
    workbook = data['workbooks'][data['workbooks']['workbook_id'] == workbook_id]
    if workbook.empty:
        st.warning(f"No workbook found with ID: {workbook_id}")
        return {}
    
    workbook = workbook.iloc[0]
    
    # Get views
    views = data['views'][data['views']['workbook_id'] == workbook_id]
    
    # Get tags
    tags = data['workbook_tags'][data['workbook_tags']['workbook_id'] == workbook_id]['tag_name'].tolist()
    
    # Get datasources
    workbook_ds = data['workbook_datasources'][data['workbook_datasources']['workbook_id'] == workbook_id]
    
    upstream_datasources = []
    embedded_datasources = []
    
    if 'is_embedded' not in workbook_ds.columns:
        st.error("The 'is_embedded' column is missing in workbook_datasources DataFrame.")
        return {
            'id': workbook['workbook_id'],
            'name': workbook['workbook_name'],
            'projectName': workbook['project_name'],
            'owner': {
                'name': workbook['owner_name'],
                'email': workbook['owner_email']
            },
            'views': views.to_dict('records'),
            'tags': tags,
            'upstreamDatasources': [],
            'embeddedDatasources': []
        }
    
    upstream_ds = workbook_ds[~workbook_ds['is_embedded']]
    embedded_ds = workbook_ds[workbook_ds['is_embedded']]
    
    for _, ds in upstream_ds.iterrows():
        ds_info = data['datasources'][data['datasources']['datasource_id'] == ds['datasource_id']]
        if ds_info.empty:
            continue
        ds_info = ds_info.iloc[0]
        conn_info = data['connections'][data['connections']['datasource_id'] == ds['datasource_id']]
        upstream_datasources.append({
            'id': ds_info['datasource_id'],
            'name': ds_info['datasource_name'],
            'hasExtracts': ds_info['has_extract'],
            'extractLastRefreshTime': ds_info['last_refresh_time'],
            'upstreamDatabases': [{
                'name': conn_info['database_name'].iloc[0],
                'connectionType': conn_info['connection_type'].iloc[0]
            }] if not conn_info.empty else []
        })
    
    for _, ds in embedded_ds.iterrows():
        ds_info = data['datasources'][data['datasources']['datasource_id'] == ds['datasource_id']]
        if ds_info.empty:
            continue
        ds_info = ds_info.iloc[0]
        embedded_datasources.append({
            'id': ds_info['datasource_id'],
            'name': ds_info['datasource_name'],
            'hasExtracts': ds_info['has_extract'],
            'extractLastRefreshTime': ds_info['last_refresh_time'],
            'upstreamDatabases': []
        })
    
    return {
        'id': workbook['workbook_id'],
        'name': workbook['workbook_name'],
        'projectName': workbook['project_name'],
        'owner': {
            'name': workbook['owner_name'],
            'email': workbook['owner_email']
        },
        'views': views.to_dict('records'),
        'tags': tags,
        'upstreamDatasources': upstream_datasources,
        'embeddedDatasources': embedded_datasources
    }

def create_workbook_df(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create a DataFrame with workbook information"""
    workbooks = data['workbooks'].copy()
    
    # Add view counts
    view_counts = data['views'].groupby('workbook_id').size().reset_index(name='view_count')
    workbooks = workbooks.merge(view_counts, on='workbook_id', how='left')
    workbooks['view_count'] = workbooks['view_count'].fillna(0).astype(int)
    
    # Add datasource counts
    ds_counts = data['workbook_datasources'].groupby('workbook_id').agg({
        'datasource_id': 'count',
        'is_embedded': lambda x: (x == True).sum() if 'is_embedded' in x else 0
    }).reset_index()
    ds_counts.columns = ['workbook_id', 'total_ds_count', 'embedded_ds_count']
    ds_counts['upstream_ds_count'] = ds_counts['total_ds_count'] - ds_counts['embedded_ds_count']
    
    workbooks = workbooks.merge(ds_counts, on='workbook_id', how='left')
    workbooks['total_ds_count'] = workbooks['total_ds_count'].fillna(0).astype(int)
    workbooks['embedded_ds_count'] = workbooks['embedded_ds_count'].fillna(0).astype(int)
    workbooks['upstream_ds_count'] = workbooks['upstream_ds_count'].fillna(0).astype(int)
    
    # Add tags
    tags = data['workbook_tags'].groupby('workbook_id')['tag_name'].agg(lambda x: ', '.join(x)).reset_index()
    tags.columns = ['workbook_id', 'tags']
    workbooks = workbooks.merge(tags, on='workbook_id', how='left')
    workbooks['tags'] = workbooks['tags'].fillna('')
    
    return workbooks

def create_data_source_df(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create a DataFrame of all data sources with their connections"""
    ds_df = data['datasources'].merge(
        data['workbook_datasources'],
        on='datasource_id',
        how='left'
    )
    
    ds_df = ds_df.merge(
        data['workbooks'][['workbook_id', 'workbook_name', 'project_name']],
        on='workbook_id',
        how='left'
    )
    
    ds_df = ds_df.merge(
        data['connections'][['datasource_id', 'database_name', 'connection_type']],
        on='datasource_id',
        how='left'
    )
    
    if 'is_embedded' in ds_df.columns:
        ds_df['type'] = ds_df['is_embedded'].map({True: 'Embedded', False: 'Upstream'})
    else:
        ds_df['type'] = 'N/A'
    
    ds_df['database_name'] = ds_df['database_name'].fillna('N/A')
    ds_df['connection_type'] = ds_df['connection_type'].fillna('N/A')
    ds_df['type'] = ds_df['type'].fillna('N/A')
    ds_df['last_refresh_time'] = ds_df['last_refresh_time'].fillna('N/A')
    
    ds_df = ds_df.rename(columns={'last_refresh_time': 'last_refresh'})
    
    return ds_df

def display_workbook_details_page(workbook: Dict[str, Any]):
    """Display the detailed workbook view page"""
    if not workbook:
        st.error("Workbook not found.")
        return
    
    # Back button
    if st.button("← Back to Workbooks", key="back_button"):
        st.session_state.current_page = 'main'
        st.session_state.selected_workbook = None
        st.rerun()
    
    # Workbook Header
    st.markdown(f"# 📊 {workbook['name']}")
    
    # Basic Info and Stats Section
    st.markdown('<hr class="hr-thin-white">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        st.markdown("### 📋 Basic Information")
        st.markdown(f"**Project:** {workbook['projectName']}")
        st.markdown(f"**Owner:** {workbook['owner']['name']}")
        st.markdown(f"**Email:** {workbook['owner']['email']}")
    
    with col2:
        st.markdown("### 🏷️ Tags")
        if workbook['tags']:
            for tag in workbook['tags']:
                st.markdown(f"<span style='background-color: #e6e9ef; color: #1e2a44; ; padding: 0.3rem 0.8rem; border-radius: 12px; margin-right: 0.5rem;'>{tag}</span>", unsafe_allow_html=True)
        else:
            st.markdown("No tags available")
    
    with col3:
        st.markdown("### 📊 Quick Stats")
        # Calculate metrics with validation
        view_count = len(workbook['views']) if workbook['views'] else 0
        upstream_ds_count = len(workbook['upstreamDatasources']) if workbook['upstreamDatasources'] else 0
        embedded_ds_count = len(workbook['embeddedDatasources']) if workbook['embeddedDatasources'] else 0
        total_ds_count = upstream_ds_count + embedded_ds_count
        
        # Display metrics in custom cards
        st.markdown(f'<div class="metric-card"><b>Total Views</b><br><span style="font-size: 1.5rem; color: #1e2a44;">{view_count}</span></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-card"><b>Upstream Data Sources</b><br><span style="font-size: 1.5rem; color: #1e2a44;">{upstream_ds_count}</span></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-card"><b>Embedded Data Sources</b><br><span style="font-size: 1.5rem; color: #1e2a44;">{embedded_ds_count}</span></div>', unsafe_allow_html=True)
        #st.markdown(f'<div class="metric-card"><b>Total Data Sources</b><br><span style="font-size: 1.5rem; color: #1e2a44;">{total_ds_count}</span></div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Views Section
    st.markdown("## 📑 Views")
    views_df = pd.DataFrame(workbook['views'])
    if not views_df.empty:
        st.dataframe(
            views_df.rename(columns={'name': 'View Name'}),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No views found in this workbook")
    
    # Data Sources Section
    st.markdown("## 🔌 Data Sources")
    ds_tab1, ds_tab2 = st.tabs(["Upstream Data Sources", "Embedded Data Sources"])
    
    with ds_tab1:
        st.markdown("### Upstream Data Sources")
        if workbook['upstreamDatasources']:
            upstream_data = []
            for ds in workbook['upstreamDatasources']:
                upstream_data.append({
                    'Name': ds['name'],
                    'ID': ds['id'],
                    'Has Extract': ds['hasExtracts'],
                    'Last Refresh': ds.get('extractLastRefreshTime', 'N/A'),
                    'Database': ds['upstreamDatabases'][0]['name'] if ds['upstreamDatabases'] else 'N/A',
                    'Connection Type': ds['upstreamDatabases'][0]['connectionType'] if ds['upstreamDatabases'] else 'N/A'
                })
            st.dataframe(
                pd.DataFrame(upstream_data),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No upstream data sources found")
    
    with ds_tab2:
        st.markdown("### Embedded Data Sources")
        if workbook['embeddedDatasources']:
            embedded_data = []
            for ds in workbook['embeddedDatasources']:
                embedded_data.append({
                    'Name': ds['name'],
                    'ID': ds['id'],
                    'Has Extract': ds['hasExtracts'],
                    'Last Refresh': ds.get('extractLastRefreshTime', 'N/A'),
                    'Database': 'N/A',
                    'Connection Type': 'N/A'
                })
            st.dataframe(
                pd.DataFrame(embedded_data),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No embedded data sources found")
    
    # Data Source Visualization
    st.markdown("## 📊 Data Source Analysis")
    col1, col2 = st.columns(2)
    
    with col1:
        ds_types = {
            'Upstream': len(workbook['upstreamDatasources']),
            'Embedded': len(workbook['embeddedDatasources'])
        }
        fig1 = px.pie(
            values=list(ds_types.values()),
            names=list(ds_types.keys()),
            title='Data Source Type Distribution',
            color_discrete_sequence=['#4e73df', '#ff7f0e']
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        connection_types = {}
        for ds in workbook['upstreamDatasources']:
            conn_type = ds['upstreamDatabases'][0]['connectionType'] if ds['upstreamDatabases'] else 'N/A'
            connection_types[conn_type] = connection_types.get(conn_type, 0) + 1
        
        fig2 = px.bar(
            x=list(connection_types.keys()),
            y=list(connection_types.values()),
            title='Connection Types Distribution',
            labels={'x': 'Connection Type', 'y': 'Count'},
            color_discrete_sequence=['#4e73df']
        )
        st.plotly_chart(fig2, use_container_width=True)

def display_workbook_card(workbook: Dict[str, Any]):
    """Display a clickable workbook card"""
    st.markdown('<hr class="hr-thin-white">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([3, 2, 1])
    
    with col1:
        st.markdown(f"### {workbook['name']}")
        st.markdown(f"**Project:** {workbook['projectName']}")
    
    with col2:
        st.markdown(f"**Owner:** {workbook['owner']['name']}")
        st.markdown(f"**Views:** {len(workbook['views'])}")
    
    with col3:
        if st.button("View Details", key=f"view_{workbook['id']}"):
            st.session_state.current_page = 'workbook_details'
            st.session_state.selected_workbook = workbook
            st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)

def get_workbook_suggestions(workbook_df: pd.DataFrame, search_term: str) -> List[str]:
    """Get list of workbook names that match the search term"""
    if not search_term:
        return []
    search_term = re.escape(search_term)
    matches = workbook_df[
        workbook_df['workbook_name'].str.contains(
            search_term,  
            case=False,  
            na=False
        )
    ]['workbook_name'].unique().tolist()
    return matches[:5]

def create_combined_view(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create a combined DataFrame with unique, meaningful column names"""
    combined = data['workbooks'].merge(
        data['workbook_datasources'],
        on='workbook_id',
        how='left',
        suffixes=('', '_wbd')
    )
    combined = combined.merge(
        data['datasources'],
        on='datasource_id',
        how='left',
        suffixes=('', '_ds')
    )
    combined = combined.merge(
        data['connections'],
        on='datasource_id',
        how='left',
        suffixes=('', '_conn')
    )
    tags = data['workbook_tags'].groupby('workbook_id')['tag_name'].agg(lambda x: ', '.join(x)).reset_index()
    combined = combined.merge(tags, on='workbook_id', how='left')

    combined = combined.rename(columns={
        'workbook_id': 'Workbook ID',
        'workbook_name': 'Workbook Name',
        'project_name': 'Project Name',
        'owner_name': 'Owner Name',
        'owner_email': 'Owner Email',
        'datasource_id': 'Datasource ID',
        'datasource_name': 'Datasource Name',
        'has_extract': 'Has Extract',
        'last_refresh_time': 'Datasource Last Refresh',
        'is_embedded': 'Is Embedded Datasource',
        'database_name': 'Database Name',
        'connection_type': 'Connection Type',
        'tag_name': 'Tags'
    })

    combined = combined.loc[:,~combined.columns.duplicated()]
    return combined

def main():
    # Load data from CSV files
    data = load_data()
    if not data:
        return
    
    workbook_df = create_workbook_df(data)
    datasource_df = create_data_source_df(data)
    
    # Navigation logic
    if st.session_state.current_page == 'workbook_details' and st.session_state.selected_workbook:
        display_workbook_details_page(st.session_state.selected_workbook)
        return
    
    # Main page content
    st.markdown('<h1 style="text-align: center;">📊 Tableau Metadata Explorer</h1>', unsafe_allow_html=True)
    
    # Sidebar filters
    st.sidebar.markdown('<h2 style="color: #ffffff;">Filters</h2>', unsafe_allow_html=True)
    
    workbook_search = st.sidebar.text_input(
        "Search Workbook",
        placeholder="Enter workbook name...",
        help="Filter workbooks by name"
    )
    
    projects = ['All'] + sorted(workbook_df['project_name'].unique().tolist())
    selected_project = st.sidebar.selectbox("Select Project", projects)
    
    owners = ['All'] + sorted(workbook_df['owner_name'].unique().tolist())
    selected_owner = st.sidebar.selectbox("Select Owner", owners)
    
    all_tags = sorted(data['workbook_tags']['tag_name'].unique().tolist())
    tags = ['All'] + all_tags
    selected_tag = st.sidebar.selectbox("Select Tag", tags)
    
    ds_types = ['All', 'Has Upstream', 'Has Embedded', 'Has Both', 'No Data Sources']
    selected_ds_type = st.sidebar.selectbox("Data Source Type", ds_types)
    
    # Apply filters
    filtered_df = workbook_df.copy()
    
    if workbook_search:
        filtered_df = filtered_df[
            filtered_df['workbook_name'].str.contains(
                re.escape(workbook_search),  
                case=False,  
                na=False
            )
        ]
        if len(filtered_df) == 0:
            st.sidebar.warning(f"No workbooks found matching '{workbook_search}'")
    
    if selected_project != 'All':
        filtered_df = filtered_df[filtered_df['project_name'] == selected_project]
    
    if selected_owner != 'All':
        filtered_df = filtered_df[filtered_df['owner_name'] == selected_owner]
    
    if selected_tag != 'All':
        workbook_ids_with_tag = data['workbook_tags'][
            data['workbook_tags']['tag_name'] == selected_tag
        ]['workbook_id'].unique()
        filtered_df = filtered_df[filtered_df['workbook_id'].isin(workbook_ids_with_tag)]
    
    if selected_ds_type != 'All':
        if selected_ds_type == 'Has Upstream':
            filtered_df = filtered_df[filtered_df['upstream_ds_count'] > 0]
        elif selected_ds_type == 'Has Embedded':
            filtered_df = filtered_df[filtered_df['embedded_ds_count'] > 0]
        elif selected_ds_type == 'Has Both':
            filtered_df = filtered_df[
                (filtered_df['upstream_ds_count'] > 0) &  
                (filtered_df['embedded_ds_count'] > 0)
            ]
        elif selected_ds_type == 'No Data Sources':
            filtered_df = filtered_df[
                (filtered_df['upstream_ds_count'] == 0) &  
                (filtered_df['embedded_ds_count'] == 0)
            ]
    
    # Display filter summary
    if any([workbook_search, selected_project != 'All', selected_owner != 'All',  
            selected_tag != 'All', selected_ds_type != 'All']):
        st.sidebar.markdown("---")
        st.sidebar.markdown("### Active Filters")
        filter_col1, filter_col2 = st.sidebar.columns([3, 1])
        
        with filter_col1:
            if workbook_search:
                st.markdown(f"📚 **Workbook:** {workbook_search}")
            if selected_project != 'All':
                st.markdown(f"📁 **Project:** {selected_project}")
            if selected_owner != 'All':
                st.markdown(f"👤 **Owner:** {selected_owner}")
            if selected_tag != 'All':
                st.markdown(f"🏷️ **Tag:** {selected_tag}")
            if selected_ds_type != 'All':
                st.markdown(f"🔌 **Data Source Type:** {selected_ds_type}")
        
        with filter_col2:
            if st.button("🗑️ Clear", use_container_width=True):
                st.rerun()
    
    # Main page metrics in cards
    st.markdown("## 📊 Dashboard Metrics")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f'<div class="metric-card"><b>Total Workbooks</b><br><span style="font-size: 1.5rem; color: #1e2a44;">{len(filtered_df)}</span></div>', unsafe_allow_html=True)
    with col2:
        st.markdown(f'<div class="metric-card"><b>Total Views</b><br><span style="font-size: 1.5rem; color: #1e2a44;">{filtered_df["view_count"].sum()}</span></div>', unsafe_allow_html=True)
    with col3:
        st.markdown(f'<div class="metric-card"><b>Upstream Data Sources</b><br><span style="font-size: 1.5rem; color: #1e2a44;">{filtered_df["upstream_ds_count"].sum()}</span></div>', unsafe_allow_html=True)
    with col4:
        st.markdown(f'<div class="metric-card"><b>Embedded Data Sources</b><br><span style="font-size: 1.5rem; color: #1e2a44;">{filtered_df["embedded_ds_count"].sum()}</span></div>', unsafe_allow_html=True)
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📚 Workbooks", "🔌 Data Sources", "📈 Analytics", "🗂️ Combined View"])
    
    with tab1:
        st.subheader("Workbook Details")
        for _, row in filtered_df.iterrows():
            workbook_id = row['workbook_id']
            workbook = get_workbook_details(data, workbook_id)
            if workbook:
                display_workbook_card(workbook)
        
        st.markdown("### 📈 Views Distribution")
        fig = px.bar(
            filtered_df,
            x='workbook_name',
            y='view_count',
            title='Views per Workbook',
            labels={'workbook_name': 'Workbook', 'view_count': 'Number of Views'},
            color='project_name',
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Data Source Analysis")
        filtered_ds_df = datasource_df[
            datasource_df['workbook_name'].isin(filtered_df['workbook_name'])
        ]
        
        fig1 = px.pie(
            filtered_ds_df,
            names='type',
            title='Data Source Type Distribution',
            color='type',
            color_discrete_map={'Upstream': '#4e73df', 'Embedded': '#ff7f0e'}
        )
        st.plotly_chart(fig1, use_container_width=True)
        
        fig2 = px.bar(
            filtered_ds_df,
            x='connection_type',
            title='Connection Types',
            color='type',
            labels={'connection_type': 'Connection Type', 'count': 'Count'},
            color_discrete_map={'Upstream': '#4e73df', 'Embedded': '#ff7f0e'}
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        st.dataframe(
            filtered_ds_df[[
                'datasource_name', 'type', 'workbook_name', 'project_name',
                'has_extract', 'last_refresh', 'connection_type'
            ]].rename(columns={
                'datasource_name': 'Data Source',
                'type': 'Type',
                'workbook_name': 'Workbook',
                'project_name': 'Project',
                'has_extract': 'Has Extract',
                'last_refresh': 'Last Refresh',
                'connection_type': 'Connection Type'
            }),
            use_container_width=True
        )
    
    with tab3:
        st.subheader("Analytics Dashboard")
        fig3 = px.pie(
            filtered_df,
            names='project_name',
            title='Workbook Distribution by Project',
            color='project_name',
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        st.plotly_chart(fig3, use_container_width=True)
        
        fig4 = px.bar(
            filtered_df.groupby('owner_name').size().reset_index(name='count'),
            x='owner_name',
            y='count',
            title='Workbooks per Owner',
            labels={'owner_name': 'Owner', 'count': 'Number of Workbooks'},
            color='count',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig4, use_container_width=True)
        
        tag_counts = {}
        for tags in filtered_df['tags'].str.split(', '):
            for tag in tags:
                if tag:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        tag_df = pd.DataFrame(list(tag_counts.items()), columns=['tag', 'count'])
        fig5 = px.bar(
            tag_df.sort_values('count', ascending=True),
            y='tag',
            x='count',
            title='Tag Distribution',
            orientation='h',
            color='count',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig5, use_container_width=True)
    
    with tab4:
        st.subheader("🗂️ Combined Metadata View")
        combined_df = create_combined_view(data)
        st.dataframe(combined_df, use_container_width=True)

if __name__ == "__main__":
    main()