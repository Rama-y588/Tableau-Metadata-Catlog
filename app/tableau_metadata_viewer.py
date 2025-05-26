import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# Set page config
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

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
    }
    .css-1d391kg {
        padding: 1rem;
    }
    .workbook-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        cursor: pointer;
    }
    .workbook-card:hover {
        background-color: #e6e9ef;
    }
    .back-button {
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)

def load_data() -> Dict[str, pd.DataFrame]:
    """Load all CSV files into DataFrames"""
    data_dir = Path(__file__).parent.parent / 'data'
    
    # Load workbook_datasources with boolean conversion for is_embedded
    workbook_datasources = pd.read_csv(data_dir / 'workbook_datasources.csv')
    workbook_datasources['is_embedded'] = workbook_datasources['is_embedded'].map({'true': True, 'false': False})
    
    return {
        'workbooks': pd.read_csv(data_dir / 'workbooks.csv'),
        'views': pd.read_csv(data_dir / 'views.csv'),
        'datasources': pd.read_csv(data_dir / 'datasources.csv'),
        'connections': pd.read_csv(data_dir / 'connections.csv'),
        'workbook_tags': pd.read_csv(data_dir / 'workbook_tags.csv'),
        'workbook_datasources': workbook_datasources
    }

def get_workbook_details(data: Dict[str, pd.DataFrame], workbook_id: str) -> Dict[str, Any]:
    """Get detailed information for a specific workbook"""
    workbook = data['workbooks'][data['workbooks']['workbook_id'] == workbook_id].iloc[0]
    
    # Get views
    views = data['views'][data['views']['workbook_id'] == workbook_id]
    
    # Get tags
    tags = data['workbook_tags'][data['workbook_tags']['workbook_id'] == workbook_id]['tag_name'].tolist()
    
    # Get datasources
    workbook_ds = data['workbook_datasources'][data['workbook_datasources']['workbook_id'] == workbook_id]
    upstream_ds = workbook_ds[~workbook_ds['is_embedded']]
    embedded_ds = workbook_ds[workbook_ds['is_embedded']]
    
    upstream_datasources = []
    for _, ds in upstream_ds.iterrows():
        ds_info = data['datasources'][data['datasources']['datasource_id'] == ds['datasource_id']].iloc[0]
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
    
    embedded_datasources = []
    for _, ds in embedded_ds.iterrows():
        ds_info = data['datasources'][data['datasources']['datasource_id'] == ds['datasource_id']].iloc[0]
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
        'is_embedded': lambda x: (x == True).sum()
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
    # Merge datasources with workbook_datasources
    ds_df = data['datasources'].merge(
        data['workbook_datasources'],
        on='datasource_id',
        how='left'
    )
    
    # Merge with workbooks to get workbook and project names
    ds_df = ds_df.merge(
        data['workbooks'][['workbook_id', 'workbook_name', 'project_name']],
        on='workbook_id',
        how='left'
    )
    
    # Merge with connections to get database info
    ds_df = ds_df.merge(
        data['connections'][['datasource_id', 'database_name', 'connection_type']],
        on='datasource_id',
        how='left'
    )
    
    # Clean up column names and fill NAs
    ds_df['database_name'] = ds_df['database_name'].fillna('N/A')
    ds_df['connection_type'] = ds_df['connection_type'].fillna('N/A')
    
    return ds_df

def display_workbook_details_page(workbook: Dict[str, Any]):
    """Display the detailed workbook view page"""
    # Back button
    if st.button("← Back to Workbooks", key="back_button"):
        st.session_state.current_page = 'main'
        st.session_state.selected_workbook = None
        st.rerun()
    
    # Workbook Header
    st.markdown(f"# 📊 {workbook['name']}")
    
    # Basic Info Card
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.markdown("### 📋 Basic Information")
        st.markdown(f"**Project:** {workbook['projectName']}")
        st.markdown(f"**Owner:** {workbook['owner']['name']}")
        st.markdown(f"**Email:** {workbook['owner']['email']}")
    
    with col2:
        st.markdown("### 🏷️ Tags")
        for tag in workbook['tags']:
            st.markdown(f"- {tag}")
    
    with col3:
        st.markdown("### 📊 Quick Stats")
        # Calculate metrics
        view_count = len(workbook['views'])
        upstream_ds_count = len(workbook['upstreamDatasources'])
        embedded_ds_count = len(workbook['embeddedDatasources'])
        total_ds_count = upstream_ds_count + embedded_ds_count
        
        # Display metrics with proper formatting
        st.metric(
            label="Total Views",
            value=view_count,
            delta=None
        )
        st.metric(
            label="Upstream Data Sources",
            value=upstream_ds_count,
            delta=None
        )
        st.metric(
            label="Embedded Data Sources",
            value=embedded_ds_count,
            delta=None
        )
        st.metric(
            label="Total Data Sources",
            value=total_ds_count,
            delta=None
        )
    
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
    
    # Create tabs for different data source types
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
                    'Database': ds['upstreamDatabases'][0]['name'] if ds['upstreamDatabases'] else 'N/A',
                    'Connection Type': ds['upstreamDatabases'][0]['connectionType'] if ds['upstreamDatabases'] else 'N/A'
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
        # Data Source Type Distribution
        ds_types = {
            'Upstream': len(workbook['upstreamDatasources']),
            'Embedded': len(workbook['embeddedDatasources'])
        }
        fig1 = px.pie(
            values=list(ds_types.values()),
            names=list(ds_types.keys()),
            title='Data Source Type Distribution'
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Connection Types Distribution
        connection_types = {}
        for ds in workbook['upstreamDatasources'] + workbook['embeddedDatasources']:
            conn_type = ds['upstreamDatabases'][0]['connectionType'] if ds['upstreamDatabases'] else 'N/A'
            connection_types[conn_type] = connection_types.get(conn_type, 0) + 1
        
        fig2 = px.bar(
            x=list(connection_types.keys()),
            y=list(connection_types.values()),
            title='Connection Types Distribution',
            labels={'x': 'Connection Type', 'y': 'Count'}
        )
        st.plotly_chart(fig2, use_container_width=True)

def display_workbook_card(workbook: Dict[str, Any]):
    """Display a clickable workbook card"""
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

def get_workbook_suggestions(workbook_df: pd.DataFrame, search_term: str) -> List[str]:
    """Get list of workbook names that match the search term"""
    if not search_term:
        return []
    # Get all workbook names that contain the search term (case-insensitive)
    matches = workbook_df[
        workbook_df['workbook_name'].str.contains(
            search_term, 
            case=False, 
            na=False
        )
    ]['workbook_name'].unique().tolist()
    return matches[:5]  # Return top 5 matches

def main():
    # Load data from CSV files
    data = load_data()
    workbook_df = create_workbook_df(data)
    datasource_df = create_data_source_df(data)
    
    # Navigation logic
    if st.session_state.current_page == 'workbook_details' and st.session_state.selected_workbook:
        display_workbook_details_page(st.session_state.selected_workbook)
        return
    
    # Main page content
    st.title("📊 Tableau Metadata Explorer")
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Workbook name filter
    workbook_search = st.sidebar.text_input(
        "Search Workbook",
        placeholder="Enter workbook name...",
        help="Filter workbooks by name"
    )
    
    # Project filter
    projects = ['All'] + sorted(workbook_df['project_name'].unique().tolist())
    selected_project = st.sidebar.selectbox("Select Project", projects)
    
    # Owner filter
    owners = ['All'] + sorted(workbook_df['owner_name'].unique().tolist())
    selected_owner = st.sidebar.selectbox("Select Owner", owners)
    
    # Tag filter
    all_tags = sorted(data['workbook_tags']['tag_name'].unique().tolist())
    tags = ['All'] + all_tags
    selected_tag = st.sidebar.selectbox("Select Tag", tags)
    
    # Data Source Type filter
    ds_types = ['All', 'Has Upstream', 'Has Embedded', 'Has Both', 'No Data Sources']
    selected_ds_type = st.sidebar.selectbox("Data Source Type", ds_types)
    
    # Apply filters
    filtered_df = workbook_df.copy()
    
    # Apply workbook name filter (case-insensitive search)
    if workbook_search:
        filtered_df = filtered_df[
            filtered_df['workbook_name'].str.contains(
                workbook_search, 
                case=False, 
                na=False
            )
        ]
        if len(filtered_df) == 0:
            st.sidebar.warning(f"No workbooks found matching '{workbook_search}'")
    
    # Apply project filter
    if selected_project != 'All':
        filtered_df = filtered_df[filtered_df['project_name'] == selected_project]
    
    # Apply owner filter
    if selected_owner != 'All':
        filtered_df = filtered_df[filtered_df['owner_name'] == selected_owner]
    
    # Apply tag filter
    if selected_tag != 'All':
        workbook_ids_with_tag = data['workbook_tags'][
            data['workbook_tags']['tag_name'] == selected_tag
        ]['workbook_id'].unique()
        filtered_df = filtered_df[filtered_df['workbook_id'].isin(workbook_ids_with_tag)]
    
    # Apply data source type filter
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
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Workbooks", len(filtered_df))
    with col2:
        st.metric("Total Views", filtered_df['view_count'].sum())
    with col3:
        st.metric("Upstream Data Sources", filtered_df['upstream_ds_count'].sum())
    with col4:
        st.metric("Embedded Data Sources", filtered_df['embedded_ds_count'].sum())
    
    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["📚 Workbooks", "🔌 Data Sources", "📈 Analytics"])
    
    with tab1:
        st.subheader("Workbook Details")
        
        # Display workbook cards
        for _, row in filtered_df.iterrows():
            workbook_id = row['workbook_id']
            workbook = get_workbook_details(data, workbook_id)
            if workbook:
                st.markdown('<div class="workbook-card">', unsafe_allow_html=True)
                display_workbook_card(workbook)
                st.markdown('</div>', unsafe_allow_html=True)
        
        # Views per workbook chart
        st.markdown("### 📈 Views Distribution")
        fig = px.bar(
            filtered_df,
            x='workbook_name',
            y='view_count',
            title='Views per Workbook',
            labels={'workbook_name': 'Workbook', 'view_count': 'Number of Views'},
            color='project_name'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Data Source Analysis")
        
        # Filter data sources based on workbook selection
        filtered_ds_df = datasource_df[
            datasource_df['workbook_name'].isin(filtered_df['workbook_name'])
        ]
        
        # Data source type distribution
        fig1 = px.pie(
            filtered_ds_df,
            names='type',
            title='Data Source Type Distribution',
            color='type',
            color_discrete_map={'Upstream': '#1f77b4', 'Embedded': '#ff7f0e'}
        )
        st.plotly_chart(fig1, use_container_width=True)
        
        # Connection types
        fig2 = px.bar(
            filtered_ds_df,
            x='connection_type',
            title='Connection Types',
            color='type',
            labels={'connection_type': 'Connection Type', 'count': 'Count'},
            color_discrete_map={'Upstream': '#1f77b4', 'Embedded': '#ff7f0e'}
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        # Data source table
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
        
        # Project distribution
        fig3 = px.pie(
            filtered_df,
            names='project_name',
            title='Workbook Distribution by Project',
            color='project_name'
        )
        st.plotly_chart(fig3, use_container_width=True)
        
        # Owner workload
        fig4 = px.bar(
            filtered_df.groupby('owner_name').size().reset_index(name='count'),
            x='owner_name',
            y='count',
            title='Workbooks per Owner',
            labels={'owner_name': 'Owner', 'count': 'Number of Workbooks'},
            color='count'
        )
        st.plotly_chart(fig4, use_container_width=True)
        
        # Tag cloud
        tag_counts = {}
        for tags in filtered_df['tags'].str.split(', '):
            for tag in tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        tag_df = pd.DataFrame(list(tag_counts.items()), columns=['tag', 'count'])
        fig5 = px.bar(
            tag_df.sort_values('count', ascending=True),
            y='tag',
            x='count',
            title='Tag Distribution',
            orientation='h',
            color='count'
        )
        st.plotly_chart(fig5, use_container_width=True)

if __name__ == "__main__":
    main() 