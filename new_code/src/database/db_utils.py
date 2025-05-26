import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

class TableauDatabase:
    """Utility class for Tableau metadata database operations"""
    
    def __init__(self, db_path: Optional[Path] = None):
        """Initialize database connection"""
        if db_path is None:
            db_path = Path(__file__).parent / 'tableau_metadata.db'
        self.db_path = db_path
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a database connection"""
        return sqlite3.connect(self.db_path)
    
    def get_workbook(self, workbook_id: str) -> Optional[Dict[str, Any]]:
        """Get a workbook with all its related data"""
        with self.get_connection() as conn:
            # Get workbook data
            workbook_df = pd.read_sql_query(
                'SELECT * FROM workbooks WHERE workbook_id = ?',
                conn,
                params=(workbook_id,)
            )
            
            if workbook_df.empty:
                return None
            
            workbook = workbook_df.iloc[0].to_dict()
            
            # Get views
            views_df = pd.read_sql_query(
                'SELECT * FROM views WHERE workbook_id = ?',
                conn,
                params=(workbook_id,)
            )
            workbook['views'] = views_df.to_dict('records')
            
            # Get datasources
            datasources_df = pd.read_sql_query('''
                SELECT d.*, wd.is_embedded, c.*
                FROM datasources d
                JOIN workbook_datasources wd ON d.datasource_id = wd.datasource_id
                LEFT JOIN connections c ON d.datasource_id = c.datasource_id
                WHERE wd.workbook_id = ?
            ''', conn, params=(workbook_id,))
            
            workbook['datasources'] = datasources_df.to_dict('records')
            
            # Get tags
            tags_df = pd.read_sql_query('''
                SELECT t.tag_name
                FROM tags t
                JOIN workbook_tags wt ON t.tag_id = wt.tag_id
                WHERE wt.workbook_id = ?
            ''', conn, params=(workbook_id,))
            
            workbook['tags'] = tags_df['tag_name'].tolist()
            
            return workbook
    
    def get_workbooks_summary(self) -> pd.DataFrame:
        """Get a summary of all workbooks with their metrics"""
        with self.get_connection() as conn:
            return pd.read_sql_query('''
                WITH workbook_metrics AS (
                    SELECT 
                        w.workbook_id,
                        COUNT(DISTINCT v.view_id) as view_count,
                        COUNT(DISTINCT CASE WHEN wd.is_embedded = 0 THEN wd.datasource_id END) as upstream_ds_count,
                        COUNT(DISTINCT CASE WHEN wd.is_embedded = 1 THEN wd.datasource_id END) as embedded_ds_count,
                        GROUP_CONCAT(DISTINCT t.tag_name) as tags
                    FROM workbooks w
                    LEFT JOIN views v ON w.workbook_id = v.workbook_id
                    LEFT JOIN workbook_datasources wd ON w.workbook_id = wd.workbook_id
                    LEFT JOIN workbook_tags wt ON w.workbook_id = wt.workbook_id
                    LEFT JOIN tags t ON wt.tag_id = t.tag_id
                    GROUP BY w.workbook_id
                )
                SELECT 
                    w.*,
                    COALESCE(m.view_count, 0) as view_count,
                    COALESCE(m.upstream_ds_count, 0) as upstream_ds_count,
                    COALESCE(m.embedded_ds_count, 0) as embedded_ds_count,
                    COALESCE(m.upstream_ds_count, 0) + COALESCE(m.embedded_ds_count, 0) as total_ds_count,
                    m.tags
                FROM workbooks w
                LEFT JOIN workbook_metrics m ON w.workbook_id = m.workbook_id
            ''', conn)
    
    def get_datasources_summary(self) -> pd.DataFrame:
        """Get a summary of all data sources with their connections"""
        with self.get_connection() as conn:
            return pd.read_sql_query('''
                SELECT 
                    d.*,
                    w.workbook_name,
                    w.project_name,
                    wd.is_embedded,
                    c.database_name,
                    c.connection_type
                FROM datasources d
                JOIN workbook_datasources wd ON d.datasource_id = wd.datasource_id
                JOIN workbooks w ON wd.workbook_id = w.workbook_id
                LEFT JOIN connections c ON d.datasource_id = c.datasource_id
            ''', conn)
    
    def search_workbooks(self, 
                        search_term: Optional[str] = None,
                        project: Optional[str] = None,
                        owner: Optional[str] = None,
                        tag: Optional[str] = None,
                        datasource_type: Optional[str] = None) -> pd.DataFrame:
        """Search workbooks with various filters"""
        query = '''
            WITH filtered_workbooks AS (
                SELECT DISTINCT w.workbook_id
                FROM workbooks w
                LEFT JOIN workbook_datasources wd ON w.workbook_id = wd.workbook_id
                LEFT JOIN workbook_tags wt ON w.workbook_id = wt.workbook_id
                LEFT JOIN tags t ON wt.tag_id = t.tag_id
                WHERE 1=1
        '''
        params = []
        
        if search_term:
            query += ' AND w.workbook_name LIKE ?'
            params.append(f'%{search_term}%')
        
        if project:
            query += ' AND w.project_name = ?'
            params.append(project)
        
        if owner:
            query += ' AND w.owner_name = ?'
            params.append(owner)
        
        if tag:
            query += ' AND t.tag_name = ?'
            params.append(tag)
        
        if datasource_type:
            if datasource_type == 'Has Upstream':
                query += ' AND EXISTS (SELECT 1 FROM workbook_datasources wd2 WHERE wd2.workbook_id = w.workbook_id AND wd2.is_embedded = 0)'
            elif datasource_type == 'Has Embedded':
                query += ' AND EXISTS (SELECT 1 FROM workbook_datasources wd2 WHERE wd2.workbook_id = w.workbook_id AND wd2.is_embedded = 1)'
            elif datasource_type == 'Has Both':
                query += ''' AND EXISTS (SELECT 1 FROM workbook_datasources wd2 WHERE wd2.workbook_id = w.workbook_id AND wd2.is_embedded = 0)
                            AND EXISTS (SELECT 1 FROM workbook_datasources wd2 WHERE wd2.workbook_id = w.workbook_id AND wd2.is_embedded = 1)'''
            elif datasource_type == 'No Data Sources':
                query += ' AND NOT EXISTS (SELECT 1 FROM workbook_datasources wd2 WHERE wd2.workbook_id = w.workbook_id)'
        
        query += '''
            )
            SELECT 
                w.*,
                COUNT(DISTINCT v.view_id) as view_count,
                COUNT(DISTINCT CASE WHEN wd.is_embedded = 0 THEN wd.datasource_id END) as upstream_ds_count,
                COUNT(DISTINCT CASE WHEN wd.is_embedded = 1 THEN wd.datasource_id END) as embedded_ds_count,
                GROUP_CONCAT(DISTINCT t.tag_name) as tags
            FROM filtered_workbooks fw
            JOIN workbooks w ON fw.workbook_id = w.workbook_id
            LEFT JOIN views v ON w.workbook_id = v.workbook_id
            LEFT JOIN workbook_datasources wd ON w.workbook_id = wd.workbook_id
            LEFT JOIN workbook_tags wt ON w.workbook_id = wt.workbook_id
            LEFT JOIN tags t ON wt.tag_id = t.tag_id
            GROUP BY w.workbook_id
        '''
        
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def get_unique_values(self, column: str, table: str) -> List[str]:
        """Get unique values for a column in a table"""
        with self.get_connection() as conn:
            df = pd.read_sql_query(f'SELECT DISTINCT {column} FROM {table}', conn)
            return df[column].tolist()
    
    def get_all_tags(self) -> List[str]:
        """Get all unique tags"""
        with self.get_connection() as conn:
            df = pd.read_sql_query('SELECT DISTINCT tag_name FROM tags', conn)
            return df['tag_name'].tolist()
    
    def get_all_projects(self) -> List[str]:
        """Get all unique project names"""
        return self.get_unique_values('project_name', 'workbooks')
    
    def get_all_owners(self) -> List[str]:
        """Get all unique owner names"""
        return self.get_unique_values('owner_name', 'workbooks') 