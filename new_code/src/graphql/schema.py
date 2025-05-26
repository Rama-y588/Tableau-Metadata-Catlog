from typing import List, Optional
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

class ConnectionType(Enum):
    """Types of database connections"""
    SQL_SERVER = "SQL Server"
    POSTGRESQL = "PostgreSQL"
    MYSQL = "MySQL"
    ORACLE = "Oracle"
    SNOWFLAKE = "Snowflake"
    BIGQUERY = "BigQuery"
    EXCEL = "Excel"
    CSV = "CSV"
    OTHER = "Other"

@dataclass
class Connection:
    """Database connection information"""
    connection_id: str
    datasource_id: str
    connection_type: ConnectionType
    database_name: str
    server_name: Optional[str]
    port: Optional[int]
    username: Optional[str]
    is_embedded: bool
    created_at: datetime
    updated_at: datetime

@dataclass
class DataSource:
    """Tableau data source information"""
    datasource_id: str
    name: str
    type: str
    is_embedded: bool
    connections: List[Connection]
    created_at: datetime
    updated_at: datetime

@dataclass
class View:
    """Tableau view (worksheet or dashboard) information"""
    view_id: str
    workbook_id: str
    name: str
    type: str  # 'worksheet' or 'dashboard'
    created_at: datetime
    updated_at: datetime
    last_accessed_at: Optional[datetime]

@dataclass
class Workbook:
    """Tableau workbook information"""
    workbook_id: str
    name: str
    project_name: str
    owner_name: str
    description: Optional[str]
    views: List[View]
    datasources: List[DataSource]
    tags: List[str]
    created_at: datetime
    updated_at: datetime
    last_accessed_at: Optional[datetime]
    view_count: int
    upstream_ds_count: int
    embedded_ds_count: int
    total_ds_count: int

# GraphQL Schema Definition
schema = """
type Connection {
    connectionId: ID!
    datasourceId: ID!
    connectionType: ConnectionType!
    databaseName: String!
    serverName: String
    port: Int
    username: String
    isEmbedded: Boolean!
    createdAt: DateTime!
    updatedAt: DateTime!
}

type DataSource {
    datasourceId: ID!
    name: String!
    type: String!
    isEmbedded: Boolean!
    connections: [Connection!]!
    createdAt: DateTime!
    updatedAt: DateTime!
}

type View {
    viewId: ID!
    workbookId: ID!
    name: String!
    type: String!
    createdAt: DateTime!
    updatedAt: DateTime!
    lastAccessedAt: DateTime
}

type Workbook {
    workbookId: ID!
    name: String!
    projectName: String!
    ownerName: String!
    description: String
    views: [View!]!
    datasources: [DataSource!]!
    tags: [String!]!
    createdAt: DateTime!
    updatedAt: DateTime!
    lastAccessedAt: DateTime
    viewCount: Int!
    upstreamDsCount: Int!
    embeddedDsCount: Int!
    totalDsCount: Int!
}

enum ConnectionType {
    SQL_SERVER
    POSTGRESQL
    MYSQL
    ORACLE
    SNOWFLAKE
    BIGQUERY
    EXCEL
    CSV
    OTHER
}

type Query {
    # Workbook queries
    workbook(workbookId: ID!): Workbook
    workbooks(
        searchTerm: String
        project: String
        owner: String
        tag: String
        datasourceType: String
    ): [Workbook!]!
    
    # Data source queries
    datasource(datasourceId: ID!): DataSource
    datasources(
        workbookId: ID
        isEmbedded: Boolean
        connectionType: ConnectionType
    ): [DataSource!]!
    
    # View queries
    view(viewId: ID!): View
    views(workbookId: ID!): [View!]!
    
    # Metadata queries
    projects: [String!]!
    owners: [String!]!
    tags: [String!]!
    connectionTypes: [ConnectionType!]!
}

scalar DateTime
""" 