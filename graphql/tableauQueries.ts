// GraphQL Queries for Tableau API

// Basic workbook query with essential fields
export const GET_WORKBOOKS_BASIC = `
  query GetWorkbooksBasic {
    workbooks {
      id
      name
      description
      createdAt
      updatedAt
      owner {
        id
        name
        email
      }
      project {
        id
        name
      }
    }
  }
`;

// Detailed workbook query with data sources and views
export const GET_WORKBOOKS_DETAILED = `
  query GetWorkbooksDetailed {
    workbooks {
      id
      name
      description
      createdAt
      updatedAt
      owner {
        id
        name
        email
      }
      project {
        id
        name
      }
      dataSources {
        id
        name
        type
        connectionType
        isEmbedded
        hasExtracts
        extractLastRefreshTime
        connectionDetails
      }
      views {
        id
        name
        workbookId
        createdAt
        updatedAt
        dataColumns {
          id
          name
          dataType
          role
        }
        filters {
          id
          name
          type
          values
        }
      }
    }
  }
`;

// Query for workbooks with specific data source types
export const GET_WORKBOOKS_BY_DATA_SOURCE_TYPE = `
  query GetWorkbooksByDataSourceType($dataSourceType: DataSourceType!) {
    workbooks(where: { dataSourceType: $dataSourceType }) {
      id
      name
      description
      dataSources {
        id
        name
        type
        connectionType
        isEmbedded
        hasExtracts
        extractLastRefreshTime
        connectionDetails
      }
    }
  }
`;

// Query for workbooks with no data sources
export const GET_WORKBOOKS_WITHOUT_DATA_SOURCES = `
  query GetWorkbooksWithoutDataSources {
    workbooks(where: { hasDataSources: false }) {
      id
      name
      description
      views {
        id
        name
        dataColumns {
          id
          name
          dataType
          role
        }
      }
    }
  }
`;

// Query for workbooks with multiple data sources
export const GET_WORKBOOKS_WITH_MULTIPLE_DATA_SOURCES = `
  query GetWorkbooksWithMultipleDataSources {
    workbooks(where: { dataSourceCount: { gt: 1 } }) {
      id
      name
      description
      dataSources {
        id
        name
        type
        connectionType
        isEmbedded
        hasExtracts
        connectionDetails
      }
      views {
        id
        name
        dataColumns {
          id
          name
          dataType
          role
        }
      }
    }
  }
`;

// Query for specific workbook with all details
export const GET_WORKBOOK_BY_ID = `
  query GetWorkbookById($workbookId: ID!) {
    workbook(id: $workbookId) {
      id
      name
      description
      createdAt
      updatedAt
      owner {
        id
        name
        email
      }
      project {
        id
        name
      }
      dataSources {
        id
        name
        type
        connectionType
        isEmbedded
        hasExtracts
        extractLastRefreshTime
        connectionDetails
      }
      views {
        id
        name
        workbookId
        createdAt
        updatedAt
        dataColumns {
          id
          name
          dataType
          role
        }
        filters {
          id
          name
          type
          values
        }
        data {
          rows
        }
      }
    }
  }
`;

// Query for metadata and statistics
export const GET_TABLEAU_METADATA = `
  query GetTableauMetadata {
    tableauMetadata {
      totalWorkbookCount
      dataSourceTypes {
        published
        embedded
        flatFiles
        noDataSources
      }
      lastUpdated
      apiVersion
    }
  }
`;

// Query for extract refresh schedules
export const GET_EXTRACT_REFRESH_SCHEDULES = `
  query GetExtractRefreshSchedules {
    extractRefreshSchedules {
      id
      name
      description
      status
      scheduleType
      scheduleDetails {
        frequency
        startTime
        timeZone
        daysOfWeek
        daysOfMonth
        endTime
        nextRunTime
        timeZone
        lastRunTime
        lastRunStatus
        lastRunError
      }
      workbooks {
        id
        name
        dataSources {
          id
          name
          type
          hasExtracts
          extractLastRefreshTime
          extractRefreshStatus
        }
      }
      createdBy {
        id
        name
        email
      }
      createdAt
      updatedAt
    }
  }
`;

// Query for specific workbook's extract refresh schedules
export const GET_WORKBOOK_EXTRACT_SCHEDULES = `
  query GetWorkbookExtractSchedules($workbookId: ID!) {
    workbook(id: $workbookId) {
      id
      name
      extractSchedules {
        id
        name
        status
        scheduleType
        scheduleDetails {
          frequency
          startTime
          timeZone
          nextRunTime
          lastRunTime
          lastRunStatus
        }
        dataSources {
          id
          name
          extractLastRefreshTime
          extractRefreshStatus
          extractSize
          extractLastIncrementalRefresh
        }
      }
    }
  }
`;

// Query for data source extract details
export const GET_DATA_SOURCE_EXTRACT_DETAILS = `
  query GetDataSourceExtractDetails($dataSourceId: ID!) {
    dataSource(id: $dataSourceId) {
      id
      name
      type
      hasExtracts
      extractDetails {
        lastRefreshTime
        lastRefreshStatus
        lastRefreshError
        nextScheduledRefresh
        refreshSchedule {
          id
          name
          status
          scheduleType
          scheduleDetails {
            frequency
            startTime
            timeZone
            nextRunTime
          }
        }
        extractSize
        isIncremental
        lastIncrementalRefresh
        incrementalRefreshField
        incrementalRefreshValue
      }
    }
  }
`;

// Query for embedded data sources
export const GET_EMBEDDED_DATA_SOURCES = `
  query GetEmbeddedDataSources($workbookId: ID!) {
    workbook(id: $workbookId) {
      id
      name
      embeddedDataSources {
        id
        name
        type
        connectionType
        isEmbedded
        hasExtracts
        extractLastRefreshTime
        connectionDetails
        embeddedIn {
          id
          name
          type
        }
        upstreamDependencies {
          id
          name
          type
          connectionType
        }
        downstreamDependencies {
          id
          name
          type
          connectionType
        }
      }
    }
  }
`;

// Query for upstream data sources
export const GET_UPSTREAM_DATA_SOURCES = `
  query GetUpstreamDataSources($dataSourceId: ID!) {
    dataSource(id: $dataSourceId) {
      id
      name
      type
      connectionType
      isEmbedded
      hasExtracts
      upstreamDependencies {
        id
        name
        type
        connectionType
        isEmbedded
        hasExtracts
        extractLastRefreshTime
        connectionDetails
        publishedBy {
          id
          name
          email
        }
        publishedAt
        lastUpdatedAt
        usedBy {
          id
          name
          type
          project {
            id
            name
          }
        }
      }
      downstreamDependencies {
        id
        name
        type
        connectionType
        isEmbedded
        hasExtracts
        usedBy {
          id
          name
          type
          project {
            id
            name
          }
        }
      }
    }
  }
`;

// Query for data source lineage
export const GET_DATA_SOURCE_LINEAGE = `
  query GetDataSourceLineage($dataSourceId: ID!) {
    dataSource(id: $dataSourceId) {
      id
      name
      type
      connectionType
      isEmbedded
      lineage {
        upstream {
          id
          name
          type
          connectionType
          isEmbedded
          hasExtracts
          level
          path
        }
        downstream {
          id
          name
          type
          connectionType
          isEmbedded
          hasExtracts
          level
          path
        }
      }
    }
  }
`;

// Example usage with variables:
/*
const variables = {
  workbookId: "wb-001",
  dataSourceType: "PUBLISHED"
};

// Using the query with a GraphQL client
const result = await client.query({
  query: GET_WORKBOOK_BY_ID,
  variables: variables
});
*/

// Type definitions for the queries
export interface TableauQueryVariables {
  workbookId?: string;
  dataSourceId?: string;
  dataSourceType?: 'PUBLISHED' | 'EMBEDDED' | 'FLAT_FILE';
}

// Add new interfaces for extract schedules
export interface ExtractScheduleDetails {
  frequency: 'HOURLY' | 'DAILY' | 'WEEKLY' | 'MONTHLY';
  startTime: string;
  timeZone: string;
  daysOfWeek?: string[];
  daysOfMonth?: number[];
  endTime?: string;
  nextRunTime: string;
  lastRunTime?: string;
  lastRunStatus?: 'SUCCESS' | 'FAILED' | 'IN_PROGRESS';
  lastRunError?: string;
}

export interface ExtractSchedule {
  id: string;
  name: string;
  description?: string;
  status: 'ACTIVE' | 'PAUSED' | 'DISABLED';
  scheduleType: 'EXTRACT' | 'SUBSCRIPTION';
  scheduleDetails: ExtractScheduleDetails;
  workbooks: Array<{
    id: string;
    name: string;
    dataSources: Array<{
      id: string;
      name: string;
      type: string;
      hasExtracts: boolean;
      extractLastRefreshTime?: string;
      extractRefreshStatus?: string;
    }>;
  }>;
  createdBy: {
    id: string;
    name: string;
    email: string;
  };
  createdAt: string;
  updatedAt: string;
}

export interface ExtractDetails {
  lastRefreshTime?: string;
  lastRefreshStatus?: 'SUCCESS' | 'FAILED' | 'IN_PROGRESS';
  lastRefreshError?: string;
  nextScheduledRefresh?: string;
  refreshSchedule?: ExtractSchedule;
  extractSize?: number;
  isIncremental: boolean;
  lastIncrementalRefresh?: string;
  incrementalRefreshField?: string;
  incrementalRefreshValue?: string;
}

// Add new interfaces for data source types
export interface EmbeddedDataSource {
  id: string;
  name: string;
  type: string;
  connectionType: string;
  isEmbedded: boolean;
  hasExtracts: boolean;
  extractLastRefreshTime?: string;
  connectionDetails: any;
  embeddedIn: {
    id: string;
    name: string;
    type: string;
  };
  upstreamDependencies: Array<{
    id: string;
    name: string;
    type: string;
    connectionType: string;
  }>;
  downstreamDependencies: Array<{
    id: string;
    name: string;
    type: string;
    connectionType: string;
  }>;
}

export interface UpstreamDataSource {
  id: string;
  name: string;
  type: string;
  connectionType: string;
  isEmbedded: boolean;
  hasExtracts: boolean;
  extractLastRefreshTime?: string;
  connectionDetails: any;
  publishedBy: {
    id: string;
    name: string;
    email: string;
  };
  publishedAt: string;
  lastUpdatedAt: string;
  usedBy: Array<{
    id: string;
    name: string;
    type: string;
    project: {
      id: string;
      name: string;
    };
  }>;
}

export interface DataSourceLineage {
  upstream: Array<{
    id: string;
    name: string;
    type: string;
    connectionType: string;
    isEmbedded: boolean;
    hasExtracts: boolean;
    level: number;
    path: string[];
  }>;
  downstream: Array<{
    id: string;
    name: string;
    type: string;
    connectionType: string;
    isEmbedded: boolean;
    hasExtracts: boolean;
    level: number;
    path: string[];
  }>;
}

// Update the TableauQueryResponse interface
export interface TableauQueryResponse {
  data: {
    workbooks?: Array<{
      id: string;
      name: string;
      description: string;
      // ... other fields
    }>;
    workbook?: {
      id: string;
      name: string;
      extractSchedules?: ExtractSchedule[];
      embeddedDataSources?: EmbeddedDataSource[];
      // ... other fields
    };
    dataSource?: {
      id: string;
      name: string;
      extractDetails?: ExtractDetails;
      upstreamDependencies?: UpstreamDataSource[];
      downstreamDependencies?: UpstreamDataSource[];
      lineage?: DataSourceLineage;
      // ... other fields
    };
    extractRefreshSchedules?: ExtractSchedule[];
    tableauMetadata?: {
      totalWorkbookCount: number;
      dataSourceTypes: {
        published: number;
        embedded: number;
        flatFiles: number;
        noDataSources: number;
      };
      lastUpdated: string;
      apiVersion: string;
    };
  };
  errors?: Array<{
    message: string;
    locations: Array<{
      line: number;
      column: number;
    }>;
  }>;
}

// Example usage:
/*
// Get all extract refresh schedules
const getAllExtractSchedules = async () => {
  try {
    const result = await client.query({
      query: GET_EXTRACT_REFRESH_SCHEDULES
    });
    return result.data.extractRefreshSchedules;
  } catch (error) {
    console.error('Error fetching extract schedules:', error);
    throw error;
  }
};

// Get extract schedules for a specific workbook
const getWorkbookExtractSchedules = async (workbookId: string) => {
  try {
    const result = await client.query({
      query: GET_WORKBOOK_EXTRACT_SCHEDULES,
      variables: { workbookId }
    });
    return result.data.workbook.extractSchedules;
  } catch (error) {
    console.error('Error fetching workbook extract schedules:', error);
    throw error;
  }
};

// Get extract details for a specific data source
const getDataSourceExtractDetails = async (dataSourceId: string) => {
  try {
    const result = await client.query({
      query: GET_DATA_SOURCE_EXTRACT_DETAILS,
      variables: { dataSourceId }
    });
    return result.data.dataSource.extractDetails;
  } catch (error) {
    console.error('Error fetching data source extract details:', error);
    throw error;
  }
};

// Get embedded data sources for a workbook
const getEmbeddedDataSources = async (workbookId: string) => {
  try {
    const result = await client.query({
      query: GET_EMBEDDED_DATA_SOURCES,
      variables: { workbookId }
    });
    return result.data.workbook.embeddedDataSources;
  } catch (error) {
    console.error('Error fetching embedded data sources:', error);
    throw error;
  }
};

// Get upstream data sources
const getUpstreamDataSources = async (dataSourceId: string) => {
  try {
    const result = await client.query({
      query: GET_UPSTREAM_DATA_SOURCES,
      variables: { dataSourceId }
    });
    return result.data.dataSource.upstreamDependencies;
  } catch (error) {
    console.error('Error fetching upstream data sources:', error);
    throw error;
  }
};

// Get data source lineage
const getDataSourceLineage = async (dataSourceId: string) => {
  try {
    const result = await client.query({
      query: GET_DATA_SOURCE_LINEAGE,
      variables: { dataSourceId }
    });
    return result.data.dataSource.lineage;
  } catch (error) {
    console.error('Error fetching data source lineage:', error);
    throw error;
  }
};

// Example: Analyze data source dependencies
const analyzeDataSourceDependencies = async (dataSourceId: string) => {
  const lineage = await getDataSourceLineage(dataSourceId);
  
  console.log('Upstream Dependencies:');
  lineage.upstream.forEach(ds => {
    console.log(`Level ${ds.level}: ${ds.name} (${ds.type})`);
    console.log(`Path: ${ds.path.join(' -> ')}`);
  });
  
  console.log('\nDownstream Dependencies:');
  lineage.downstream.forEach(ds => {
    console.log(`Level ${ds.level}: ${ds.name} (${ds.type})`);
    console.log(`Path: ${ds.path.join(' -> ')}`);
  });
};
*/ 