// Comparison of REST API vs GraphQL for Tableau Metadata Catalog

// REST API Approach
export const tableauRestEndpoints = {
  // Multiple endpoints needed for different resources
  workbooks: '/api/3.12/sites/{siteId}/workbooks',
  dataSources: '/api/3.12/sites/{siteId}/datasources',
  views: '/api/3.12/sites/{siteId}/workbooks/{workbookId}/views',
  extracts: '/api/3.12/sites/{siteId}/datasources/{datasourceId}/extracts',
  schedules: '/api/3.12/sites/{siteId}/schedules',
  projects: '/api/3.12/sites/{siteId}/projects',
  users: '/api/3.12/sites/{siteId}/users',
  groups: '/api/3.12/sites/{siteId}/groups'
};

// REST API Implementation Example
export class TableauRestClient {
  private baseUrl: string;
  private siteId: string;
  private token: string;

  constructor(baseUrl: string, siteId: string, token: string) {
    this.baseUrl = baseUrl;
    this.siteId = siteId;
    this.token = token;
  }

  // Multiple separate API calls needed
  async getWorkbookMetadata(workbookId: string) {
    // 1. Get basic workbook info
    const workbookResponse = await fetch(
      `${this.baseUrl}${tableauRestEndpoints.workbooks}/${workbookId}`,
      {
        headers: { 'X-Tableau-Auth': this.token }
      }
    );
    const workbook = await workbookResponse.json();

    // 2. Get views for this workbook
    const viewsResponse = await fetch(
      `${this.baseUrl}${tableauRestEndpoints.views.replace('{workbookId}', workbookId)}`,
      {
        headers: { 'X-Tableau-Auth': this.token }
      }
    );
    const views = await viewsResponse.json();

    // 3. Get data sources for this workbook
    const dataSourcesResponse = await fetch(
      `${this.baseUrl}${tableauRestEndpoints.dataSources}?filter=workbookId:eq:${workbookId}`,
      {
        headers: { 'X-Tableau-Auth': this.token }
      }
    );
    const dataSources = await dataSourcesResponse.json();

    // 4. Get extract schedules for each data source
    const extractPromises = dataSources.map(ds => 
      fetch(
        `${this.baseUrl}${tableauRestEndpoints.extracts.replace('{datasourceId}', ds.id)}`,
        {
          headers: { 'X-Tableau-Auth': this.token }
        }
      ).then(res => res.json())
    );
    const extracts = await Promise.all(extractPromises);

    // Combine all the data
    return {
      ...workbook,
      views,
      dataSources: dataSources.map((ds, index) => ({
        ...ds,
        extracts: extracts[index]
      }))
    };
  }

  // Need separate methods for different types of queries
  async getDataSourcesByType(type: string) {
    const response = await fetch(
      `${this.baseUrl}${tableauRestEndpoints.dataSources}?filter=type:eq:${type}`,
      {
        headers: { 'X-Tableau-Auth': this.token }
      }
    );
    return response.json();
  }

  async getWorkbooksByProject(projectId: string) {
    const response = await fetch(
      `${this.baseUrl}${tableauRestEndpoints.workbooks}?filter=projectId:eq:${projectId}`,
      {
        headers: { 'X-Tableau-Auth': this.token }
      }
    );
    return response.json();
  }
}

// GraphQL Approach
export const tableauGraphQLQueries = {
  // Single endpoint with flexible queries
  getWorkbookMetadata: `
    query GetWorkbookMetadata($workbookId: ID!) {
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
        views {
          id
          name
          createdAt
          updatedAt
          dataColumns {
            id
            name
            dataType
            role
          }
        }
        dataSources {
          id
          name
          type
          connectionType
          isEmbedded
          hasExtracts
          extractLastRefreshTime
          extractDetails {
            lastRefreshTime
            nextScheduledRefresh
            refreshSchedule {
              id
              name
              status
              scheduleDetails {
                frequency
                startTime
                timeZone
                nextRunTime
              }
            }
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
  `,

  getDataSourcesByType: `
    query GetDataSourcesByType($type: DataSourceType!) {
      dataSources(where: { type: $type }) {
        id
        name
        type
        connectionType
        isEmbedded
        hasExtracts
        extractLastRefreshTime
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
  `,

  getWorkbooksByProject: `
    query GetWorkbooksByProject($projectId: ID!) {
      project(id: $projectId) {
        id
        name
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
          dataSources {
            id
            name
            type
            hasExtracts
          }
          views {
            id
            name
            createdAt
          }
        }
      }
    }
  `
};

// GraphQL Implementation Example
export class TableauGraphQLClient {
  private endpoint: string;
  private token: string;

  constructor(endpoint: string, token: string) {
    this.endpoint = endpoint;
    this.token = token;
  }

  // Single method to handle all queries
  async query<T>(query: string, variables?: any): Promise<T> {
    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.token}`
      },
      body: JSON.stringify({
        query,
        variables
      })
    });

    const result = await response.json();
    if (result.errors) {
      throw new Error(result.errors[0].message);
    }
    return result.data;
  }

  // Reuse the same query method for different needs
  async getWorkbookMetadata(workbookId: string) {
    return this.query(
      tableauGraphQLQueries.getWorkbookMetadata,
      { workbookId }
    );
  }

  async getDataSourcesByType(type: string) {
    return this.query(
      tableauGraphQLQueries.getDataSourcesByType,
      { type }
    );
  }

  async getWorkbooksByProject(projectId: string) {
    return this.query(
      tableauGraphQLQueries.getWorkbooksByProject,
      { projectId }
    );
  }
}

// Comparison of approaches:

/*
REST API Advantages:
1. Simpler to understand and implement
2. Better caching capabilities
3. More mature tooling and documentation
4. Easier to debug (can use browser tools)
5. Better for file uploads/downloads
6. Stateless by design

REST API Disadvantages:
1. Multiple endpoints to manage
2. Over-fetching or under-fetching data
3. More HTTP requests needed for complex data
4. Versioning can be challenging
5. Less flexible for complex queries
6. More boilerplate code

GraphQL Advantages:
1. Single endpoint for all queries
2. Flexible data fetching (get exactly what you need)
3. Strong typing and schema validation
4. Better for complex, nested data
5. Reduced number of HTTP requests
6. Self-documenting through schema
7. Better for real-time updates
8. Easier to maintain and evolve

GraphQL Disadvantages:
1. Steeper learning curve
2. More complex to set up
3. Can be overkill for simple applications
4. Caching is more challenging
5. Performance can be an issue with complex queries
6. Requires more server-side processing

Use REST API when:
1. Building simple CRUD applications
2. Need strong caching
3. Working with file uploads/downloads
4. Have simple data requirements
5. Need to support older clients
6. Have limited server resources

Use GraphQL when:
1. Building a metadata catalog
2. Need flexible data fetching
3. Have complex, nested data structures
4. Want to reduce number of API calls
5. Need real-time updates
6. Have complex filtering requirements
7. Want to minimize over-fetching
*/

// Example usage of both approaches:

/*
// REST API Usage
const restClient = new TableauRestClient(
  'https://your-tableau-server',
  'your-site-id',
  'your-token'
);

// Need multiple calls to get complete metadata
const workbookMetadata = await restClient.getWorkbookMetadata('wb-001');
const dataSources = await restClient.getDataSourcesByType('PUBLISHED');
const projectWorkbooks = await restClient.getWorkbooksByProject('proj-001');

// GraphQL Usage
const graphqlClient = new TableauGraphQLClient(
  'https://your-tableau-server/graphql',
  'your-token'
);

// Single call to get complete metadata
const workbookMetadata = await graphqlClient.getWorkbookMetadata('wb-001');
const dataSources = await graphqlClient.getDataSourcesByType('PUBLISHED');
const projectWorkbooks = await graphqlClient.getWorkbooksByProject('proj-001');
*/ 