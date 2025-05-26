// Type definitions for Tableau GraphQL API responses

export interface TableauWorkbook {
  id: string;
  name: string;
  description: string;
  createdAt: string;
  updatedAt: string;
  owner: {
    id: string;
    name: string;
    email: string;
  };
  project: {
    id: string;
    name: string;
  };
  views: TableauView[];
}

export interface TableauView {
  id: string;
  name: string;
  workbookId: string;
  createdAt: string;
  updatedAt: string;
  dataColumns: DataColumn[];
  filters: Filter[];
}

export interface DataColumn {
  id: string;
  name: string;
  dataType: 'STRING' | 'NUMBER' | 'DATE' | 'BOOLEAN';
  role: 'DIMENSION' | 'MEASURE';
}

export interface Filter {
  id: string;
  name: string;
  type: 'RANGE' | 'LIST' | 'WILDCARD';
  values: string[];
}

// Dummy data
export const dummyTableauData = {
  workbooks: [
    {
      id: "wb-001",
      name: "Sales Analysis Dashboard",
      description: "Comprehensive sales analysis across regions and products",
      createdAt: "2024-01-15T08:00:00Z",
      updatedAt: "2024-03-10T14:30:00Z",
      owner: {
        id: "user-001",
        name: "John Doe",
        email: "john.doe@company.com"
      },
      project: {
        id: "proj-001",
        name: "Sales Analytics"
      },
      views: [
        {
          id: "view-001",
          name: "Regional Sales Overview",
          workbookId: "wb-001",
          createdAt: "2024-01-15T08:00:00Z",
          updatedAt: "2024-03-10T14:30:00Z",
          dataColumns: [
            {
              id: "col-001",
              name: "Region",
              dataType: "STRING",
              role: "DIMENSION"
            },
            {
              id: "col-002",
              name: "Sales Amount",
              dataType: "NUMBER",
              role: "MEASURE"
            },
            {
              id: "col-003",
              name: "Order Date",
              dataType: "DATE",
              role: "DIMENSION"
            }
          ],
          filters: [
            {
              id: "filter-001",
              name: "Date Range",
              type: "RANGE",
              values: ["2024-01-01", "2024-03-31"]
            },
            {
              id: "filter-002",
              name: "Region Selection",
              type: "LIST",
              values: ["North", "South", "East", "West"]
            }
          ]
        },
        {
          id: "view-002",
          name: "Product Performance",
          workbookId: "wb-001",
          createdAt: "2024-01-15T08:00:00Z",
          updatedAt: "2024-03-10T14:30:00Z",
          dataColumns: [
            {
              id: "col-004",
              name: "Product Category",
              dataType: "STRING",
              role: "DIMENSION"
            },
            {
              id: "col-005",
              name: "Units Sold",
              dataType: "NUMBER",
              role: "MEASURE"
            },
            {
              id: "col-006",
              name: "Profit Margin",
              dataType: "NUMBER",
              role: "MEASURE"
            }
          ],
          filters: [
            {
              id: "filter-003",
              name: "Product Category",
              type: "LIST",
              values: ["Electronics", "Clothing", "Home Goods", "Food"]
            }
          ]
        }
      ]
    },
    {
      id: "wb-002",
      name: "Customer Insights",
      description: "Customer behavior and demographics analysis",
      createdAt: "2024-02-01T10:00:00Z",
      updatedAt: "2024-03-08T16:45:00Z",
      owner: {
        id: "user-002",
        name: "Jane Smith",
        email: "jane.smith@company.com"
      },
      project: {
        id: "proj-002",
        name: "Customer Analytics"
      },
      views: [
        {
          id: "view-003",
          name: "Customer Demographics",
          workbookId: "wb-002",
          createdAt: "2024-02-01T10:00:00Z",
          updatedAt: "2024-03-08T16:45:00Z",
          dataColumns: [
            {
              id: "col-007",
              name: "Age Group",
              dataType: "STRING",
              role: "DIMENSION"
            },
            {
              id: "col-008",
              name: "Customer Count",
              dataType: "NUMBER",
              role: "MEASURE"
            },
            {
              id: "col-009",
              name: "Average Purchase Value",
              dataType: "NUMBER",
              role: "MEASURE"
            }
          ],
          filters: [
            {
              id: "filter-004",
              name: "Age Groups",
              type: "LIST",
              values: ["18-24", "25-34", "35-44", "45-54", "55+"]
            }
          ]
        }
      ]
    }
  ]
};

// Example GraphQL query response structure
export const dummyGraphQLResponse = {
  data: {
    workbooks: dummyTableauData.workbooks,
    metadata: {
      totalCount: 2,
      pageSize: 10,
      currentPage: 1
    }
  },
  errors: null
}; 