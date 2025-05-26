from typing import Dict, Any, Optional
import requests
from src.utils.logger import app_logger

class TableauGraphQLClient:
    """Handles GraphQL API interactions for Tableau data"""
    
    def __init__(self, api_url: str, api_token: str = None, headers: Dict[str, str] = None):
        """
        Initialize the GraphQL client
        
        Args:
            api_url: URL of the GraphQL API endpoint
            api_token: Optional authentication token
            headers: Optional custom headers
        """
        self.api_url = api_url
        self.headers = headers or {}
        
        if api_token:
            self.headers['Authorization'] = f'Bearer {api_token}'
        
        # Add default headers if not present
        if 'Content-Type' not in self.headers:
            self.headers['Content-Type'] = 'application/json'
    
    def execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a GraphQL query
        
        Args:
            query: GraphQL query string
            variables: Optional variables for the query
            
        Returns:
            Dictionary containing the query response
        """
        try:
            payload = {
                'query': query,
                'variables': variables or {}
            }
            
            app_logger.info(f"Executing GraphQL query to {self.api_url}")
            response = requests.post(
                self.api_url,
                json=payload,
                headers=self.headers
            )
            
            # Raise exception for HTTP errors
            response.raise_for_status()
            
            # Parse JSON response
            result = response.json()
            
            # Check for GraphQL errors
            if 'errors' in result:
                error_messages = [error.get('message', 'Unknown error') for error in result['errors']]
                raise Exception(f"GraphQL errors: {', '.join(error_messages)}")
            
            app_logger.info("Successfully executed GraphQL query")
            return result.get('data', {})
            
        except requests.exceptions.RequestException as e:
            app_logger.error(f"HTTP request failed: {str(e)}")
            raise
        except Exception as e:
            app_logger.error(f"Failed to execute GraphQL query: {str(e)}")
            raise
    
    def get_tableau_data(self) -> Dict[str, Any]:
        """
        Fetch Tableau data using the predefined query
        
        Returns:
            Dictionary containing the Tableau data
        """
        # Define the GraphQL query
        query = """
        query GetTableauData {
            workbooks {
                id
                name
                projectName
                uri
                owner {
                    id
                    name
                    username
                    email
                }
                views {
                    id
                    name
                    path
                    __typename
                    createdAt
                    updatedAt
                }
                upstreamDatasources {
                    id
                    name
                    uri
                    hasExtracts
                    extractLastRefreshTime
                    upstreamDatabases {
                        name
                        connectionType
                        __typename
                    }
                }
                embeddedDatasources {
                    id
                    name
                    hasExtracts
                    extractLastRefreshTime
                    upstreamDatabases {
                        name
                        connectionType
                        __typename
                    }
                }
                tags {
                    id
                    name
                }
            }
        }
        """
        
        try:
            app_logger.info("Fetching Tableau data from GraphQL API")
            result = self.execute_query(query)
            return result
            
        except Exception as e:
            app_logger.error(f"Failed to fetch Tableau data: {str(e)}")
            raise

# Example usage (commented out for now - will be used when GraphQL API is ready)
"""
if __name__ == "__main__":
    # Initialize the client
    client = TableauGraphQLClient(
        api_url="https://your-tableau-server/api/graphql",
        api_token="your-api-token"
    )
    
    try:
        # Fetch data
        data = client.get_tableau_data()
        print("Successfully fetched data from GraphQL API")
        
    except Exception as e:
        print(f"Error: {str(e)}")
""" 