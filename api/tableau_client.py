import aiohttp
import json
from typing import Dict, Any, Optional
import logging

class TableauGraphQLClient:
    def __init__(self, server_url: str, site_id: str, token: str):
        """
        Initialize Tableau GraphQL client.
        
        Args:
            server_url: Tableau server URL
            site_id: Tableau site ID
            token: Authentication token
        """
        self.server_url = server_url.rstrip('/')
        self.site_id = site_id
        self.token = token
        self.endpoint = f"{self.server_url}/api/{site_id}/graphql"
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('TableauGraphQLClient')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    async def query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a GraphQL query.
        
        Args:
            query: GraphQL query string
            variables: Optional query variables
            
        Returns:
            Query response data
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        
        payload = {
            'query': query,
            'variables': variables or {}
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.endpoint,
                    headers=headers,
                    json=payload
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        self.logger.error(f'Query failed: {error_text}')
                        raise Exception(f'Query failed with status {response.status}: {error_text}')
                    
                    result = await response.json()
                    
                    if 'errors' in result:
                        self.logger.error(f'GraphQL errors: {result["errors"]}')
                        raise Exception(f'GraphQL errors: {result["errors"]}')
                    
                    return result['data']

        except Exception as e:
            self.logger.error(f'Error executing query: {str(e)}')
            raise

    async def download_hyper_file(self, datasource_id: str, output_path: str) -> str:
        """
        Download a .hyper file from a published data source.
        
        Args:
            datasource_id: ID of the data source
            output_path: Path to save the .hyper file
            
        Returns:
            Path to the downloaded file
        """
        url = f"{self.server_url}/api/{self.site_id}/datasources/{datasource_id}/content"
        headers = {'Authorization': f'Bearer {self.token}'}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        self.logger.error(f'Download failed: {error_text}')
                        raise Exception(f'Download failed with status {response.status}: {error_text}')
                    
                    content = await response.read()
                    
                    with open(output_path, 'wb') as f:
                        f.write(content)
                    
                    self.logger.info(f'Successfully downloaded .hyper file to {output_path}')
                    return output_path

        except Exception as e:
            self.logger.error(f'Error downloading .hyper file: {str(e)}')
            raise

# Usage Example:
"""
async def main():
    # Initialize client
    client = TableauGraphQLClient(
        server_url='https://your-tableau-server',
        site_id='your-site-id',
        token='your-token'
    )
    
    # Execute a query
    query = '''
        query GetWorkbooks {
            workbooks {
                id
                name
            }
        }
    '''
    result = await client.query(query)
    print(result)
    
    # Download a .hyper file
    await client.download_hyper_file(
        datasource_id='ds-123',
        output_path='./downloads/datasource.hyper'
    )

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
""" 