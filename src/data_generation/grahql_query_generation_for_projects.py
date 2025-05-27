from typing import Dict, List

def generate_graphql_queries(mapping: Dict[str, str], query_template: str) -> List[Dict[str, str]]:
    """
    Generates a list of GraphQL queries by replacing placeholders in the query template
    with actual project_id and project_name values from the mapping.

    Args:
        mapping (Dict[str, str]): A dictionary where the key is the project_id and 
                                  the value is the corresponding project_name.
        query_template (str): A string containing the GraphQL query with placeholders 
                              "project_id_val" and "project_name_val".

    Returns:
        List[Dict[str, str]]: A list of dictionaries, each containing:
            - project_id (str)
            - project_name (str)
            - query (str): The generated GraphQL query with placeholders replaced.
    """
    queries = []

    # Loop through each project_id and project_name pair in the mapping
    for project_id, project_name in mapping.items():
        # Replace placeholders in the query template with actual values
        query = query_template.replace("project_id_val", project_id).replace("project_name_val", project_name)

        # Append the result as a dictionary to the queries list
        queries.append({
            "project_id": project_id,
            "project_name": project_name,
            "query": query
        })

    return queries
