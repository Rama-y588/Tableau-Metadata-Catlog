from typing import Dict, List


def generate_graphql_queries(mapping: Dict[str, str], query_template: str) -> List[Dict[str, str]]:
    queries = []
    for project_id, project_name in mapping.items():
        query = query_template.replace("project_id_val", project_id).replace("project_name_val", project_name)
        queries.append({
            "project_id": project_id,
            "project_name": project_name,
            "query": query
        })
    return queries  