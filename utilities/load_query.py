from enum import Enum


class Query(str, Enum):
    REPO_DETAILS = "queries/repo_details.gql"
    REPOS_BY_TOPIC = "queries/repos_by_topic.gql"
    REPOS_BY_TOPIC_IS_ORGANIZATION = "queries/repos_by_topic_isOrganization.gql"


def load_gql_query(query: Query):
    with open(query, "r") as f:
        query = f.read()
    return query
