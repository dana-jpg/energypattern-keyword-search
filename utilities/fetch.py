import asyncio
import json
import os

import requests

from constants.urls import GITHUB_GRAPHQL_ENDPOINT
from utilities.load_query import Query, load_gql_query


async def query_gql_endpoint(url, token, query, variables=None):
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    data = {"query": query, "variables": variables}

    response = await asyncio.get_event_loop().run_in_executor(None, lambda: requests.post(url, headers=headers,
                                                                                          data=json.dumps(data)))

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"GraphQL query failed with status code {response.status_code}, {response.reason}")


async def fetch(query_name: Query, variables):
    url = GITHUB_GRAPHQL_ENDPOINT
    token = os.getenv("GITHUB_TOKEN")
    query = load_gql_query(query_name)
    return await query_gql_endpoint(url, token, query, variables)
