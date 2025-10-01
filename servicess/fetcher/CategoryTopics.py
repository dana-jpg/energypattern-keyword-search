import asyncio
from typing import Type, Callable, Coroutine, Any

from servicess.Cache import CategoryCache, CategoryCache_isOrganization
from servicess.IJSONFileCache import IJSONFileCache
from utilities.data_transformation import wrap_with_update_one_operation
from utilities.fetch import fetch
from utilities.load_query import Query


class CategoryTopicsFetcher:
    _query: Query = Query.REPOS_BY_TOPIC
    _cache: Type[IJSONFileCache] = CategoryCache

    def __init__(self, callback: Callable[[object], Coroutine[Any, Any, None]]):
        self._callback = callback
        self._cache.init()

    async def run(self, category: str, n_repos_to_fetch: int):
        cache = self._cache
        cursor, finished = cache.get(category, (None, False))
        has_next = not finished
        loop = 0

        while has_next:
            variables = {"topic_name": category, "number_of_repos": n_repos_to_fetch, 'cursor': cursor}
            try:
                result = await fetch(self._query, variables)
            except Exception as e:
                print("stumbled upon an exception", e)
                await asyncio.sleep(10)
                continue

            topic = result['data']['topic']
            page_info = (repos_raw := topic['repositories'])['pageInfo']

            repos = [wrap_with_update_one_operation(self._transform_data(repo, category)) for repo in
                     repos_raw['nodes']]

            try:
                await self._callback(repos)
            except Exception as e:
                print("Error in upsert_collection_async", e)
                return None

            print(
                f"got repos {category = }, {loop = }, idx={loop * n_repos_to_fetch}, {repos_raw['totalCount'] = }, {has_next =}, {cursor = } ")
            cursor = page_info['endCursor']
            has_next = page_info['hasNextPage']
            loop += 1

            cache.set(category, (cursor, not has_next))

        print(f"Finished fetching {category = }")

    @classmethod
    def _transform_data(cls, repo, category):
        keys_with_total_objects = ["watchers", "contributors", "branches", "tags", "deployments", "issuesOpen",
                                   "issuesClosed", "issuesAll", "PRsOpen", "PRsClosed", "PRsMerged", "PRsAll",
                                   "languages",
                                   "releases", ]
        keys_to_omit = keys_with_total_objects + ["owner", "repositoryTopics"]
        filtered_repo = {k: v for k, v in repo.items() if k not in keys_to_omit}
        return {'_id': repo['resourcePath'], "topic": category,
                **cls._extract_total_count(repo, keys_with_total_objects),
                **filtered_repo, "owner": repo['owner']['login'], "languages": repo['languages']['nodes'],
                "repositoryTopics": repo['repositoryTopics']['nodes'], }

    @classmethod
    def _extract_total_count(cls, repo, field_names: list[str]):
        return {f"{field_name}N": repo[field_name]['totalCount'] for field_name in field_names}


class CategoryTopicsFetcher_isOrganization(CategoryTopicsFetcher):
    _query: Query = Query.REPOS_BY_TOPIC_IS_ORGANIZATION
    _cache: Type[IJSONFileCache] = CategoryCache_isOrganization

    @classmethod
    def _transform_data(cls, repo, category):
        return {'_id': repo['resourcePath'], **repo}
