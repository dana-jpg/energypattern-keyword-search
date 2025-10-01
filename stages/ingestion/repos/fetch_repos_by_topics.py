import asyncio
from asyncio import Semaphore

import dotenv

from actions.db_actions import upsert_collection_async
from constants.db import PROJECTS_DB_NAME, DBCollections
from servicess.fetcher.CategoryTopics import CategoryTopicsFetcher_isOrganization
from servicess.MongoDBConnection import MongoDBConnection
from tag_parser.tag_parser import get_tags
from utilities.paths import Paths
from utilities.utils import get_golden_repos

dotenv.load_dotenv()

REPOSITORY_COUNT_PER_QUERY = 15
MAX_PARALLEL_TASKS = 6


async def perform_fetch(Fetcher, new_tags=False):
    tags = await load_tags(new_tags)
    semaphore = asyncio.Semaphore(MAX_PARALLEL_TASKS)
    tasks = [query_and_insert(semaphore, Fetcher, topic) for topic in tags]
    await asyncio.gather(*tasks)
    MongoDBConnection().close_connection()


async def query_and_insert(s: 'Semaphore', Fetcher, category):
    async with s:
        await Fetcher.run(category, REPOSITORY_COUNT_PER_QUERY)


async def load_tags(new=False):
    if new:
        repos = get_golden_repos()
        tags = get_tags(repos)
    else:
        with open(Paths.TAGS, "r") as f:
            tags = f.read().splitlines()

    return tags


def main():
    Fetcher = CategoryTopicsFetcher_isOrganization(
        lambda data: upsert_collection_async(PROJECTS_DB_NAME, DBCollections.Repos_by_category.value, data))
    asyncio.run(perform_fetch(Fetcher, False))


if __name__ == "__main__":
    main()
