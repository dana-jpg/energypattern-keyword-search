from typing import Optional

from servicess.IJSONFileCache import IJSONFileCache
from utilities.paths import Paths

type CategoryCacheItem = tuple[Optional[str], bool]


class CategoryCache(IJSONFileCache[CategoryCacheItem]):
    @classmethod
    def _get_filename(cls) -> str:
        return Paths.CACHED_CATEGORY_CURSORS


class CategoryCache_isOrganization(IJSONFileCache[CategoryCacheItem]):
    @classmethod
    def _get_filename(cls) -> str:
        return Paths.CACHED_CATEGORY_CURSORS_isOrganization
