from constants.abs_paths import AbsDirPath


class Paths:
    GOLDEN_REPOS = AbsDirPath.ANALYSIS / 'repos/golden-repositories.txt'
    TAGS = AbsDirPath.ANALYSIS / 'repo_topics/topics.txt'
    TAGS_TO_ELIMINATE = AbsDirPath.ANALYSIS / 'repo_topics/topics-to-exclude.txt'
    TAGS_EXTRACTED = AbsDirPath.ANALYSIS / 'repo_topics/topics-raw.txt'
    CACHED_CATEGORY_CURSORS = AbsDirPath.CACHE / 'category_cursors.json'
    CACHED_CATEGORY_CURSORS_isOrganization = AbsDirPath.CACHE / 'category_cursors_addition.json'
