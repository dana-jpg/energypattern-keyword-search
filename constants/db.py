from enum import Enum

PROJECTS_DB_NAME = "projects"


class DBCollections(Enum):
    Repos_by_category = "repos-by-category"
    Selected_projects = "selected-projects"
    Proposal_1 = "proposal-1"
