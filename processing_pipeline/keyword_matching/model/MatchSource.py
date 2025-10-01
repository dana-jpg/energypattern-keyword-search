from enum import Enum


class MatchSource(Enum):
    RELEASE = "release"
    WIKI = "wiki"
    DOCS = "docs"
    ISSUE = "issue"
    ISSUE_COMMENT = "issue_comment"
    CODE_COMMENT = "code_comment"
