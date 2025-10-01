import os
import shelve
import time
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Dict, cast, TypeGuard, List, Iterator, Literal

from github import Github
from github.Issue import Issue
from github.IssueComment import IssueComment
from github import RateLimitExceededException
from tqdm import tqdm

from constants.abs_paths import AbsDirPath
from models.Repo import Repo

InternalReactionKey = Literal[
    "thumbs_up", "thumbs_down", "laugh", "confused", 
    "heart", "hooray", "rocket", "eyes"
]
ReactionKey = Literal[InternalReactionKey, "+1", "-1"]

@dataclass
class ReactionDTO:
    thumbs_up: int = 0
    thumbs_down: int = 0
    laugh: int = 0
    confused: int = 0
    heart: int = 0
    hooray: int = 0
    rocket: int = 0
    eyes: int = 0

    _name_map: ClassVar[Dict[ReactionKey, InternalReactionKey]] = {'+1': 'thumbs_up', '-1': 'thumbs_down', }

    def _get_key(self, key: ReactionKey) -> InternalReactionKey:
        return self._name_map.get(key, cast(InternalReactionKey, key))

    @classmethod
    def is_reaction_key(cls, key: str) -> TypeGuard[ReactionKey]:
        return key in [*cls.__dict__.keys(), "+1", "-1"]

    def add(self, reaction: str):
        if self.is_reaction_key(reaction):
            key = self._get_key(reaction)
            self.__setattr__(key, self.__getattribute__(key) + 1)


@dataclass
class CommentDTO:
    issue_id: int
    _id: int
    html_url: str
    body: str
    user: str | None
    created_at: datetime
    updated_at: datetime
    reactions: ReactionDTO


@dataclass
class IssueDTO:
    _id: int
    html_url: str
    number: int
    pull_request_html_url: str | None
    title: str
    body: str
    state: str
    created_at: datetime
    updated_at: datetime
    closed_at: datetime
    labels: List[str]
    author: str | None
    assignees: List[str]
    milestone: str | None
    comments_count: int
    comments_data: List[CommentDTO]
    reactions: ReactionDTO

    @property
    def id(self):
        return self._id


@dataclass
class ReleaseDTO:
    _id: int
    html_url: str
    tag_name: str
    title: str
    name: str
    body: str
    created_at: datetime
    published_at: datetime
    draft: bool
    prerelease: bool
    author: str | None
    asset_count: int

    @property
    def id(self):
        return self._id


@dataclass
class RepoInfoDTO:
    latest_version: str
    homepage: str


class GithubDataFetcher:
    def __init__(self, token: str, repo: Repo):
        """
        Initialize the fetcher with GitHub token

        Args:
            token (str): GitHub Personal Access Token
        """
        self.github = Github(token, per_page=100, retry=3, timeout=30)
        self.repo = repo
    
    def _respect_rate_limit(self, min_remaining: int = 100) -> None:
        """
        Sleep until reset if our cached remaining tokens drop below min_remaining.
        Uses header-derived attributes that do NOT trigger an extra API call.
        """
        try:
            remaining, _ = self.github.rate_limiting  # tuple: (remaining, limit)
            reset_ts = self.github.rate_limiting_resettime  # unix epoch seconds

            if remaining is not None and reset_ts is not None and remaining < min_remaining:
                sleep_for = max(int(reset_ts - time.time()) + 1, 1)
                time.sleep(sleep_for)
        except Exception:
            # Be conservative if anything looks odd; don’t fail the run because of rate checks.
            pass


    def get_repo_info(self) -> RepoInfoDTO:
        repo = self.github.get_repo(self.repo.git_id)
        return RepoInfoDTO(latest_version=repo.get_latest_release().tag_name, homepage=repo.homepage)

    def get_issues(self, batch_size: int = 10) -> Iterator[List[IssueDTO]]:
        assert batch_size > 0, "Batch size must be greater than 0"
        repo = self.github.get_repo(self.repo.git_id)

        issue_cache_dir = AbsDirPath.CACHE / "issues"
        os.makedirs(issue_cache_dir, exist_ok=True)
        with shelve.open(issue_cache_dir / self.repo.repo_name) as db:
            since = db.get("since", None)
            # Get all issues (including pull requests)
            issues = repo.get_issues(state='all', direction='asc', since=since) if since else repo.get_issues(
                state='all', direction='asc')
            total_count = issues.totalCount

            batch = []
            for issue in tqdm(issues, total=total_count, desc="Fetching issues"):
                try:
                    batch.append(self._map_issue_to_dto(issue))

                    if len(batch) == batch_size:
                        yield batch
                        db["since"] = issue.created_at
                        batch.clear()

                    self._respect_rate_limit(min_remaining=100)

                except RateLimitExceededException as rle:
                    # Fallback safety: if we still hit the limit, sleep until reset and continue
                    # PyGithub raises this with headers already parsed; the global attributes are set.
                    self._respect_rate_limit(min_remaining=1)
                    continue
                except Exception as e:
                    print(f"Error processing issue #{getattr(issue, 'number', '?')}: {str(e)}")
                    continue

            if len(batch) > 0:
                yield batch

    def _map_issue_to_dto(self, issue: Issue) -> IssueDTO:
        # Get reactions for the issue
        reactions = self._get_reactions(issue)
        # Get comments with their reactions
        comments_data = self._get_comments(issue)
        issue_data = IssueDTO(_id=issue.id, html_url=issue.html_url, number=issue.number,
                              pull_request_html_url=issue.pull_request.html_url if issue.pull_request else None,
                              title=issue.title, body=issue.body, state=issue.state, created_at=issue.created_at,
                              updated_at=issue.updated_at, closed_at=issue.closed_at,
                              labels=[label.name for label in issue.labels],
                              author=issue.user.login if issue.user else None,
                              assignees=[assignee.login for assignee in issue.assignees],
                              milestone=issue.milestone.title if issue.milestone else None,
                              comments_count=issue.comments, comments_data=comments_data, reactions=reactions)
        return issue_data

    def _get_comments(self, issue: Issue) -> List[CommentDTO]:
        """Fetch all comments for an issue with their reactions"""
        comments_data = []

        for comment in issue.get_comments():
            try:
                reactions = self._get_reactions(comment)

                comment_data = CommentDTO(_id=comment.id, issue_id=issue.id, html_url=comment.html_url,
                                          body=comment.body, user=comment.user.login if comment.user else None,
                                          created_at=comment.created_at, updated_at=comment.updated_at,
                                          reactions=reactions)
                comments_data.append(comment_data)

            except Exception as e:
                print(f"Error processing comment {comment.id}: {str(e)}")
                continue

        return comments_data

    def _get_reactions(self, item: Issue | IssueComment) -> ReactionDTO:
        """Get reaction counts for an issue or comment"""
        reaction_counts = ReactionDTO()

        try:
            reactions = item.get_reactions()
            for reaction in reactions:
                reaction_counts.add(reaction.content)
        except Exception as e:
            print(f"Error getting reactions: {str(e)}")

        return reaction_counts

    def get_releases(self, batch_size=10) -> Iterator[List[ReleaseDTO]]:
        assert batch_size > 0, "Batch size must be greater than 0"

        repo = self.github.get_repo(self.repo.git_id)

        releases = repo.get_releases()
        total_releases = releases.totalCount

        batch = []
        for release in tqdm(releases, total=total_releases, desc="Fetching releases"):
            try:
                release_data = ReleaseDTO(_id=release.id, html_url=release.html_url, title=release.title,
                                          tag_name=release.tag_name, name=release.title, body=release.body,
                                          created_at=release.created_at, published_at=release.published_at,
                                          draft=release.draft, prerelease=release.prerelease,
                                          author=release.author.login if release.author else None,
                                          asset_count=release.get_assets().totalCount)
                batch.append(release_data)
                if len(batch) == batch_size:
                    yield batch
                    batch.clear()

            except Exception as e:
                print(f"Error processing release {release.title}: {str(e)}")
                continue

        if len(batch) > 0:
            yield batch
