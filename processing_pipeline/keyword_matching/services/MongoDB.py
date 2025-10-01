import dataclasses
import re
from typing import TypedDict, List

from pymongo import UpdateOne
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.command_cursor import CommandCursor

from models.Repo import Repo
from processing_pipeline.keyword_matching.services.GithubDataFetcher import IssueDTO, ReleaseDTO
from servicess.MongoDBConnection import MongoDBConnection


class MongoMatch(TypedDict):
    text: str
    html_url: str


class MongoDB:
    def __init__(self, repo: Repo):
        self.non_robot_users = ["olgabot", "hugtalbot", "arrogantrobot", "robot-chenwei", "Bot-Enigma-0"]
        self.regex_omitting_bots = re.compile(r"bot\b", re.IGNORECASE)
        self.repo = repo
        self.client = MongoDBConnection().get_client()

    def _issue_collection(self) -> Collection:
        return self.client['git_issues'][self.repo.repo_name]

    def _releases_collection(self) -> Collection:
        return self.client['git_releases'][self.repo.repo_name]

    def insert_issues(self, documents: List[IssueDTO]):
        table = self._issue_collection()
        try:
            res = table.bulk_write(
                [UpdateOne({"_id": issue.id}, {"$set": dataclasses.asdict(issue)}, upsert=True) for issue in documents])
            print(res)
        except Exception as e:
            print(e)

    def insert_releases(self, documents: List[ReleaseDTO]):
        table = self._releases_collection()
        try:
            res = table.bulk_write(
                [UpdateOne({"_id": release.id}, {"$set": dataclasses.asdict(release)}, upsert=True) for release in
                 documents])
            print(res)
        except Exception as e:
            print(e)

    def extract_comments(self) -> CommandCursor[MongoMatch]:
        return self._issue_collection().aggregate(
            [{"$unwind": "$comments_data"}, {"$addFields": {"text": "$comments_data.body"}}, {"$match": {
                "$or": [{"comments_data.user": {"$not": {"$regex": self.regex_omitting_bots}}},
                        {"comments_data.user": {"$in": self.non_robot_users}}]}},
             {"$project": {"text": 1, "html_url": 1, }}])

    def extract_issues(self) -> CommandCursor[MongoMatch]:
        return self._issue_collection().aggregate([{# 1. Concatenate 'title' and 'body' into a new field called 'text'
            "$addFields": {"text": {"$concat": ["$title", "; ", "$body"]}}},
            {"$match": {
                "$or": [{"author": {"$not": {"$regex": self.regex_omitting_bots}}},
                        {"author": {"$in": self.non_robot_users}}]}},
            {"$project": {"text": 1, "html_url": 1, }}])

    def extract_releases(self) -> CommandCursor[MongoMatch]:
        return self._releases_collection().aggregate(
            [{"$addFields": {"text": {"$trim": {"input": "$body"}}}}, {"$project": {"text": 1, "html_url": 1}}])

    def count_comments(self):
        return self._issue_collection().aggregate(
            [{"$group": {"_id": None, "totalComments": {"$sum": "$comments_count"}}}]).to_list()
