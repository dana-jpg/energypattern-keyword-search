import os
from pathlib import Path
from typing import Optional

from git import Repo


def get_repo_base_path(author, repo_name, postfix="master"):
    repo_base_path = f"./.tmp/source/{author}/{repo_name}/{postfix}"
    return repo_base_path


def clone_repo(author, repo_name, postfix: Optional[str] = "master"):
    repo_url = f"https://github.com/{author}/{repo_name}.git"
    path = get_repo_base_path(author, repo_name, postfix)
    os.makedirs(path, exist_ok=True)
    if not os.path.exists(path) or not os.listdir(path):
        repo = Repo.clone_from(repo_url, path)
        repo.git.checkout(postfix)
        repo.git.reset('--hard', postfix)
    return path


def clone_tag(author, repo_name, repo_path, tag):
    path = f"./.tmp/source/{author}/{repo_name}/{tag}"
    if not os.path.exists(path) or not os.listdir(path):
        repo = Repo.clone_from(repo_path, path)
        repo.git.checkout(tag)
        repo.git.reset('--hard', tag)
    return path


def checkout_tag(author, repo_name, tag) -> str:
    path = get_repo_base_path(author, repo_name, tag)
    if not os.path.exists(path) or not os.listdir(path):
        clone_repo(author, repo_name, tag)

    Repo(path).git.checkout(tag)
    return path


def get_abs_parent_dir():
    return Path(__file__).resolve().parent
