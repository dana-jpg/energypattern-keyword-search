import os
import json
import subprocess
import tempfile
from git import Repo
from pygit2 import Repository, GIT_SORT_TOPOLOGICAL


def clone_repo(author, repo_name):
    repo_url = f"https://github.com/{author}/{repo_name}.git"
    temp_dir = tempfile.mkdtemp()
    print(temp_dir)
    Repo.clone_from(repo_url, temp_dir)
    return temp_dir


def checkout_tag(repo_path, tag):
    repo = Repo(repo_path)
    repo.git.checkout(tag)


def run_gumtree_diff(repo_path, tag1, tag2):
    # Create temporary directories for each tag
    temp_dir1 = tempfile.mkdtemp()
    temp_dir2 = tempfile.mkdtemp()
    print(f"{temp_dir1=}, {temp_dir2=}")

    # Clone the repository twice and checkout the respective tags
    Repo.clone_from(repo_path, temp_dir1).git.checkout(tag1)
    Repo.clone_from(repo_path, temp_dir2).git.checkout(tag2)

    # Run GumTree on the entire project
    cmd = [
        "gumtree", "textdiff",
        "-f", "JSON",
        temp_dir1,
        temp_dir2
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Clean up temporary directories
    subprocess.run(["rm", "-rf", temp_dir1, temp_dir2])

    return json.loads(result.stdout)


def generate_metadata(author, repo_name, tag1, tag2):
    repo_path = clone_repo(author, repo_name)

    metadata = {
        "repo": f"{author}/{repo_name}",
        "tag1": tag1,
        "tag2": tag2,
        "changes": run_gumtree_diff(repo_path, tag1, tag2)
    }

    return metadata


def main():
    # author = input("Enter GitHub repository author: ")
    # repo_name = input("Enter GitHub repository name: ")
    # tag1 = input("Enter first tag: ")
    # tag2 = input("Enter second tag: ")

    author = "scverse"
    repo_name = "scanpy"
    tag1 = "1.10.1"
    tag2 = "1.10.2"

    metadata = generate_metadata(author, repo_name, tag1, tag2)

    with open("diff_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print("Metadata has been saved to diff_metadata.json")


if __name__ == "__main__":
    main()
