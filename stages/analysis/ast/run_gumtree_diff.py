import os
import json
import subprocess

from services.git import clone_repo, clone_tag, get_abs_parent_dir


def run_gumtree_diff(author, repo_name, repo_path, tag1, tag2):

    # Clone the repository twice and checkout the respective tags
    path1 = clone_tag(author, repo_name, repo_path, tag1)
    path2 = clone_tag(author, repo_name, repo_path, tag2)

    root_dir = get_abs_parent_dir()

    # Run GumTree on the entire project
    cmd = [
        "gumtree", "textdiff",
        "-f", "JSON",
        os.path.join(root_dir, path1),
        os.path.join(root_dir, path2),
        "-o", f"{repo_path}_{tag1}..{tag2}.diff.json"
    ]
    print(" ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    return json.loads(result.stdout)


def generate_metadata(author, repo_name, tag1, tag2):
    os.makedirs('../../../.tmp/source', exist_ok=True)
    repo_path = clone_repo(author, repo_name)

    metadata = {
        "repo": f"{author}/{repo_name}",
        "tag1": tag1,
        "tag2": tag2,
        "changes": run_gumtree_diff(author, repo_name, repo_path, tag1, tag2)
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
