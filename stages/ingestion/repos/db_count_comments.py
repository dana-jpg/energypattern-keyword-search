from processing_pipeline.keyword_matching.services.MongoDB import MongoDB
from cfg.selected_repos import all_repos


def main():
    comment_count_map = {}
    for repo in all_repos:
        print(f"Parsing github metadata for {repo}")
        db = MongoDB(repo)
        comment_count_map[repo.repo_name] = db.count_comments()[0]["totalComments"]

    print(comment_count_map)
    print("Done!")


if __name__ == "__main__":
    main()
