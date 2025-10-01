import os
from pathlib import Path
import re

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame

from services.ast_extractor import read_file, parse_code, extract_tree, ast_main_definitions_iterator
from services.git import checkout_tag, clone_repo


def parse_ast(project_root_path, path, project_data):
    try:
        code, filename, lang = read_file(path)
        tree = parse_code(code, lang)
        items = []
        for el in ast_main_definitions_iterator(lang, tree):
            embedding_no_type = re.sub(r"^[\w\s]+?: ?", "", el.get("embedding", ""))
            id = "_".join(project_data.values())
            item = dict(id=id, filename=filename, ext=lang, embedding_no_type=embedding_no_type, **el, **project_data)
            print(item)
            items.append(item)
        relative_dir_path = path.relative_to(project_root_path).parent
        relative_file_path = path.relative_to(project_root_path)
        items.append(dict(filename=filename, ext=lang, **project_data, embedding=f"File: {relative_dir_path}"))
        items.append(dict(filename=filename, ext=lang, **project_data, embedding=f"Directory: {relative_file_path}"))
        return items
    except Exception as e:
        print(f"Error parsing {path}: {e}")
        return []


def main():
    author = "scverse"
    repo_name = "scanpy"
    tag = "1.10.1"

    # author = "allenai"
    # repo_name = "scispacy"
    # tag = "v0.5.4"

    # author = "qutip"
    # repo_name = "qutip"
    # tag = "v5.0.4"
    project_data = dict(author=author, repo_name=repo_name, tag=tag)

    path = checkout_tag(**project_data)
    project_path = Path(path)

    df = DataFrame()

    for dirpath, dirnames, filenames in os.walk(project_path):
        if ".git" in dirpath:
            continue
        for filename in filenames:
            file_path = Path(dirpath) / filename
            if not file_path.suffix == ".py" or file_path.name.startswith("."):
                continue

            print("Parsing", filename)
            df = pd.concat([df, DataFrame(parse_ast(project_path, file_path, project_data))], ignore_index=True)

    df.to_csv(f"metadata/ast/no_types_{author}_{repo_name}_{tag}.csv", index=False)


if __name__ == "__main__":
    main()
