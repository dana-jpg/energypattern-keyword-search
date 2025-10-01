from pathlib import Path

from services.ast_extractor import read_file, parse_code, extract_tree, extract_comments


def main():
    dir = Path("tree_sitter_playground")
    file_path = dir / "test_file.py"
    # file_path = "ast-project-extractor.py"
    code, filename, lang = read_file(str(file_path))
    tree = parse_code(code, lang)

    extract_tree(tree, f"{file_path}.tree")
    comments = extract_comments(lang, tree)

    print(comments)


    # items = []
    # for el in ast_main_definitions_iterator(lang, tree):
    #     items.append(dict(filename=filename, ext=lang, **el))
    #     print(el)
    #
    # import pandas as pd
    # df = pd.DataFrame(items)
    # df.to_csv(f"{file_path}.csv", index=False)


if __name__ == "__main__":
    main()
