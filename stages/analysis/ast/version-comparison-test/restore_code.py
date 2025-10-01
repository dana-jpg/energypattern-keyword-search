import re


def restore_code(source_code, start_pos, end_pos):
    return source_code[start_pos:end_pos]


actions = [
    {
        "action": "update-node",
        "tree": "comment:     # Clean up temporary directories [1086,1122]",
        "label": "    #"
    },
    {
        "action": "insert-node",
        "tree": "comment:     # # Clean up temporary directories [1091,1129]",
        "parent": "block [554,1198]",
        "at": 10
    },
    {
        "action": "insert-node",
        "tree": "comment:     # subprocess.run([\"rm\", \"-rf\", temp_dir1, temp_dir2]) [1131,1188]",
        "parent": "block [554,1198]",
        "at": 11
    },
    {
        "action": "delete-tree",
        "tree": "expression_statement [1124,1175]"
    },
    {
        "action": "delete-tree",
        "tree": "pair [1416,1428]"
    }
]


def main():
    with open("compare_versions.py", "r", encoding="utf-8", newline='') as f:
        source_code1 = f.read()

    with open("compare_versions2.py", "r", encoding="utf-8", newline='') as f:
        source_code2 = f.read()

    # Extracting the code snippet
    for action in actions:
        print(f"\n{action=}")
        match = re.search(r"\[([0-9]+),([0-9]+)]", action["tree"])
        if match:
            start_pos, end_pos = int(match.group(1)), int(match.group(2))
            # start_pos, end_pos = 11,22
            print(f"{start_pos=}, {end_pos=}")

            restored_comment = restore_code(source_code1, start_pos, end_pos)
            print(f"{restored_comment=}")  # Should print "# Clean up temporary directories"
            restored_comment = restore_code(source_code2, start_pos, end_pos)
            print(f"{restored_comment=}")  # Should print "# Clean up temporary directories"


if __name__ == "__main__":
    main()
