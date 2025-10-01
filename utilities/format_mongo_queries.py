from pathlib import Path
import re

from constants.abs_paths import AbsPaths


def update_query(query: str):
    query = re.sub(r'db.getCollection\(".+"\).aggregate\(', '', query)
    query = re.sub(r'^]\)$', ']', query, flags=re.MULTILINE)
    query = re.sub(r'(?<!")\$\w+', r'"\g<0>"', query)
    query = re.sub(r'(?<=regex:) ?/(.+?)/\w*', r" r'\g<1>'", query)
    query = re.sub(r'(?<=\s)(?<!")\$?\w{2,}(?!=")', r'"\g<0>"', query)
    query = re.sub(r'"(true|false)"', lambda m: m.group(1).capitalize(), query)
    return query


def main():
    queries = (AbsPaths.QUERIES / "mongo").glob("*.js")
    for query in queries:
        with open(query, "r") as f:
            content = f.read()
        with open(query.with_suffix(".formatted"), "w") as f:
            f.write(update_query(content))

if __name__ == "__main__":
    main()