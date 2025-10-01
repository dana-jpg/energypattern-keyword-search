import datetime
import os
import re
import traceback
from pathlib import Path

import pandas as pd

from constants.foldernames import FolderNames


def get_data(keywords_dir):
    dfs = []
    for file in keywords_dir.glob(f"*.csv"):
        try:
            print(f"reading file {file}")
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as error:
            print(f"Error reading {file}, {error=}")

    return pd.concat(dfs)

def save_data(df, target_filename: Path):
    os.makedirs(target_filename.parent, exist_ok=True)
    df.to_excel(target_filename, index=False)

def add_word_count(df, field_name = "sentence"):
    df["word_count"] = df[field_name].apply(lambda x: len(re.sub(r"[\W_]+", " ", x).strip().split()))
    return df


def main():
    keywords_dir = Path(f"metadata/keywords/{FolderNames.FORMAT_VALIDATION_DIR}")
    # target_filename = Path(f"metadata/results/sample_{FolderNames.FORMAT_VALIDATION_DIR}.csv")
    target_filename = Path(f"metadata/results/sample_{FolderNames.FORMAT_VALIDATION_DIR}__{re.sub(r"\W+", "_", str(datetime.datetime.now()))}.xlsx")

    try:
        df = get_data(keywords_dir)
        df = add_word_count(df)
        save_data(df, target_filename)
    except Exception as error:
        print(f"Error processing {error=}\n{traceback.format_exc()}")


if __name__ == "__main__":
    main()
