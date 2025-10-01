import shutil

import pandas as pd
from tqdm import tqdm

from constants.abs_paths import AbsDirPath

def clone_file(file, subset_of_wikis):
    tqdm.write(f"Cloning {file}")
    subset_file = subset_of_wikis / file.name
    shutil.copyfile(file, subset_file)


def convert_to_parquet(file, subset_of_wikis):
    tqdm.write(f"Converting to parquet {file}")
    df = pd.read_csv(file)
    subset_file = (subset_of_wikis / file.name).with_suffix(".parquet")
    df.to_parquet(subset_file, engine='pyarrow', compression='snappy', index=False)

def main():
    original_dir = AbsDirPath.KEYWORDS_MATCHING
    subset_of_wikis = original_dir.parent / "matched_wikis_pq"

    subset_of_wikis.mkdir(exist_ok=True)
    for file in tqdm(original_dir.glob("*.WIKI.csv")):
        # clone(file, subset_of_wikis)
        convert_to_parquet(file, subset_of_wikis)


if __name__ == "__main__":
    main()