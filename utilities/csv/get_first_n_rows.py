from pathlib import Path

import pandas as pd

from constants.abs_paths import AbsDirPath

MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024


def extract_first_n_rows_from_file(file_path: Path, dir_out: Path, n):
    content = pd.read_parquet(file_path)
    content = content.head(n)
    new_name = dir_out / file_path.with_stem(f"{file_path.stem}.first_{n}").name
    content.to_parquet(new_name, compression="snappy", engine="pyarrow")


def extract_first_rows(dir_in, dir_out, names_containing, n=100):
    for file_path in Path(dir_in).glob("*[A-Z].parquet"):
        if any(name in str(file_path) for name in names_containing):
            extract_first_n_rows_from_file(file_path, dir_out, n)


if __name__ == "__main__":
    extract_first_rows(AbsDirPath.O_KEYWORDS_MATCHING, AbsDirPath.PARAMETER_TUNING_DIR,
                       ["allenai.scispacy", "google.deepvariant", "OpenGene.fastp"])

    # print(list(grouper_ranges(98, 10)))  # split_file("metadata/keywords/broadinstitute.cromwell.87.ISSUE_COMMENT.csv", 500_000)
