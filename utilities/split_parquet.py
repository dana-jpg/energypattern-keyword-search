import math
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from constants.abs_paths import AbsDirPath
from processing_pipeline.model.ParquetDFHandler import ParquetDFHandler
from utilities.csv.split_file_into_n_parts import split_file_in_batches, split_file_in_seq_batches, \
    split_parquet_in_seq_batches_memory_safe

MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024


def split_file(file_path: Path, size_bytes=MAX_FILE_SIZE_BYTES / 2):
    content = pd.read_parquet(file_path)
    file_size = file_path.stat().st_size
    num_rows = content.shape[0]
    num_chunks = math.ceil(file_size / size_bytes)
    print(f"Splitting file {file_path} of size {file_size} into {num_chunks} chunks")
    chunk_size_rows = math.ceil(num_rows / num_chunks)
    chunks = [content.iloc[ran] for ran in grouper_ranges(num_rows, chunk_size_rows)]
    n_chunks = len(chunks)
    for i, chunk in enumerate(chunks):
        chunk.to_parquet(file_path.with_stem(f"{file_path.stem}.{i:0{len(str(n_chunks))}}"), engine='pyarrow',
                         compression='snappy', index=False)


def split_files_exceeding_max_limit(dir, size_limit=MAX_FILE_SIZE_BYTES):
    for file_path in Path(dir).glob("*[A-Z].parquet"):
        size = file_path.stat().st_size
        if size > size_limit:
            print(f"File size: {file_path} | {size}")
            split_file_in_batches(file_path, ParquetDFHandler(), 1500)


def split_big_files_into_seq_batches(dir, rows_limit: int=2000):
    for file_path in Path(dir).glob("*[A-Z].parquet"):
        num_rows = pq.ParquetFile(file_path).metadata.num_rows
        if num_rows > rows_limit:
            print(f"{file_path}: # rows {num_rows}")
            split_parquet_in_seq_batches_memory_safe(file_path, rows_limit)


def grouper_ranges(total_size, chunk_size):
    return (range(i, min(i + chunk_size, total_size)) for i in range(0, total_size, chunk_size))


if __name__ == "__main__":
    split_files_exceeding_max_limit(AbsDirPath.O_KEYWORDS_MATCHING)
