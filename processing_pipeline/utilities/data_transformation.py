import gc
from pathlib import Path
from typing import List

import pandas as pd
import pyarrow.dataset as ds


def clean_dir(out_dir):
    for filepath in out_dir.iterdir():
        Path(filepath).unlink()


def split_dataset_by_repo_and_source(out_dir: Path, df: pd.DataFrame, *, clean_before_saving=False, keep_index=False,
                                     drop_columns_before_save: List[str] = None):
    drop_columns_before_save = drop_columns_before_save or []

    df['repo_id'] = df['repo_id'].astype('category')
    df['source'] = df['source'].astype('category')

    out_dir.mkdir(exist_ok=True)
    if clean_before_saving:
        clean_dir(out_dir)

    grouped = df.groupby(['repo_id', 'source'], observed=True)

    for (repo_id, source), group_df in grouped:
        output_file = out_dir / f"{repo_id.replace('/', '.')}.{source}.parquet"
        print(output_file)

        if drop_columns_before_save:
            group_df = group_df.drop(columns=drop_columns_before_save, errors='ignore')

        group_df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=keep_index)

        print(f"Saved {output_file}")

    # It can be helpful to manually trigger garbage collection after a large operation
    gc.collect()


def load_all_files(in_dir: Path, *, name_contains: str = None, columns=None):
    files = [file_path for file_path in in_dir.glob("*.parquet") if
             not name_contains or (name_contains in str(file_path))]
    try:
        # 1. Create a dataset object (this is fast, it only reads metadata)
        dataset = ds.dataset(files, format="parquet")
        df = dataset.to_table(columns=columns).to_pandas() if columns else dataset.to_table().to_pandas()

        print(f"Loaded {len(dataset.files)} files, {files}")
        return df
    except Exception as e:
        print(f"Error while loading dataset: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def load_all_csv_files(in_dir: Path, *, name_contains: str = None):
    dfs = []
    for file_path in in_dir.glob("*.csv"):
        if name_contains and not name_contains in str(file_path):
            continue
        try:
            file = pd.read_csv(file_path)
            # file['fname'] = file_path
            dfs.append(file)
            print(f"Loaded {file_path}")
        except:
            print(f"Error while loading {file_path}")

    df = pd.concat(dfs)
    return df
