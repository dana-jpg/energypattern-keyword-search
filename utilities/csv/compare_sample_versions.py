import os
import traceback
from pathlib import Path

import pandas as pd


def save_data(df, target_filename: Path):
    os.makedirs(target_filename.parent, exist_ok=True)
    df.to_excel(target_filename, index=False)

def main():
    keywords_dir = Path(f"metadata/results/")
    f1 = keywords_dir / "sample_validate_format_v9__2025_04_11_16_11_05_335959.xlsx"
    f2 = keywords_dir / "sample_validate_format_v10__2025_04_13_05_31_07_366236.xlsx"
    target_filename = keywords_dir / "comparison_v9_v10.xlsx"

    try:
        df1 = pd.read_excel(f1)
        df2 = pd.read_excel(f2)
        df = pd.merge(df1, df2, on='id', how='inner', suffixes=('_v9', '_v10'))
        columns_to_keep = [col for col in df.columns if any(x in col for x in ['v9', 'to_eliminate','reason'])]
        df = df[columns_to_keep]
        df['diff'] = df['to_eliminate_v9'] != df['to_eliminate_v10']
        save_data(df[df['diff'] == True], target_filename)
    except Exception as error:
        print(f"Error processing {error=}\n{traceback.format_exc()}")


if __name__ == "__main__":
    main()
