from pathlib import Path

import pandas as pd

from processing_pipeline.model.IDFHandler import IDfHandler


class CSVDFHandler(IDfHandler):
    file_ext = ".csv"

    def _write_df(self, df: pd.DataFrame, res_filepath: Path):
        df.to_csv(res_filepath, index=False)

    def _read_df(self, file_path: Path):
        return pd.read_csv(file_path)
