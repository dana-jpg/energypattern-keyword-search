import pandas as pd

from processing_pipeline.model.IDFHandler import IDfHandler


class ParquetDFHandler(IDfHandler):
    file_ext = ".parquet"

    def _write_df(self, df, res_filepath):
        df.to_parquet(res_filepath, engine='pyarrow',
                      compression='snappy', index=False)

    def _read_df(self, file_path):
        return pd.read_parquet(file_path)
