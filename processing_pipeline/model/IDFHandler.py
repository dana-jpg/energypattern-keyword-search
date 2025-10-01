from abc import ABCMeta, abstractmethod
from pathlib import Path

import pandas as pd


class IDfHandler(metaclass=ABCMeta):
    @property
    @abstractmethod
    def file_ext(self) -> str:
        pass

    def _verify_ext(self, res_filepath):
        assert res_filepath.suffix == self.file_ext, f"File extension does not match for the path {res_filepath}, expected {self.file_ext}"

    @abstractmethod
    def _write_df(self, df: pd.DataFrame, res_filepath: Path):
        pass

    def write_df(self, df: pd.DataFrame, res_filepath: Path):
        self._verify_ext(res_filepath)
        self._write_df(df, res_filepath)

    @abstractmethod
    def _read_df(self, file_path: Path):
        pass

    def read_df(self, file_path: Path):
        self._verify_ext(file_path)
        return self._read_df(file_path)
