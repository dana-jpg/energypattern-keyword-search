import json
import os
from abc import ABC, abstractmethod
from pathlib import Path


class IJSONFileCache[T](ABC):
    @classmethod
    @abstractmethod
    def _get_filename(cls) -> str:
        pass

    @classmethod
    def init(cls) -> None:
        """create cache file if it doesn't exist"""
        dirname = Path(cls._get_filename()).parent
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        if not os.path.exists(cls._get_filename()):
            with open(cls._get_filename(), "w") as f:
                f.write("{}")

    def clear(cls) -> None:
        """clear cache file"""
        with open(cls._get_filename(), "w") as f:
            f.write("{}")

    @classmethod
    def get(cls, key: str, fallback: T) -> T:
        try:
            with open(cls._get_filename(), "r") as f:
                data = cls._load_json_from_file(f)
                return data.get(key, fallback)
        except Exception as e:
            print(f"{cls.__name__}: Error in get", e)
            return fallback

    @staticmethod
    def _load_json_from_file(f) -> dict[str, T]:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}

    @classmethod
    def set(cls, key: str, value: T) -> None:
        try:
            with open(cls._get_filename(), "r+") as f:
                data = cls._load_json_from_file(f)
                data[key] = value
                f.seek(0)
                json.dump(data, f)
                f.truncate()
        except Exception as e:
            print(f"{cls.__name__}: Error in set", e)

    @classmethod
    def delete(cls, key: str) -> None:
        try:
            with open(cls._get_filename(), "r+") as f:
                data = cls._load_json_from_file(f)
                del data[key]
                f.seek(0)
                json.dump(data, f)
                f.truncate()
        except Exception as e:
            print(f"{cls.__name__}: Error in delete", e)
