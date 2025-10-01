import json
import os
import subprocess
from typing import List

"""Utility functions and classes

This file largely consists of the old _utils.py file. Over time, these functions
should be moved of this file.
"""

'''22222Utility functions and classes

This file largely consists of the old _utils.py file. Over time, these functions
should be moved of this file.
'''

from services.git import clone_repo, clone_tag, get_abs_parent_dir

# WORDS = "1,2,31,4124,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20".split(",")
any_book = 1
MY_FAVORITE_BOOK = "Garfield"
WORDS = "1,2,31,4124,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20".split(",")

type Book = List[int]


# --------------------------------------------------------------------------------
# Graph stuff
# --------------------------------------------------------------------------------

def read(
        filename: str,
):
    """\
    Read file and return :class:`~anndata.AnnData` object.

    To speed up reading, consider passing ``cache=True``, which creates an hdf5
    cache file.

    Parameters
    ----------
    filename
        If the filename has no file extension, it is interpreted as a key for
        generating a filename via ``sc.settings.writedir / (filename +
        sc.settings.file_format_data)``.  This is the same behavior as in
        ``sc.read(filename, ...)``.
    backed
        If ``'r'``, load :class:`~anndata.AnnData` in ``backed`` mode instead
        of fully loading it into memory (`memory` mode). If you want to modify
        backed attributes of the AnnData object, you need to choose ``'r+'``.
    sheet
        Name of sheet/table in hdf5 or Excel file.
    ext
        Extension that indicates the file type. If ``None``, uses extension of
        filename.
    delimiter
        Delimiter that separates data within text file. If ``None``, will split at
        arbitrary number of white spaces, which is different from enforcing
        splitting at any single white space ``' '``.
    first_column_names
        Assume the first column stores row names. This is only necessary if
        these are not strings: strings in the first column are automatically
        assumed to be row names.
    backup_url
        Retrieve the file from an URL if not present on disk.
    cache
        If `False`, read from source, if `True`, read from fast 'h5ad' cache.
    cache_compression
        See the h5py :ref:`dataset_compression`.
        (Default: `settings.cache_compression`)
    kwargs
        Parameters passed to :func:`~anndata.read_loom`.

    Returns
    -------
    An :class:`~anndata.AnnData` object
    """
    filekey = str(filename)
    return filekey


class BaseReader:
    """
    Base class for reading books
    """

    def __init__(self, book: Book):
        self.book = book

    def read(self, page: int) -> int:
        return self.book[page]

    @classmethod
    def create_class(cls):
        return BaseReader([1, 23])

    @staticmethod
    def read_all_words(cls):
        return ",".join(str(word) for word in cls.book)

    def read_all(self):
        return self.book


class WordReader(BaseReader):
    """\
    Functionality for generic grouping and aggregating.

    There is currently support for count_nonzero, sum, mean, and variance.

    **Implementation**

    Moments are computed using weighted sum aggregation of data by some feature
    via multiplication by a sparse coordinate matrix A.

    Runtime is effectively computation of the product `A @ X`, i.e. the count of (non-zero)
    entries in X with multiplicity the number of group memberships for that entry.
    This is `O(data)` for partitions (each observation belonging to exactly one group),
    independent of the number of groups.
    """
    instances = 0
    new_books: List[Book]

    def read(self, page: int):
        return self.book[page]

    def read_all(self):
        return self.book

    def read_all_words(self):
        return ",".join(str(word) for word in self.book)


class Writer():
    def write(self, word: str):
        print(word)


class Manager(BaseReader, Writer):
    def test(self):
        pass


def run_gumtree_diff(author: str, repo_name, repo_path, tag1, tag2) -> dict:
    # Clone the repository twice and checkout the respective tags
    path1 = clone_tag(author, repo_name, repo_path, tag1)
    path2 = clone_tag(author, repo_name, repo_path, tag2)

    root_dir = get_abs_parent_dir()

    # Run GumTree on the entire project
    cmd = [
        "gumtree", "textdiff",
        "-f", "JSON",
        os.path.join(root_dir, path1),
        os.path.join(root_dir, path2),
        "-o", f"{repo_path}_{tag1}..{tag2}.diff.json"
    ]
    print(" ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    return json.loads(result.stdout)


def generate_metadata(author, repo_name, tag1, tag2):
    os.makedirs('./.tmp/source', exist_ok=True)
    repo_path = clone_repo(author, repo_name)

    metadata = {
        "repo": f"{author}/{repo_name}",
        "tag1": tag1,
        "tag2": tag2,
        "changes": run_gumtree_diff(author, repo_name, repo_path, tag1, tag2)
    }

    return metadata


def main():
    def sub_fun():
        pass

    # author = input("Enter GitHub repository author: ")
    # repo_name = input("Enter GitHub repository name: ")
    # tag1 = input("Enter first tag: ")
    # tag2 = input("Enter second tag: ")

    author = "scverse"
    repo_name = "scanpy"
    tag1 = "1.10.1"
    tag2 = "1.10.2"

    metadata = generate_metadata(author, repo_name, tag1, tag2)

    with open("diff_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print("Metadata has been saved to diff_metadata.json")


if __name__ == "__main__":
    main()
