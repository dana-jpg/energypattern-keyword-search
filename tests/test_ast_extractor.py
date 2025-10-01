from pathlib import Path

import pytest

from servicess.ast_extractor import code_comments_iterator


def test_code_comments_iterator_cpp():
    comments = list(code_comments_iterator(get_fixture_path("make_examples_native.cc")))
    assert """/*
 * Copyright 2024 Google LLC.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: ...
 */""" in comments
    assert '// Initialize samples.' in comments
    assert """// Initialize reference reader.
// We should always have reference name set up, except for some unit tests.
// This code will fail if reference is not set up or cannot be loaded.""" in comments
    # debug_comments(comments)

def test_code_comments_iterator_cpp_header_files():
    comments = list(code_comments_iterator(get_fixture_path("ZIP.h")))
    assert "/* for crypt.c:  include zip password functions, not unzip */" in comments
    assert """/* Maximum window size = 32K. If you are really short of memory, compile
 * with a smaller WSIZE but this reduces the compression ratio for files
 * of size > WSIZE. WSIZE must be a power of two in the current implementation.
 */""" in comments
    # debug_comments(comments)

def test_code_comments_iterator_js():
    comments = list(code_comments_iterator(get_fixture_path("EveManager.js")))
    # debug_comments(comments)
    assert '// array of receivers of highlight messages' in comments
    assert "/** Returns element with given ID */" in comments
    assert """// console.log('ArrayBuffer size ',
// msg.byteLength, 'offset', offset);""" in comments

def test_code_comments_iterator_c():
    comments = list(code_comments_iterator(get_fixture_path("fileopen.c")))
    # debug_comments(comments)
    assert """// @(#)macros:$Id$
// Author: Axel Naumann, 2008-05-22
//
// This script gets executed when double-clicking a ROOT file (currently only on Windows).
// The file that got double clicked and opened is accessible as _file0.""" in comments

def test_code_comments_iterator_c_sharp():
    comments = list(code_comments_iterator(get_fixture_path("ClangFormatPackage.cs")))
    # debug_comments(comments)
    assert '// Use MemberwiseClone to copy value types.' in comments
    assert """// Check if string contains quotes. On Windows, file names cannot contain quotes.
// We do not accept them however to avoid hard-to-debug problems.
// A quote in user input would end the parameter quote and so break the command invocation.""" in comments

def test_code_comments_iterator_python():
    comments = list(code_comments_iterator(get_fixture_path("settings.py")))
    # debug_comments(comments)
    assert '# Collected from the print_* functions in matplotlib.backends' in comments
    assert '"""Logging verbosity levels."""' in comments
    assert '''"""Verbosity level (default `warning`).

        Level 0: only show 'error' messages.
        Level 1: also show 'warning' messages.
        Level 2: also show 'info' messages.
        Level 3: also show 'hint' messages.
        Level 4: also show very detailed progress for 'debug'ging.
        """''' in comments

# TODO: add java
def test_code_comments_iterator_java():
    comments = list(code_comments_iterator(get_fixture_path("EveManager.js")))
    # debug_comments(comments)
    assert '// array of receivers of highlight messages' in comments
    assert "/** Returns element with given ID */" in comments
    assert """// console.log('ArrayBuffer size ',
// msg.byteLength, 'offset', offset);""" in comments



def debug_comments(comments):
    [print(f"\n{i=:3d}: {comment}") for i, comment in enumerate(comments)]


def get_fixture_path(name):
    return Path("./fixtures/code_samples") / name
