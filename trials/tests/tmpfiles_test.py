"""
learned about tmpfile module from:
https://livebook.manning.com/book/data-pipelines-with-apache-airflow/chapter-9/93
"""
import os
from pathlib import Path


def test_tmppath(tmp_path: Path):
    print(f"tmp_path: {tmp_path.as_posix()}")


def test_tmpdir(tmpdir: os.path):
    print(f"tmp_dir: {tmpdir.dirname}")
