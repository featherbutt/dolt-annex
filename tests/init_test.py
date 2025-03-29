#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from tests.setup import setup_file_remote

def test_init(tmp_path):
    """Test that the repository is initialized correctly"""
    setup_file_remote(tmp_path)
    assert os.path.exists("dolt")
    assert os.path.exists("git")
    assert os.path.exists("dolt/.dolt")
    assert os.path.exists("git/annex")
