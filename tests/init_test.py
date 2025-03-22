#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from tests.setup import setup

def test_init(tmp_path):
    """Test that the repository is initialized correctly"""
    setup(tmp_path)
    assert os.path.exists("dolt")
    assert os.path.exists("git")
    assert os.path.exists("dolt/.dolt")
    assert os.path.exists("git/annex")
