#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from typing_extensions import List, Optional
import hashlib

import annex
from .base import Importer, declare_importer

class InkbunnyDataImporter(Importer):
    def url(self, abs_path: str, rel_path: str) -> List[str]:
        return []
    
    def submission_id(self, abs_path: str, rel_path: str) -> Optional[annex.SubmissionId]:
        return None
    
    def md5(self, path: str) -> str | None:
        with open(path, 'rb') as fd:
            return hashlib.md5(fd.read()).hexdigest()
    
    def skip(self, path: str) -> bool:
        return Path(path).suffix == '.json'

    
declare_importer(InkbunnyDataImporter)
