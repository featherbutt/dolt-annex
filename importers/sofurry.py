#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import magic

from typing_extensions import List, Optional

import annex
from .base import ImporterBase

class SoFurry(ImporterBase):
    def url(self, abs_path: str, rel_path: str) -> List[str]:
        return []
    
    def submission_id(self, abs_path: str, rel_path: str) -> Optional[annex.SubmissionId]:
        sid = int(Path(abs_path).name)
        return annex.SubmissionId("sofurry.com", sid, "0", 1)

    def md5(self, path: str) -> str | None:
        return None

    def extension(self, path: str) -> str | None:
        match magic.from_file(path, mime=True):
            case "image/png":
                return "png"
            case "image/jpeg":
                return "jpeg"
            case "text/plain":
                return "txt"
            case "image/gif":
                return "gif"
            case "audio/mpeg":
                return "mp3"
            case "video/mp4":
                return "mp4"
            case x:
                raise Exception(f"Unknown mime type {x}")
    
    def skip(self, path: str) -> bool:
        return magic.from_file(path, mime=True) == 'text/html'
