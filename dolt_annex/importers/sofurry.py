#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import magic
import os

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
            case "video/webm":
                return "webm"
            case "audio/x-wav":
                return "wav"
            case "image/webp":
                return "webp"
            case "video/x-m4v":
                return "m4v"
            case "video/quicktime":
                return "mov"
            case "video/3gpp":
                return "3gp"
            case "audio/x-m4a":
                return "m4a"
            case "application/x-shockwave-flash":
                return "swf"
            case "text/x-c":
                # false positive
                return "txt"
            case "application/javascript":
                # false positive
                return "txt"
            case "text/x-makefile":
                # false
                return "txt"
            case "text/csv":
                # false
                return "txt"
            case x:
                raise Exception(f"Unknown mime type {x} in {path}")
    
    def skip(self, path: str) -> bool:
        mime = magic.from_file(path, mime=True)
        if mime == 'text/html' or mime == 'inode/x-empty':
            return True
        return os.path.getsize(path) == 1
