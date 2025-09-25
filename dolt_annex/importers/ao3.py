#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from typing_extensions import List, Optional

from bs4 import BeautifulSoup

import annex
from .base import ImporterBase

class AO3Importer(ImporterBase):
    def url(self, abs_path: str, rel_path: str) -> List[str]:
        stem = Path(rel_path).stem
        num = stem.split()[0]
        return [f"https://archiveofourown.org/works/{num}"]

    def getDate(self, soup):

        tags = soup.find(class_ = "tags")

        strings = tags.stripped_strings

        def getData(strings):
            for s in strings:
                for line in s.split('\n'):
                    splits = line.split(': ')
                    if len(splits) == 2:
                        yield splits

        data = {key: value for [key, value] in getData(strings)}

        if "Completed" in data:
            date = data["Completed"]
        elif "Updated" in data:
            date = data["Updated"]
        else:
            date = data["Published"]

        return date

    def submission_id(self, abs_path: str, rel_path: str) -> Optional[annex.SubmissionId]:
        stem = Path(rel_path).stem
        stem = stem.split()[0]
        with open(abs_path, encoding="utf-8") as fd:
            soup = BeautifulSoup(fd.read(), 'html.parser')
        try:
            updated = self.getDate(soup)
        except AttributeError:
            return None
        part = 1
        return annex.SubmissionId("archiveofourown.org", int(stem), updated, part)

    def md5(self, path: str) -> str | None:
        return None
    
    def skip(self, path: str) -> bool:
        return Path(path).stem == 'images'
