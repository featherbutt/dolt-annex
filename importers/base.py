from abc import ABC as AbstractBaseClass, abstractmethod
import os

from typing_extensions import List

from plumbum import local # type: ignore

import annex

class Importer(AbstractBaseClass):
    @abstractmethod
    def url(self, abs_path: str, rel_path: str) -> List[str]:
        ...

    @abstractmethod
    def md5(self, path: str) -> str | None:
        ...

    @abstractmethod
    def skip(self, path: str) -> bool:
        ...
    
class OtherAnnexImporter(Importer):
    def __init__(self, other_annex_path: str):
        self.other_annex_path = other_annex_path

    def url(self, abs_path: str, rel_path: str) -> List[str]:
        parts = abs_path.split(os.path.sep)
        annex_object_path = '/'.join(parts[-4:-1])
        other_git = local.cmd.git["-C", self.other_annex_path]
        web_log = other_git('show', f'git-annex:{annex_object_path}.log.web', retcode=None)
        if web_log:
            return annex.parse_web_log(web_log)
        return []
    
    def md5(self, path: str) -> str | None:
        return None
    
    def skip(self, path: str) -> bool:
        return False
    
class DirectoryImporter(Importer):
    def __init__(self, prefix_url: str):
        self.prefix_url = prefix_url

    def url(self, abs_path: str, rel_path: str) -> List[str]:
        return [f"{self.prefix_url}/{rel_path}"]
    
    def md5(self, path: str) -> str | None:
        return None
    
    def skip(self, path: str) -> bool:
        return False

class FALRImporter(Importer):
    def __init__(self, dolt_sql_server, other_dolt_db: str, other_dolt_branch: str):
        self.dolt_sql_server = dolt_sql_server
        self.other_dolt_db = other_dolt_db
        self.other_dolt_branch = other_dolt_branch

    def url(self, abs_path: str, rel_path: str) -> List[str]:
        parts = abs_path.split(os.path.sep)
        sid = int(''.join(parts[-6:-1]))
        res = self.dolt_sql_server.execute(f"SELECT DISTINCT url FROM `{self.other_dolt_db}/{self.other_dolt_branch}`.filenames WHERE source = 'furaffinity.net' and id = %s;", (sid,))
        return [row[0] for row in res]
    
    def md5(self, path: str) -> str | None:
        return None
    
    def skip(self, path: str) -> bool:
        return 'thumbnail' in path.split('/')[-1]

class MD5Importer(Importer):
    def url(self, abs_path: str, rel_path: str) -> List[str]:
        basename = os.path.basename(abs_path)
        basename_parts = basename.split('.')
        if len(basename_parts) < 3:
            raise ValueError(f"Invalid filename: {basename}")
        md5, source, *rest, ext = basename_parts
        if len(md5) != 32:
            raise ValueError(f"Invalid MD5: {md5}")
        if source == "e621":
            return [f"https://static1.e621.net/data/{md5[:2]}/{md5[2:4]}/{md5}.{ext}"]
        elif source == "Gelbooru":
            return [f"https://img3.gelbooru.com/images/{md5[:2]}/{md5[2:4]}/{md5}.{ext}"]
        elif source == "rule34":
            return [f"https://r34i.paheal-cdn.net/{md5[:2]}/{md5[2:4]}/{md5}"]
        elif source == "e6ai":
            return [f"https://static1.e6ai.net/data/{md5[:2]}/{md5[2:4]}/{md5}.{ext}"]
        elif source == "danbooru.donmai.us":
            return [f"https://cdn.donmai.us/original/{md5[:2]}/{md5[2:4]}/{md5}.{ext}"]
        else:
            raise ValueError(f"Unknown source: {basename}")
    
    def md5(self, path: str) -> str | None:
        basename = os.path.basename(path)
        return basename.split('.')[0]
    
    def skip(self, path: str) -> bool:
        return False
    
class NullImporter(Importer):
    """An importer that does nothing."""
    def url(self, abs_path: str, rel_path: str) -> List[str]:
        return []
    
    def md5(self, path: str) -> str | None:
        return None
    
    def skip(self, path: str) -> bool:
        return False