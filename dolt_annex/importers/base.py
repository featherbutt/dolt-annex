from abc import ABC as AbstractBaseClass, abstractmethod
import importlib
from pathlib import Path
from typing_extensions import List, Optional, Type, Dict, override

from dolt_annex.datatypes import TableRow

class ImporterBase(AbstractBaseClass):

    @abstractmethod
    def key_columns(self, path: Path) -> Optional[TableRow]:
        ...

    @abstractmethod
    def table_name(self, path: Path) -> str:
        ...

    def skip(self, path: Path) -> bool:
        return False

    def extension(self, path: Path) -> str | None:
        return path.suffix[1:]  # Get the file extension without the dot
    
   

importers: Dict[str, Type[ImporterBase]] = {}

def get_importer(importerName: str, *args, **kwargs) -> ImporterBase:
    match importerName.split('.'):
        case [module_name]:
            class_name = module_name
        case [module_name, class_name]:
            pass
        case _:
            raise ImportError(f"Invalid importer name: {importerName}")
    importer_module = importlib.import_module(f"..{module_name.lower()}", package=__name__)
    if class_name in dir(importer_module):
        return getattr(importer_module, class_name)(*args, **kwargs)
    return getattr(importer_module, "Importer")(*args, **kwargs)

class DirectoryImporter(ImporterBase):
    def __init__(self, table_name: str, prefix: str = ""):
        self.prefix = prefix
        self._table_name = table_name

    @override
    def key_columns(self, path: Path) -> Optional[TableRow]:
        return TableRow((self.prefix + '/' + path.as_posix(),))
    
    @override
    def table_name(self, path: Path) -> str:
        return self._table_name

class MD5Importer(ImporterBase):
    def __init__(self, table_name: str):
        self._table_name = table_name

    def key_columns(self, path: Path):
        return (path.stem.split('.')[0],)

    def url(self, path: Path) -> List[str]:
        basename = path.name
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
    
    @override
    def table_name(self, path: Path) -> str:
        return self._table_name
    