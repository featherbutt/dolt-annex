import hashlib
from pathlib import Path
from typing_extensions import Optional

from dolt_annex.datatypes import AnnexKey

def key_from_file(key_path: Path, extension: Optional[str] = None) -> AnnexKey:
    """Generate an AnnexKey from the hash of a file."""
    if extension is None:
        extension = key_path.suffix[1:]  # Get the file extension without the dot
    with open(key_path, 'rb') as f:
        data = f.read()
    data_hash = hashlib.sha256(data).hexdigest()
    return AnnexKey(f"SHA256E-s{len(data)}--{data_hash}.{extension}")
