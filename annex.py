# Description: This file contains functions for interacting with git-annex

from typing import List, Set

# reserved git-annex UUID for the web special remote
WEB_UUID = '00000000-0000-0000-0000-000000000001'

def parse_log_file(content: str) -> Set[str]:
    """Parse a .log file and return a set of UUIDs that have the file"""
    uuids = set()
    for line in content.splitlines():
        if not line.strip():
            continue
        try:
            timestamp, present, uuid = line.split()
            if present == "1" and uuid != WEB_UUID:
                uuids.add(uuid)
        except ValueError:
            print(f"Warning: malformed log line: {line}")
    return uuids

def parse_web_log(content: str) -> List[str]:
    """Parse a .log.web file and return a list of URLs"""
    urls = []
    for line in content.splitlines():
        if not line.strip():
            continue
        try:
            timestamp, present, url = line.split(maxsplit=2)
            if present == "1":
                urls.append(url)
        except ValueError:
            print(f"Warning: malformed web log line: {line}")
    return urls

