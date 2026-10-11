from pathlib import Path
from typing import Optional

import pyjson5

from dolt_annex.datatypes.config import Config, default_config_file_locations
from dolt_annex.interactive.ensure_database import ensure_database

import logging
logger = logging.getLogger(__name__)

def load_config_from_file(config_file_path: Path) -> Config:
    with open(config_file_path, "r") as f:
        result = Config(**pyjson5.load(f))
    print(f"Loaded existing configuration from {config_file_path}.")
    return result

async def ensure_config(config_file: Optional[str], repo_name: str) -> Config:
    config_file_path: Optional[Path] = None
    if not config_file:
        # No explicit path was provided, check the default locations
        for default_config_file in default_config_file_locations:
            if default_config_file.exists():
                return load_config_from_file(default_config_file)
        return await make_config(default_config_file_locations[0], repo_name)
    
    config_file_path = Path(config_file)
    if config_file_path.exists():
        with open(config_file_path, "r") as f:
            result = Config(**pyjson5.load(f))
        print(f"Loaded existing configuration from {config_file}.")
        return result
    else:
        print(f"No existing configuration found at {config_file}. Creating a new configuration.")
        return await make_config(config_file_path, repo_name)
        

async def make_config(config_file_path: Path, repo_name: str) -> Config:
    dolt_config = await ensure_database()
    config = Config(
        dolt=dolt_config,
        local_repo=repo_name,
    )
    with open(config_file_path, "w", encoding="utf-8") as f:
        f.write(config.model_dump_json(ensure_ascii=False, indent=4))
    print(f"Created configuration at {config_file_path}")
    return config


    
