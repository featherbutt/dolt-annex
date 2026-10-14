import json
from pathlib import Path
from typing import Optional
from InquirerPy.base.control import Choice

from pydantic import ValidationError
import pyjson5

from dolt_annex.constants import PROGNAME
from dolt_annex.datatypes.config import Config, default_config_file_locations
from dolt_annex.datatypes.errors import ConfigurationError
from dolt_annex.datatypes.loader import validation_error
from dolt_annex.interactive.ensure_database import ensure_database

import logging

from dolt_annex.util.inquirerpy import choices
logger = logging.getLogger(__name__)

gallery_dl_config_file_location = Path("./gallery-dl.conf")

async def ensure_gallery_dl_config():
    if gallery_dl_config_file_location.exists():
        return

    extractor = {}
    config = {"extractor": extractor}

    browser = await choices(
        f"Would you like to use your cookies from a browser? This will allow {PROGNAME} to authenticate with many websites.",
        [
            Choice(name="Firefox", value="firefox"),
            Choice(name="Chrome", value="chrome"),
            Choice(name="Chromium", value="chromium"),
            Choice(name="Safari", value="safari"),
            Choice(name="Do not use cookies", value=None)
        ]
    )

    if browser:
        extractor["cookies"] = [browser]

    with open(gallery_dl_config_file_location, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)
    print(f"Created gallery-dl configuration at {gallery_dl_config_file_location}")
    return config


    
