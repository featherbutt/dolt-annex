import pathlib
from InquirerPy.base.control import Choice


from dolt_annex.config import open_json
from dolt_annex.constants import PROGNAME

import logging

from dolt_annex.util.prompts import choices
logger = logging.getLogger(__name__)

gallery_dl_config_file_location = pathlib.Path("gallery-dl.conf")

async def ensure_gallery_dl_config():
    gallery_dl_config_file_location.touch(exist_ok=True)
    async with open_json(gallery_dl_config_file_location) as config:
        if "extractor" in config:
            return

        extractor = {}
        config["extractor"] = extractor

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

        print(f"Created gallery-dl configuration at {gallery_dl_config_file_location}")
