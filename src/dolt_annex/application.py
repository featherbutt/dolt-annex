#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing_extensions import Literal

from pydantic import ValidationError
import pyjson5

from dolt_annex.commands import BaseApplication
from dolt_annex.datatypes.config import Config

default_config_file_locations = [
    Path("config.json5"),
    Path("config.json"),
]

class Application(BaseApplication):
    def main(self, *args) -> Literal[0, 1]:
        # Set each config parameter in order of preference:
        # 1. Command line argument
        # 2. environment variable
        # 3. Existing config file passed in with -c
        # 4. Existing config file in default location
        # 5. Default value
        if self.config_file is not None:
            config_file_locations = [Path(self.config_file)]
        else:
            config_file_locations = default_config_file_locations
        for config_path in config_file_locations:
            if config_path.exists():
                with open(config_path, encoding="utf-8") as fd:
                    config_json = pyjson5.load(fd) # type: ignore
                try:
                    self.config = Config(**config_json)
                except ValidationError as e:
                    print(e)
                    return 1
                break
        else:
            self.config = Config()

        logging.basicConfig(level=self.log_level.upper())

        if args:
            print(f"Unknown command: dolt-annex {args[0]}")
            return 1
        
        if self.nested_command is None:
            self.help()

        return 0
