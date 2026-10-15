from dataclasses import dataclass
from pathlib import Path
import pathlib
import uuid

from plumbum import cli

from dolt_annex.commands.application import Application
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.config.base import Config, DoltConfig
from dolt_annex.interactive.ensure_dolt_binary import ensure_dolt_binary
from dolt_annex.interactive.ensure_database import create_dolt_directory
from dolt_annex.interactive.ensure_filestore import make_new_local_filestore

@dataclass
class InitConfig:
    init_dolt: bool # Do we need to initialize the Dolt repository?
    dolt_url: str   # Are we adding a remote Dolt repository?
    remote_name: str
    
class Init(cli.Application):
    """Initialize the Dolt and Git repositories"""

    parent: Application

    no_dolt = cli.Flag(
        "--no-dolt",
        help = "Don't initialize the Dolt repository",
    )

    dolt_url = cli.SwitchAttr(
        "--dolt-url",
        str,
        envname = "DA_DOLT_URL",
    )

    remote_name = cli.SwitchAttr(
        "--name",
        str,
        envname = "DA_REMOTE_NAME",
    )

    async def main(self, *args):
        if args:
            print("Unexpected positional arguments: ", args)
            self.help()
            return 1
        
        base_config: Config = self.parent.config

        dolt_path = pathlib.Path("./dolt")
        create_dolt_directory(dolt_path)
        dolt_binary_path = await ensure_dolt_binary()
        base_config.dolt = DoltConfig(
            spawn_dolt_server=True,
            dolt_binary=dolt_binary_path,
            dolt_dir=dolt_path,
        )

        filestore_model = await make_new_local_filestore(base_config, "__local__")
        filestore_model.save()

        repo = RepoModel(
            name="__local__",
            uuid=filestore_model.uuid or uuid.uuid4(),
            filestore=filestore_model,
            alternate_key_formats=[base_config.default_file_key_type] + base_config.default_alternate_key_types,
            )
        repo.save()
        
        path = Path("config.json")
        if not path.exists():
            with open(path, "w", encoding="utf-8") as f:
                f.write(base_config.model_dump_json(ensure_ascii=False, indent=4))
            print("Created config.json")

        return 0

