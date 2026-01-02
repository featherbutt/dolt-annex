
from plumbum import cli # type: ignore

from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.application import Application
from dolt_annex.file_keys import get_file_key_type

class Insert(cli.Application):
    """Insert a single record into a filestore. Primarily used for testing."""

    parent: Application

    file_bytes = cli.SwitchAttr(
        "--file-bytes",
        str,
        help="The bytes of the file to insert",
        mandatory = True
    )

    file_key_type = cli.SwitchAttr(
        "--file-key-type",
        str,
        help="The type of file key to use",
        default = "Sha256e",
    )

    extension = cli.SwitchAttr(
        "--extension",
        str,
        help="The file extension of the inserted record",
        default = "txt",
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, insert the record into the specified repo's filestore instead of the default repo's filestore",
    )

    async def main(self, *args) -> int:
        if args:
            print("This command does not take positional arguments")
            return 1
        base_config: Config = self.parent.config

        file_key_type = get_file_key_type(self.file_key_type)
        file_bytes = self.file_bytes.encode('utf-8')
        if self.extension == "":
            extension = None
        else:
            extension = self.extension
        key = file_key_type.from_bytes(file_bytes, extension)

        if self.repo:
            repo = Repo.must_load(self.repo)
        else:
            repo = base_config.get_default_repo()
        async with repo.filestore.open(base_config):
            await maybe_await(repo.filestore.put_file_bytes(file_bytes, key))
        print(f"Inserted file with key {key} into filestore of repo '{repo.name}'")

        return 0