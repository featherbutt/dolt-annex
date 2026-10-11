from pathlib import Path
import uuid

from InquirerPy.inquirer import select, text, confirm # pyright: ignore[reportPrivateImportUsage]
from typing import cast
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator

from dolt_annex.datatypes.common import SSHConnection
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.filestore.archivefs import ArchiveFSModel
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.sftp import SftpFileStoreModel
from dolt_annex.filestore.sqlite import SQLiteModel
from dolt_annex.filestore.unionfs import UnionFSModel

import logging
logger =logging.getLogger(__name__)

class FileStoreSetupError(Exception):
    pass

async def ensure_repo(config: Config, repo_name: str) -> RepoModel:
    if (repo := config.maybe_get_default_repo()) is not None:
        return repo
    filestore = await ensure_filestore(config, repo_name)
    repo = RepoModel(
        name=repo_name,
        uuid=filestore.uuid,
        filestore=filestore,
        alternate_key_formats=[config.default_file_key_type] + config.default_alternate_key_types,
        )
    repo.save()
    return repo

async def ensure_filestore(config: Config, filestore_name: str) -> FileStoreModel:
    if (filestore := FileStoreModel.load(filestore_name)) is not None:
        return filestore
    
    filestore: FileStoreModel = await (await select(
        message=f"""Choose a download strategy:

Local Only - Add files to the local filestore without checking if they already exist remotely. Use this to ensure that downloaded files exist locally.

Remote Only - Add files to the remote filestore without storing them locally. Use this to ensure that downloaded files exist remotely.

Local AND Remote - Always add files to both filestores. Use this to ensure that downloaded files exist both locally and remotely.

Local OR Remote - Add files locally, but skip files that already exist in either location. Use this to ensure that downloaded files have at least one copy while avoiding unnecessary downloads.

Local AND/OR Remote - Add files to both local and remote filestores, but skip files that already exist in either location. Use this to ensure that the remote has a copy while both avoiding unnecessary downloads and maintaining copies of files that are downloaded. 
""",
        choices=[
            Choice(name="Local Only", value=make_new_local_filestore),
            Choice(name="Remote Only", value=make_new_remote_filestore),
            Choice(name="Local AND Remote", value=make_new_local_and_remote_filestore),
            Choice(name="Local OR Remote", value=make_new_local_or_remote_filestore),
            Separator("Local AND/OR Remote (Not Implmemented)"),
            ]
    ).execute_async())(config, filestore_name) # pyright: ignore[reportOptionalCall]
    filestore.save()
    return filestore

async def make_new_remote_filestore(config: Config, filestore_name: str) -> FileStoreModel:
    while True:
        url: str = cast(str, await text(
            message="Enter the remote filestore URL (sftp://username@hostname:port/path):",
        ).execute_async())
        try:
            filestore_conn = SSHConnection.parse_url(url)
            break
        except ValueError as e:
            print(f"Error: {e}")

    while True:
        client_key = cast(str, await text(
            message="Enter the path to the client key (leave blank if not applicable):",
        ).execute_async())
        if not client_key:
            break
        client_key_path = Path(client_key)
        if not client_key_path.exists():
            proceed = cast(bool, await confirm(
                message=f"Warning: Client key path '{client_key_path}' does not exist. Proceed anyway?",
                default=False
            ).execute_async())
            if not proceed:
                continue
        filestore_conn.client_key = client_key_path
        break

    if filestore_conn.password is False:
        filestore_conn.password = cast(bool, await confirm(
            message="Is a password required for the new remote filestore (or the client key)?",
            default=True
        ).execute_async())

    filestore_model = SftpFileStoreModel(
        name=filestore_name,
        connection=filestore_conn
    )

    try:
        filestore_model.uuid = await get_remote_filestore_uuid(config, filestore_model)
    except Exception as e:
        raise FileStoreSetupError(f"Error retrieving remote filestore UUID: {e}") from e
    
    return filestore_model

async def make_new_local_filestore(config: Config, repo_name: str) -> FileStoreModel:
    filestore=ArchiveFSModel(
        name=repo_name,
        uuid=uuid.uuid4(),
        root=Path("./archivefs"),
        secondary=SQLiteModel(
            root=Path(".")
        )
    )
    return filestore

async def make_new_local_and_remote_filestore(config: Config, filestore_name: str) -> FileStoreModel:
    local_filestore = await make_new_local_filestore(config, filestore_name+"/local")
    remote_filestore = await make_new_remote_filestore(config, filestore_name+"/remote")
    union_filestore = UnionFSModel(
        name=filestore_name,
        children=[local_filestore, remote_filestore],
        write_to_all_children=True,
    )
    return union_filestore

async def make_new_local_or_remote_filestore(config: Config, filestore_name: str) -> FileStoreModel:
    local_filestore = await make_new_local_filestore(config, filestore_name+"/local")
    remote_filestore = await make_new_remote_filestore(config, filestore_name+"/remote")
    union_filestore = UnionFSModel(
        name=filestore_name,
        children=[local_filestore, remote_filestore],
        write_to_all_children=False,
    )
    return union_filestore

async def get_remote_filestore_uuid(config: Config, filestore_model: SftpFileStoreModel) -> uuid.UUID:
    async with (
        filestore_model.open(config) as filestore,
        filestore.sftp.open("uuid", "rb") as f,
    ):
        return uuid.UUID(bytes=await f.read())