# Validate that example configuration files are correct by
# - loading them and seeing that they validate and match
# - regenerating them produces the same output as the original file

import contextlib
import os
import pathlib
import uuid

from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.sftp import SftpFileStoreModel
import pyjson5

from dolt_annex.config.base import Config, UserConfig, DoltConfig, SshSettings
from dolt_annex.config.connection import MySQLConnection, SSHConnection
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.file_keys import Sha256E

def test_example_config():
    example_config = Config(
        user = UserConfig(
            name="A U Thor",
            email="author@example.com",
        ),
        dolt = DoltConfig(
            default_remote="origin",
            default_commit_message="update",
            connection=MySQLConnection(
                user="root",
                hostname="localhost",
                port=3306,
                server_socket=pathlib.Path("/tmp/dolt.sock"),
                database="dolt",
            ),
            spawn_dolt_server=True,
        ),
        ssh = SshSettings(
            ssh_config=pathlib.Path("~/.ssh/config"),
            known_hosts=pathlib.Path("~/.ssh/known_hosts"),
        ),
        local_repo="__local__",
        default_annex_remote="origin",
        default_file_key_type=Sha256E,
    )

    example_local_repo = AnnexFSModel(
        name = "__local__",
        uuid = uuid.UUID("fedcba98-7654-3210-ca11-8675309abcde"),

        root=pathlib.Path("./annex"),
    )

    example_remote_repo = SftpFileStoreModel(
        name = "sftp",
        uuid = uuid.UUID("12345678-9abc-def0-cafe-cba987654321"),

        connection=SSHConnection(
            user="your_username",
            hostname="sftp.example.com",
            port=22,
            client_key=pathlib.Path("~/.ssh/id_rsa"),
            path=pathlib.Path("."),
        )
    )

    example_dataset_schema = DatasetSchema(
        name="file_archive",
        
        tables = [
            FileTableSchema(
                name="files",
                file_column="file_key",
                key_columns=["path"],
            ),
        ],
        empty_table_ref = "file_archive"
    )

    with contextlib.chdir(os.path.dirname(__file__)):
        with open("example_config.json5", encoding="utf-8") as f:
            loaded_config = Config.model_validate(pyjson5.decode(f.read()))
            assert loaded_config == example_config
        
        with open("filestores/__local__.filestore", encoding="utf-8") as f:
            loaded_local_repo = FileStoreModel.model_validate(pyjson5.decode(f.read()))
            assert loaded_local_repo == example_local_repo

        with open("filestores/sftp.filestore", encoding="utf-8") as f:
            loaded_remote_repo = FileStoreModel.model_validate(pyjson5.decode(f.read()))
            assert loaded_remote_repo == example_remote_repo

        with open("file_archive.dataset", encoding="utf-8") as f:
            loaded_file_archive = DatasetSchema.model_validate(pyjson5.decode(f.read()))
            assert loaded_file_archive == example_dataset_schema
        