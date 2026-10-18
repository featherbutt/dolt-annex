import pathlib
import uuid

import pytest

from dolt_annex.config.base import Config
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.file_keys.base import Sha256HSe
from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.filestore.memory import MemoryFSModel

def test_repo_with_named_filestore():

    AnnexFSModel(
        name="test_filestore",
        root=pathlib.Path("/tmp/test_repo"),
    )

    repo_with_named_filestore = RepoModel(
        uuid=uuid.uuid4(),
        filestore="test_filestore",
    )

    assert repo_with_named_filestore.filestore.name == "test_filestore"
    assert isinstance(repo_with_named_filestore.filestore, AnnexFSModel)
    assert repo_with_named_filestore.filestore.root == pathlib.Path("/tmp/test_repo")

    dumped_repo = repo_with_named_filestore.model_dump()
    assert dumped_repo["filestore"] == "test_filestore"

def test_repo_with_emedded_filestore():

    embedded_filestore = AnnexFSModel(
        root=pathlib.Path("/tmp/test_repo"),
    )
    
    repo_with_embedded_filestore = RepoModel(
        uuid=uuid.uuid4(),
        filestore=embedded_filestore,
        key_format=Sha256HSe,
)

    assert isinstance(repo_with_embedded_filestore.filestore, AnnexFSModel)
    assert repo_with_embedded_filestore.filestore.root == pathlib.Path("/tmp/test_repo")

    dumped_repo = repo_with_embedded_filestore.model_dump()
    assert dumped_repo["filestore"] == embedded_filestore.model_dump()

def test_repo_uuid_clashes_with_embedded_filestore():

    test_uuid = uuid.uuid4()

    RepoModel(
        uuid=uuid.uuid4(),
        name="test_filestore",
        filestore=MemoryFSModel(
            name="test_filestore",
            uuid=test_uuid,
        ),
    )

    with pytest.RaisesGroup(pytest.RaisesExc(ValueError, match="UUID"), flatten_subgroups=True, allow_unwrapped=True):
        RepoModel.open(Config(), "test_filestore")

def test_repo_uuid_clashes_with_named_filestore():

    test_uuid = uuid.uuid4()

    MemoryFSModel(
        name="test_filestore",
        uuid=test_uuid,
    )

    RepoModel(
        uuid=uuid.uuid4(),
        name="test_filestore",
        filestore="test_filestore",
    )

    with pytest.RaisesGroup(pytest.RaisesExc(ValueError, match="UUID"), flatten_subgroups=True, allow_unwrapped=True):
        RepoModel.open(Config(), "test_filestore")

def test_repo_name_clashes_with_embedded_filestore():

    test_uuid = uuid.uuid4()

    RepoModel(
        uuid=uuid.uuid4(),
        name="test_filestore",
        filestore=MemoryFSModel(
            name="other_name",
            uuid=test_uuid,
        ),
    )

    with pytest.RaisesGroup(pytest.RaisesExc(ValueError, match="name"), flatten_subgroups=True, allow_unwrapped=True):
        RepoModel.open(Config(), "test_filestore")

def test_repo_name_clashes_with_named_filestore():

    test_uuid = uuid.uuid4()

    MemoryFSModel(
        name="other_name",
        uuid=test_uuid,
    )

    RepoModel(
        uuid=uuid.uuid4(),
        name="test_filestore",
        filestore="other_name",
    )

    with pytest.RaisesGroup(pytest.RaisesExc(ValueError, match="name"), flatten_subgroups=True, allow_unwrapped=True):
        RepoModel.open(Config(), "test_filestore")

def test_repo_uuid_clashes_with_same_named_filestore():

    MemoryFSModel(
        uuid=uuid.uuid4(),
        name="test_filestore"
    )

    test_uuid = uuid.uuid4()
    RepoModel(
        uuid=test_uuid,
        name="test_filestore",
        filestore=MemoryFSModel(),
    )

    with pytest.RaisesGroup(pytest.RaisesExc(ValueError, match="UUID"), flatten_subgroups=True, allow_unwrapped=True):
        RepoModel.open(Config(), "test_filestore")

def test_no_loadable_repo():

    MemoryFSModel(
        uuid=uuid.uuid4(),
        name="test_filestore"
    )

    repo_model = RepoModel.open(Config(), "test_filestore")
    assert repo_model.name == "test_filestore"
    assert repo_model.uuid == repo_model.filestore.uuid
