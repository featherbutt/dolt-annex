import pathlib
import uuid

from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.file_keys.base import Sha256HSe
from dolt_annex.filestore.annexfs import AnnexFSModel

def test_repo_with_named_filestore():

    named_filestore = AnnexFSModel(
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