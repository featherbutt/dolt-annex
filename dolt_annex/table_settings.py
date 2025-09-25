from dataclasses import dataclass
from uuid import UUID

from tables import FileKeyTable
from dolt_annex.remote import Remote


@dataclass
class TableSettings:
    uuid: UUID
    table: FileKeyTable
    remote: Remote