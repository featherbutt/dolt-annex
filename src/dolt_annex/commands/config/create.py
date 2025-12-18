import json
import types
from typing import Optional

from dolt_annex.commands import SubCommand
from dolt_annex.datatypes.loader import Loadable
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
    
class Creator[T: Loadable]:
    loadable_type: type[T]

    def __init_subclass__(cls, loadable_type: type[T]) -> None:
        cls.loadable_type = loadable_type

    def main(self, name: str, value: str) -> int:
        loadable_value = self.loadable_type(name=name, **json.loads(value))
        loadable_value.save()
        return 0

LoadableTypes: dict[str, type[Loadable]] = {
    "repo": Repo,
    "dataset": DatasetSchema,
}

class Create(SubCommand):
    """Create a new configuration object (repo, dataset, etc)."""

    nested_command: Optional[Creator]

    def main(self, *args) -> int:
        if args:
            print(f"Unknown command: create {args[0]}. Accepted values are: {', '.join(LoadableTypes.keys())}")
            return 1
        if self.nested_command is None:
            self.help()
            return 0
        return 0

for (name, loadable_type) in LoadableTypes.items():
    CreateSubcommand = types.new_class(
        f"Create{name.capitalize()}",
        (Creator[loadable_type], SubCommand),
        {"loadable_type": loadable_type}
    )
    Create.subcommand(name, CreateSubcommand)