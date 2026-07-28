from typing import List, Optional


class Application:
    executable: str
    
    @classmethod
    def subcommand(cls, name: str, subapp: type["Application"]):
        ...

    @classmethod
    def run(
        cls,
        argv: Optional[List[str]] = None,
        exit=True,  # pylint: disable=redefined-builtin
    ):
        ...

    def help(self):
        """Prints this help message and quits"""