from pathlib import Path

class SwitchAttr[T]:
    """
    A switch that stores its result in an attribute (descriptor). Usage::

        class MyApp(Application):
            logfile = SwitchAttr(["-f", "--log-file"], str)

            def main(self):
                if self.logfile:
                    open(self.logfile, "w")

    :param names: The switch names
    :param argtype: The switch argument's (and attribute's) type
    :param default: The attribute's default value (``None``)
    :param argname: The switch argument's name (default is ``"VALUE"``)
    :param kwargs: Any of the keyword arguments accepted by :func:`switch <plumbum.cli.switch>`
    """
    ATTR_NAME = ...
    VALUE = ...
    def __init__(self, names, argtype: type[T], default=..., list=..., argname=..., **kwargs) -> None:
        ...
    
    def __call__(self, inst, val): # -> None:
        ...
    
    def __get__(self, inst, cls) -> T:
        ...
    
    def __set__(self, inst, val): # -> None:
        ...

ExistingDirectory = Path

MakeDirectory = Path

ExistingFile = Path

NonexistentPath = Path
