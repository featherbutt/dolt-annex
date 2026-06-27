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

class Flag(SwitchAttr):
    """A specialized :class:`SwitchAttr <plumbum.cli.SwitchAttr>` for boolean flags. If the flag is not
    given, the value of this attribute is ``default``; if it is given, the value changes
    to ``not default``. Usage::

        class MyApp(Application):
            verbose = Flag(["-v", "--verbose"], help = "If given, I'll be very talkative")

    :param names: The switch names
    :param default: The attribute's initial value (``False`` by default)
    :param kwargs: Any of the keyword arguments accepted by :func:`switch <plumbum.cli.switch>`,
                   except for ``list`` and ``argtype``.
    """

    def __init__(self, names, default=False, **kwargs):
        ...


ExistingDirectory = str

ExistingFile = str

NonexistentPath = str

def switch(
    names,
    argtype=None,
    argname=None,
    list=False,  # pylint: disable=redefined-builtin
    mandatory=False,
    requires=(),
    excludes=(),
    help=None,  # pylint: disable=redefined-builtin
    overridable=False,
    group="Switches",
    envname=None,
):
    ...