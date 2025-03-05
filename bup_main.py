# bup_main is a C module that provides the main entry point for the bup
# command-line tool. It is implemented in bup_main.c.

# We don't depend on bup_main, but some of the bup modules try to import it.
# We define an empty module here to prevent import errors.