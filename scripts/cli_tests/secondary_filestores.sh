# Test Outline:
# - Create a Repo with an ArchiveFS filestore with a LevelDB secondary
# - Create a dataset and insert a file into the repo
# - Create a second repo with the same ArchiveFS filestore and a SQLite secondary
# - Copy just the secondary filestore from the first repo to the second repo
# - Verify that the file is still accessible in the second repo
# - Use the gallery_dl_alter_file_key_type script to change the file key type in the second repo
# - Verify that the file key has changed, but the file still has the same hash