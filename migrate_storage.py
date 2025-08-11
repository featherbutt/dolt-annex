# Since we're not using git-annex anymore, we don't need to mirror their storage backend.
# Giving each file its own directory was a bad idea anyway.

import os
import sys

_, inDir = sys.argv

i = 0
for root, dirs, files in os.walk(inDir):
    for file in files:
        i += 1
        if i % 10 == 0:
            exit(0)
        print(f"Checking {file} in {root}")
        if root.split('/')[-1] == file:
            # Move the file to the parent directory
            print(f"Moving {file} from {root} to {os.path.join(root, '..', file)}")
            # os.rename(os.path.join(root, file), os.path.join(root, '..', f"{file}.tmp"))
            # Remove the empty directory
            print(f"Removing {root}")
            # os.rmdir(root)
            print(f"Renaming {os.path.join(root, '..', f'{file}.tmp')} to {os.path.join(root, '..', file)}")
            #os.rename(os.path.join(root, '..', f"{file}.tmp"), os.path.join(root, '..', file))
            exit(0)
