import asyncio
import os
from pathlib import Path
import platform

import shutil
import subprocess
import tarfile
from typing import Optional
import urllib.request
import zipfile

MIN_DOLT_VERSION = (2, 3, 5)

def get_latest_dolt_release_url(uname: platform.uname_result) -> str:
    RELEASES_BASE_URL="https://github.com/dolthub/dolt/releases/latest/download"
    system = uname.system.lower()
    machine = uname.machine.lower()
    if machine == "aarch64":
        machine = "arm64"
    if machine == "amd64":
        machine = "x86_64"
    match system, machine:
        case "linux", "x86_64":
            return f"{RELEASES_BASE_URL}/dolt-linux-amd64.tar.gz"
        case "linux", "arm64":
            return f"{RELEASES_BASE_URL}/dolt-linux-arm64.tar.gz"
        case "darwin", "x86_64":
            return f"{RELEASES_BASE_URL}/dolt-darwin-amd64.tar.gz"
        case "darwin", "arm64":
            return f"{RELEASES_BASE_URL}/dolt-darwin-arm64.tar.gz"
        case "windows", "x86_64":
            return f"{RELEASES_BASE_URL}/dolt-windows-amd64.zip"
        case _:
            raise ValueError(f"Unsupported system/machine combination: {system}/{machine}")

# If the dolt binary path is specified by the user, we use that path unconditionally.
# Otherwise, we check if an up to date version exists on the system PATH
# Finally, we fall back to the default location for dolt-annex (~/.dolt-annex).
def dolt_version_check(dolt_bin_path: Path) -> bool:
    output = subprocess.run([str(dolt_bin_path), "version"], capture_output=True).stdout
    # The first line of the output contains the version information.
    output = output.splitlines()[0]
    version = output.decode().strip().split()[-1]
    return tuple(map(int, version.split("."))) >= MIN_DOLT_VERSION

def extract_file_from_tar(tar: tarfile.TarFile, member_name: str, extract_path: Path):
        member = tar.getmember(member_name)
        member.name = os.path.basename(member.name)
        tar.extract(member, path=extract_path)

async def download_and_extract_dolt(dolt_binary: Path):
    dolt_binary.parent.mkdir(parents=True, exist_ok=True)
    dolt_release_url = get_latest_dolt_release_url(platform.uname())
    path, _ = urllib.request.urlretrieve(dolt_release_url)
    if dolt_release_url.endswith(".zip"):
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extract("bin/dolt.exe", path=dolt_binary)
            zip_ref.extract("LICENSES", path=dolt_binary.parent)
    else:
        with tarfile.open(path, "r:gz") as tar:
            root = tar.getnames()[0]
            extract_file_from_tar(tar, f"{root}/bin/dolt", dolt_binary.parent)
            extract_file_from_tar(tar, f"{root}/LICENSES", dolt_binary.parent)
    print(f"Dolt successfully downloaded and extracted to {dolt_binary}.")
    await asyncio.sleep(1)

def find_dolt_binary_on_path() -> Optional[Path]:
    dolt_binary = shutil.which("dolt")
    if not dolt_binary:
        print("Dolt binary not found on system PATH.")
        return None
    print("Found dolt binary at", dolt_binary)
    if dolt_version_check(Path(dolt_binary)):
        print("Dolt binary at", dolt_binary, "is up to date.")
        return Path(dolt_binary)
    else:
        print("Dolt binary at", dolt_binary, "is out of date.")
        return None

async def ensure_dolt_binary(dolt_binary_path: Optional[Path] = None):
    # TODO: We only need the dolt binary if we're connecting to a local Dolt repository.
    # But if we are, we need to make sure that the binary is correctly when we try to connect.
    if dolt_binary_path:
        if dolt_binary_path.exists():
            print("Using user-supplied path for dolt binary at", dolt_binary_path)
    else:
        dolt_binary_path = find_dolt_binary_on_path()
        if not dolt_binary_path:
            # No dolt binary exists, install one into the default location.
            dolt_binary_path = Path.home() / ".dolt-annex" / "bin" / "dolt"

    if not dolt_binary_path.exists():
        print(f"Dolt binary not found at {dolt_binary_path}. Downloading and extracting from GitHub...")
        await download_and_extract_dolt(dolt_binary_path)
    elif not dolt_version_check(dolt_binary_path):
        print(f"Dolt binary at {dolt_binary_path} is out of date. Downloading and extracting the latest version...")
        await download_and_extract_dolt(dolt_binary_path)
    return dolt_binary_path