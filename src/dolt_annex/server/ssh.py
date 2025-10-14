#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import pathlib
from cryptography.hazmat.primitives import hashes, serialization
from typing_extensions import Optional, override

import paramiko # type: ignore

from dolt_annex.logger import logger

CHUNK_SIZE = 4096

def get_public_key_fingerprint(public_key: serialization.SSHPublicKeyTypes) -> str:
    """
    Get the fingerprint of a public key in OpenSSH format.
    This is used to identify the key in the authorized_keys file.
    """
    formatted = public_key.public_bytes(
        encoding= serialization.Encoding.OpenSSH,
        format= serialization.PublicFormat.OpenSSH,
    )
    parts = formatted.split(b" ")
    key_bytes = base64.b64decode(parts[1])

    digest = hashes.Hash(hashes.SHA256())
    digest.update(key_bytes)
    fingerprint = base64.b64encode(digest.finalize()).rstrip(b"=").decode("utf-8")

    return f"SHA256:{fingerprint}"

def load_public_key(path: pathlib.Path) -> serialization.SSHPublicKeyTypes:
    """
    Load a public key from a file, in either OpenSSH format or PEM format.
    """
    data = path.read_bytes()
    try:
        return serialization.load_ssh_public_key(data)
    # Fall back on legacy PEM type
    except ValueError:
        return serialization.load_pem_public_key(data) # type: ignore

class AnnexSshServer(paramiko.ServerInterface):
    """
    A custom SSH server for running git-shell commands.
    When AnnexSftpServer is added as a subsystem, this server can handle all commands necessary for
    a remote server, while still restricting the user from running arbitrary commands.
    """

    server_key: Optional[paramiko.PKey]
    authorized_fingerprints: list[str]

    def __init__(self, key: paramiko.PKey, authorized_keys_dir=None):
        super().__init__()
        self.authorized_fingerprints = []
        self.server_key = key

        if authorized_keys_dir is None:
            logger.warning("No authorized keys directory provided. No keys will be accepted.")
        else:
            logger.info(f"Loading authorized keys from {authorized_keys_dir}")
            for keyfile in pathlib.Path(authorized_keys_dir).glob("*.pub"):
                try:
                    authorized_key = load_public_key(keyfile)
                    fingerprint = get_public_key_fingerprint(authorized_key)
                    logger.verbose(f"Loaded key {keyfile} with fingerprint {fingerprint}")
                    if fingerprint in self.authorized_fingerprints:
                        logger.warning(f"Duplicate key found: {fingerprint}")
                        continue
                    self.authorized_fingerprints.append(fingerprint)
                except ValueError as e:
                    logger.info(f"Failed to load key {keyfile}: {e}")

    @override
    def check_auth_publickey(self, username, key: paramiko.PKey):
        if key.fingerprint in self.authorized_fingerprints:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    @override
    def check_channel_request(self, kind, chanid):
        if kind != "session":
            return paramiko.OPEN_UNKNOWN_CHANNEL_TYPE # type: ignore
        return paramiko.OPEN_SUCCEEDED
    
    @override
    def get_allowed_auths(self, username):
        """List availble auth mechanisms."""
        return "none"
    
    @override
    def check_channel_exec_request(self, channel: paramiko.Channel, command):
        return False
