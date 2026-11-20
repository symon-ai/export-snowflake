"""
Client-side encryption compatible with Snowflake external stages (AES-CBC file key wrapped with AES-ECB master key).
"""
import base64
import os
import tempfile
from typing import NamedTuple, Optional, Tuple

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

BLOCK_SIZE = algorithms.AES.block_size // 8
CHUNK_SIZE = 64 * 1024


class EncryptionMetadata(NamedTuple):
    key: str
    iv: str


def _pkcs5_pad(value: bytes) -> bytes:
    pad = BLOCK_SIZE - len(value) % BLOCK_SIZE
    return value + bytes([pad]) * pad


def encrypt_file(master_key: str, in_filename: str,
                 tmp_dir: Optional[str] = None) -> Tuple[EncryptionMetadata, str]:
    """Encrypt a file into tmp_dir and return the encryption metadata and encrypted file path"""
    decoded_key = base64.standard_b64decode(master_key)
    iv_data = os.urandom(BLOCK_SIZE)
    file_key = os.urandom(len(decoded_key))

    encryptor = Cipher(algorithms.AES(file_key), modes.CBC(iv_data)).encryptor()
    temp_output_fd, temp_output_file = tempfile.mkstemp(
        text=False, dir=tmp_dir, prefix=os.path.basename(in_filename) + '#'
    )
    with open(in_filename, 'rb') as infile, os.fdopen(temp_output_fd, 'wb') as outfile:
        padded = False
        while True:
            chunk = infile.read(CHUNK_SIZE)
            if not chunk:
                break
            if len(chunk) % BLOCK_SIZE != 0:
                chunk = _pkcs5_pad(chunk)
                padded = True
            outfile.write(encryptor.update(chunk))
        if not padded:
            outfile.write(encryptor.update(bytes([BLOCK_SIZE]) * BLOCK_SIZE))
        outfile.write(encryptor.finalize())

    key_encryptor = Cipher(algorithms.AES(decoded_key), modes.ECB()).encryptor()
    enc_kek = key_encryptor.update(_pkcs5_pad(file_key)) + key_encryptor.finalize()

    metadata = EncryptionMetadata(
        key=base64.b64encode(enc_kek).decode('utf-8'),
        iv=base64.b64encode(iv_data).decode('utf-8'),
    )
    return metadata, temp_output_file
