"""
S3 Upload Client
"""
from __future__ import annotations

import base64
import json
import os
import tempfile
import boto3
import datetime
from logging import getLogger
from typing import IO, NamedTuple

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .base_upload_client import BaseUploadClient

# SnowflakeEncryptionUtil and SnowflakeFileEncryptionMaterial below are copied from
# snowflake-connector-python 4.8.1 (Apache License 2.0), which connector 5.x no longer ships.
# The output must stay readable by Snowflake stages created with MASTER_KEY.

UTF8 = "utf-8"
kilobyte = 1024

block_size = int(algorithms.AES.block_size / 8)  # in bytes

logger = getLogger(__name__)


class SnowflakeFileEncryptionMaterial(NamedTuple):
    query_stage_master_key: str  # query stage master key
    query_id: str  # query id
    smk_id: int  # SMK id


class MaterialDescriptor(NamedTuple):
    smk_id: int
    query_id: str
    key_size: int


class EncryptionMetadata(NamedTuple):
    key: str
    iv: str
    matdesc: str


def PKCS5_PAD(value: bytes, block_size: int) -> bytes:
    return b"".join(
        [
            value,
            (block_size - len(value) % block_size)
            * chr(block_size - len(value) % block_size).encode(UTF8),
        ]
    )


def matdesc_to_unicode(matdesc: MaterialDescriptor) -> str:
    """Convert Material Descriptor to Unicode String."""
    return str(
        json.dumps(
            {
                "queryId": matdesc.query_id,
                "smkId": str(matdesc.smk_id),
                "keySize": str(matdesc.key_size),
            },
            separators=(",", ":"),
        )
    )


class SnowflakeEncryptionUtil:
    @staticmethod
    def get_secure_random(byte_length: int) -> bytes:
        return os.urandom(byte_length)

    @staticmethod
    def encrypt_stream(
        encryption_material: SnowflakeFileEncryptionMaterial,
        src: IO[bytes],
        out: IO[bytes],
        chunk_size: int = 64 * kilobyte,  # block_size * 4 * 1024,
    ) -> EncryptionMetadata:
        """Reads content from src and write the encrypted content into out.

        This function is sensitive to current position of src and out.
        It does not seek to position 0 in neither stream objects before or after the encryption.

        Args:
            encryption_material: The encryption material for file.
            src: The input stream.
            out: The output stream.
            chunk_size: The size of read chunks (Default value = block_size * 4 * 1024

        Returns:
            The encryption metadata.
        """
        decoded_key = base64.standard_b64decode(
            encryption_material.query_stage_master_key
        )
        key_size = len(decoded_key)
        logger.debug("key_size = %s", key_size)

        # Generate key for data encryption
        iv_data = SnowflakeEncryptionUtil.get_secure_random(block_size)
        file_key = SnowflakeEncryptionUtil.get_secure_random(key_size)
        backend = default_backend()
        cipher = Cipher(algorithms.AES(file_key), modes.CBC(iv_data), backend=backend)
        encryptor = cipher.encryptor()

        padded = False
        while True:
            chunk = src.read(chunk_size)
            if len(chunk) == 0:
                break
            elif len(chunk) % block_size != 0:
                chunk = PKCS5_PAD(chunk, block_size)
                padded = True
            out.write(encryptor.update(chunk))
        if not padded:
            out.write(encryptor.update(block_size * chr(block_size).encode(UTF8)))
        out.write(encryptor.finalize())

        # encrypt key with QRMK
        cipher = Cipher(algorithms.AES(decoded_key), modes.ECB(), backend=backend)
        encryptor = cipher.encryptor()
        enc_kek = (
            encryptor.update(PKCS5_PAD(file_key, block_size)) + encryptor.finalize()
        )

        mat_desc = MaterialDescriptor(
            smk_id=encryption_material.smk_id,
            query_id=encryption_material.query_id,
            key_size=key_size * 8,
        )
        metadata = EncryptionMetadata(
            key=base64.b64encode(enc_kek).decode("utf-8"),
            iv=base64.b64encode(iv_data).decode("utf-8"),
            matdesc=matdesc_to_unicode(mat_desc),
        )
        return metadata

    @staticmethod
    def encrypt_file(
        encryption_material: SnowflakeFileEncryptionMaterial,
        in_filename: str,
        chunk_size: int = 64 * kilobyte,
        tmp_dir: str | None = None,
    ) -> tuple[EncryptionMetadata, str]:
        """Encrypts a file in a temporary directory.

        Args:
            encryption_material: The encryption material for file.
            in_filename: The input file's name.
            chunk_size: The size of read chunks (Default value = block_size * 4 * 1024).
            tmp_dir: Temporary directory to use, optional (Default value = None).

        Returns:
            The encryption metadata and the encrypted file's location.
        """
        temp_output_fd, temp_output_file = tempfile.mkstemp(
            text=False, dir=tmp_dir, prefix=os.path.basename(in_filename) + "#"
        )
        logger.debug(
            "unencrypted file: %s, temp file: %s, tmp_dir: %s",
            in_filename,
            temp_output_file,
            tmp_dir,
        )
        with open(in_filename, "rb") as infile:
            with os.fdopen(temp_output_fd, "wb") as outfile:
                metadata = SnowflakeEncryptionUtil.encrypt_stream(
                    encryption_material, infile, outfile, chunk_size
                )
        return metadata, temp_output_file


class S3UploadClient(BaseUploadClient):
    """S3 Upload Client class"""

    def __init__(self, connection_config):
        super().__init__(connection_config)
        self.s3_client = self._create_s3_client()

    def _create_s3_client(self, config=None):
        if not config:
            config = self.connection_config

        # Get the required parameters from config file and/or environment variables
        aws_profile = config.get('aws_profile') or os.environ.get('AWS_PROFILE')
        aws_access_key_id = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)

        # Create the s3 client
        return aws_session.client('s3',
                                  region_name=config.get('s3_region_name'),
                                  endpoint_url=config.get('s3_endpoint_url'))

    def upload_file(self, file, stream, temp_dir=None):
        """Upload file to an external snowflake stage on s3"""
        # Generating key in S3 bucket
        bucket = self.connection_config['s3_bucket']
        s3_acl = self.connection_config.get('s3_acl')
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")

        s3_key = f"{s3_key_prefix}pipelinewise_{stream}_{timestamp}_{os.path.basename(file)}"
        self.logger.info('Target S3 bucket: %s, local file: %s, S3 key: %s', bucket, file, s3_key)

        # Encrypt csv if client side encryption enabled
        master_key = self.connection_config.get('client_side_encryption_master_key', '')
        if master_key != '':
            # Encrypt the file
            encryption_material = SnowflakeFileEncryptionMaterial(
                query_stage_master_key=master_key,
                query_id='',
                smk_id=0
            )
            encryption_metadata, encrypted_file = SnowflakeEncryptionUtil.encrypt_file(
                encryption_material,
                file,
                tmp_dir=temp_dir
            )

            # Upload to s3
            extra_args = {'ACL': s3_acl} if s3_acl else {}

            # Send key and iv in the metadata, that will be required to decrypt and upload the encrypted file
            extra_args['Metadata'] = {
                'x-amz-key': encryption_metadata.key,
                'x-amz-iv': encryption_metadata.iv
            }
            self.s3_client.upload_file(encrypted_file, bucket, s3_key, ExtraArgs=extra_args)

            # Remove the uploaded encrypted file
            os.remove(encrypted_file)

        # Upload to S3 without encrypting
        else:
            extra_args = {'ACL': s3_acl} if s3_acl else None
            self.s3_client.upload_file(file, bucket, s3_key, ExtraArgs=extra_args)

        return s3_key

    def delete_object(self, stream: str, key: str) -> None:
        """Delete object from an external snowflake stage on S3"""
        self.logger.info('Deleting %s from external snowflake stage on S3', key)
        bucket = self.connection_config['s3_bucket']
        self.s3_client.delete_object(Bucket=bucket, Key=key)

    def copy_object(self, copy_source: str, target_bucket: str, target_key: str, target_metadata: dict) -> None:
        """Copy object to another location on S3"""
        self.logger.info('Copying %s to %s/%s', copy_source, target_bucket, target_key)
        source_bucket, source_key = copy_source.split("/", 1)
        metadata = self.s3_client.head_object(Bucket=source_bucket, Key=source_key).get('Metadata', {})
        metadata.update(target_metadata)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object
        self.s3_client.copy_object(CopySource=copy_source, Bucket=target_bucket, Key=target_key,
                                   Metadata=metadata, MetadataDirective="REPLACE")
