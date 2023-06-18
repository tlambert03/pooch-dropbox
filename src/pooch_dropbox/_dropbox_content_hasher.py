"""From https://github.com/dropbox/dropbox-api-content-hasher.

License: Apache 2.0

See also https://www.dropbox.com/developers/reference/content-hash

To calculate the content_hash of a file:
- Split the file into blocks of 4 MB (4,194,304 or 4 * 1024 * 1024 bytes). The last
  block (if any) may be smaller than 4 MB.
- Compute the hash of each block using SHA-256.
- Concatenate the hash of all blocks in the binary format to form a single binary
  string.
- Compute the hash of the concatenated string using SHA-256. Output the
  resulting hash in hexadecimal format.

"""
from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from os import PathLike
    from typing import Protocol, Union

    StrOrBytesPath = Union[str, bytes, PathLike[str], PathLike[bytes]]

    class SupportsUpdate(Protocol):
        """Protocol for objects that have an update() method."""

        def update(self, new_data: bytes) -> None:
            """Update this object with `new_data`."""


def db_content_hash(file: StrOrBytesPath, chunksize: int = 1024) -> str:
    """Return the content hash of a file using the same algorithm as Dropbox."""
    with open(file, "rb") as f:
        hasher = DropboxContentHasher()
        while chunk := f.read(chunksize):
            hasher.update(chunk)
    return hasher.hexdigest()


class DropboxContentHasher:
    """Computes content_hash using the same algorithm that the Dropbox API uses.

    The digest() method returns a raw binary representation of the hash.  The
    hexdigest() convenience method returns a hexadecimal-encoded version, which
    is what the "content_hash" metadata field uses.

    This class has the same interface as the hashers in the standard 'hashlib'
    package.

    Examples
    --------
        hasher = DropboxContentHasher()
        with open('some-file', 'rb') as f:
            while True:
                chunk = f.read(1024)  # or whatever chunk size you want
                if len(chunk) == 0:
                    break
                hasher.update(chunk)
        print(hasher.hexdigest())
    """

    BLOCK_SIZE = 4 * 1024 * 1024

    def __init__(self) -> None:
        self._overall_hasher: hashlib._Hash | None = hashlib.sha256()
        self._block_hasher: hashlib._Hash | None = hashlib.sha256()
        self._block_pos = 0

        self.digest_size = self._overall_hasher.digest_size

    def update(self, new_data: bytes) -> None:
        """Update this hasher with `new_data`."""
        if self._overall_hasher is None or self._block_hasher is None:
            raise RuntimeError(
                "can't use this object anymore; you already called digest()"
            )

        assert isinstance(new_data, bytes), f"Expecting a byte string, got {new_data!r}"

        new_data_pos = 0
        while new_data_pos < len(new_data):
            if self._block_pos == self.BLOCK_SIZE:
                self._overall_hasher.update(self._block_hasher.digest())
                self._block_hasher = hashlib.sha256()
                self._block_pos = 0

            space_in_block = self.BLOCK_SIZE - self._block_pos
            part = new_data[new_data_pos : (new_data_pos + space_in_block)]
            self._block_hasher.update(part)

            self._block_pos += len(part)
            new_data_pos += len(part)

    def _finish(self) -> hashlib._Hash:
        if self._overall_hasher is None or self._block_hasher is None:
            raise RuntimeError(
                "can't use this object anymore; "
                "you already called digest() or hexdigest()"
            )

        if self._block_pos > 0:
            self._overall_hasher.update(self._block_hasher.digest())
            self._block_hasher = None
        h = self._overall_hasher
        self._overall_hasher = None  # Make sure we can't use this object anymore.
        return h

    def digest(self) -> bytes:
        """Return the digest value as a binary string."""
        return self._finish().digest()

    def hexdigest(self) -> str:
        """Return the digest value as a hexadecimal string."""
        return self._finish().hexdigest()

    # def copy(self) -> DropboxContentHasher:
    #     """Return a copy of this object."""
    #     c = DropboxContentHasher.__new__(DropboxContentHasher)
    #     c._overall_hasher = self._overall_hasher.copy()
    #     c._block_hasher = self._block_hasher.copy()
    #     c._block_pos = self._block_pos
    #     return c


class StreamHasher:
    """Hasher for a file-like stream that hashes everything that passes through it.

    Can be used with DropboxContentHasher or any hasher with an update() method.

    Examples
    --------
        hasher = DropboxContentHasher()
        with open('some-file', 'rb') as f:
            wrapped_f = StreamHasher(f, hasher)
            response = some_api_client.upload(wrapped_f)

        locally_computed = hasher.hexdigest()
        assert response.content_hash == locally_computed
    """

    def __init__(self, f, hasher: SupportsUpdate):
        self._f = f
        self._hasher = hasher

    def close(self):
        return self._f.close()

    def flush(self):
        return self._f.flush()

    def fileno(self):
        return self._f.fileno()

    def tell(self):
        return self._f.tell()

    def read(self, *args):
        b = self._f.read(*args)
        self._hasher.update(b)
        return b

    def write(self, b):
        self._hasher.update(b)
        return self._f.write(b)

    def next(self):
        b = self._f.next()
        self._hasher.update(b)
        return b

    def readline(self, *args):
        b = self._f.readline(*args)
        self._hasher.update(b)
        return b

    def readlines(self, *args):
        bs = self._f.readlines(*args)
        for b in bs:
            self._hasher.update(b)
        return b
