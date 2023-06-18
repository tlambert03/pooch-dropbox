"""Tools for creating pooch registries from dropbox folders."""
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("pooch-dropbox")
except PackageNotFoundError:
    __version__ = "uninstalled"

__author__ = "Talley Lambert"
__email__ = "talley.lambert@gmail.com"
__all__ = ["db_content_hash", "create_pooch_registry", "create_shared_links"]

from ._dbx import create_pooch_registry, create_shared_links
from ._dropbox_content_hasher import db_content_hash
