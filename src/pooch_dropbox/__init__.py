"""Tools for creating pooch registries from dropbox folders"""
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("pooch-dropbox")
except PackageNotFoundError:
    __version__ = "uninstalled"

__author__ = "Talley Lambert"
__email__ = "talley.lambert@gmail.com"
