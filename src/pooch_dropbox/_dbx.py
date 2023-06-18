from __future__ import annotations

import os
import tempfile
import warnings
from typing import TYPE_CHECKING, Iterator, cast

import dropbox
import dropbox.files
import pooch

if TYPE_CHECKING:
    from dropbox.files import ListFolderResult


def create_shared_links(
    folder_path: str,
    recursive: bool = True,
    extension: str = "",
    api_token: str | None = None,
) -> Iterator[dict]:
    """Yield a dictionary of file metadata for each file in a Dropbox folder.

    Parameters
    ----------
    folder_path : str
        Path to the folder in Dropbox.  This should start with a forward slash.
        Depending on the permissions of the access token, this may be the root
        folder or a subfolder.
    recursive : bool, optional
        If True, recursively search subfolders for files.  Default is True.
    extension : str, optional
        If provided, only return files with this extension.  Default is "".
    api_token : str, optional
        Dropbox API token.  If not provided, will look for the
        DROPBOX_API_TOKEN environment variable.
    """
    # Authenticate with Dropbox API
    if not api_token:
        api_token = os.getenv("DROPBOX_API_TOKEN")
    if not api_token:
        raise ValueError(
            "Please provide a api_token or set the DROPBOX_API_TOKEN "
            "environment variable"
        )
    dbx = dropbox.Dropbox(api_token)

    # Retrieve file list in the folder
    result = dbx.files_list_folder(path=folder_path, recursive=recursive)

    for entry in cast("ListFolderResult", result).entries:
        # iterate through all file entries
        if isinstance(entry, dropbox.files.FileMetadata):
            # skip files that don't match the extension
            if extension and not entry.name.endswith(extension):
                continue
            # create a shared link for each file
            shared_link = dbx.sharing_create_shared_link(entry.path_lower)
            yield {
                "name": entry.name,
                "last_updated": entry.client_modified.isoformat(),
                "content_hash": entry.content_hash,
                "path": shared_link.path,
                "url": shared_link.url.replace("dl=0", "dl=1"),
                "size": entry.size,
                "public": shared_link.visibility.is_public(),
            }


def create_pooch_registry(
    dropbox_folder: str,
    output_path: str = "registry.txt",
    extension: str = "",
    force_hash: str | None = None,
) -> None:
    """Given a dropbox folder, create a pooch registry file.

    A note on the `force_hash` option:

    Dropbox does provide a content_hash ... but it's not the same as what pooch needs...
    so we have to download the file to get the correct hash.

    This is annoying, and could be improved by accepting the local dropbox folder as an
    input rather than downloading.

    Alternatively, if `force_hash` is truthy, we just skip it, and write the hash to the
    registry.

    but then one needs to use the following code to clear bad keys:

    >>> POOCH = pooch.create(...)
    >>> POOCH.load_registry("registry.txt")
    >>> for key, val in POOCH.registry.items():
    ...     if val == force_hash:
    ...         POOCH.registry[key] = None

    Parameters
    ----------
    dropbox_folder : str
        Path to the folder in Dropbox.  This should start with a forward slash.
        Depending on the permissions of the access token, this may be the root
        folder or a subfolder.
    output_path : str, optional
        Path to the output registry file.  Default is "registry.txt".
    extension : str, optional
        If provided, only return files in `dropbox_folder` that end with this extension.
        Default is "".
    force_hash : str, optional
        If not null, this hash will be written to the registry file for all files.
        (See notes above for more details.)


    """
    from pooch import get_logger

    from ._dropbox_content_hasher import db_content_hash

    with tempfile.TemporaryDirectory() as directory:
        with open(output_path, "w") as registry:
            shared_links = create_shared_links(dropbox_folder, extension=extension)
            for link in shared_links:
                fname = link["name"]
                url = link["url"]

                if force_hash:
                    line = f"{fname} {force_hash} {url}"
                    get_logger().info(line)
                    registry.write(line + "\n")
                    continue

                # Download each data file to the specified directory
                path = pooch.retrieve(url=url, known_hash=None, path=directory)

                dropbox_hash = link["content_hash"]
                if db_content_hash(path) != dropbox_hash:
                    warnings.warn(
                        f"Dropbox hash for file {fname} does not not match, skipping.",
                        stacklevel=2,
                    )
                    continue

                pooch_hash = pooch.file_hash(path)
                registry.write(f"{fname} {pooch_hash} {url}\n")


POOCH = pooch.create(path=pooch.os_cache("nd2-samples"), base_url="")
POOCH.load_registry("registry.txt")
for key, val in POOCH.registry.items():
    if val in ("None", "none", "_"):
        POOCH.registry[key] = None
