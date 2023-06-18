import os
from typing import TYPE_CHECKING

import dropbox
import dropbox.files

if TYPE_CHECKING:
    from dropbox.files import ListFolderResult

# Dropbox API access token
DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_TOKEN")
assert DROPBOX_ACCESS_TOKEN, "Please set the DROPBOX_TOKEN environment variable"


def create_shared_links(
    folder_path: str = "/nd2_test_data", extension: str = ".nd2"
) -> list[dict]:
    # Authenticate with Dropbox API
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

    # Retrieve file list in the folder
    result: ListFolderResult = dbx.files_list_folder(path=folder_path, recursive=True)

    urls = []
    for entry in result.entries:
        if isinstance(entry, dropbox.files.FileMetadata) and entry.name.endswith(
            extension
        ):
            shared_link = dbx.sharing_create_shared_link(entry.path_lower)
            if shared_link.visibility.is_public():
                urls.append(
                    {
                        "name": entry.name,
                        "last_updated": entry.client_modified.isoformat(),
                        "content_hash": entry.content_hash,
                        "path": shared_link.path,
                        "url": shared_link.url,
                        "size": entry.size,
                    }
                )

    return urls
