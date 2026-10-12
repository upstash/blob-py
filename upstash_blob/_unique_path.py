import re
import secrets
from typing import Tuple

_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_SUFFIX_LENGTH = 8
_EXTENSION = re.compile(r"\.[a-z0-9]{1,8}\Z", re.IGNORECASE | re.ASCII)


def _split_extension(name: str) -> Tuple[str, str]:
    m = _EXTENSION.search(name)
    return (name[: m.start()], m.group(0)) if m and m.start() > 0 else (name, "")


def unique_path(path: str) -> str:
    """
    Adds a random suffix to the final filename, before its extension, so uploads never overwrite
    each other. Everything else is kept exactly as given: case, spaces, punctuation, Unicode,
    slashes and length. An empty final filename uses "file".

    >>> unique_path(f"users/{user_id}/{file.name}")
    'users/Alice_123/Q3 Report-4fKq2ZxA.pdf'

    Paths are not validated here; every request refuses "." and ".." segments. Store the returned
    path and use it unchanged for later reads and deletes.
    """
    at = path.rfind("/") + 1
    stem, extension = _split_extension(path[at:])
    suffix = "".join(secrets.choice(_BASE58) for _ in range(_SUFFIX_LENGTH))
    return f"{path[:at]}{stem or 'file'}-{suffix}{extension}"
