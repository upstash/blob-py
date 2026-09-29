import re
import secrets
import unicodedata
from string import Formatter
from typing import Any, Tuple

_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_SUFFIX_LENGTH = 8
_MAX_STEM = 64
_EXTENSION = re.compile(r"\.[a-z0-9]{1,8}\Z")
_formatter = Formatter()


def _split_extension(name: str) -> Tuple[str, str]:
    m = _EXTENSION.search(name)
    return (name[: m.start()], m.group(0)) if m else (name, "")


def _slug(stem: str) -> str:
    # Letters and digits stay; every run of anything else becomes one '-'.
    out = []
    for ch in stem:
        out.append(ch if unicodedata.category(ch)[0] in "LN" else "-")
    slug = re.sub("-+", "-", "".join(out)).strip("-")
    return slug[:_MAX_STEM].rstrip("-")


def _sanitize(value: str) -> str:
    """Everything an interpolated value can contribute: no directories, no control characters."""
    basename = re.split(r"[/\\]", value)[-1]
    cleaned = "".join(ch for ch in basename if unicodedata.category(ch) not in ("Cc", "Cf"))
    cleaned = unicodedata.normalize("NFC", cleaned).lower()
    stem, extension = _split_extension(cleaned)
    return (_slug(stem) or "file") + extension


def unique_path(template: str, *args: Any, **kwargs: Any) -> str:
    """
    A path with a random suffix, built from a ``str.format`` template whose trust boundary is the
    replacement fields: slashes in the template are structure, while a value's directories, control
    characters and punctuation are stripped.

    >>> unique_path("avatars/{}/{}", user.id, "My Photo.PNG")
    'avatars/7/my-photo-4fKq2ZxA.png'
    """
    path = ""
    auto = 0
    for literal, field, spec, conversion in _formatter.parse(template):
        path += literal.strip()
        if field is None:
            continue
        if field == "":
            field = str(auto)
            auto += 1
        value, _ = _formatter.get_field(field, args, kwargs)
        value = _formatter.convert_field(value, conversion)
        path += _sanitize(format(value, spec or ""))
    at = path.rfind("/") + 1
    stem, extension = _split_extension(path[at:])
    suffix = "".join(secrets.choice(_BASE58) for _ in range(_SUFFIX_LENGTH))
    return f"{path[:at]}{stem or 'file'}-{suffix}{extension}"
