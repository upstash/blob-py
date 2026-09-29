import re
from typing import Dict, List, Optional, Sequence

from upstash_blob.errors import BlobError

# The leading bytes a type check looks at. Every signature known here sits in the first 32 bytes;
# the number matches the TypeScript SDK so both halves agree on what they are talking about.
SNIFF_BYTES = 4100


def _magic(b: bytes, offset: int, sig: bytes) -> bool:
    return b[offset : offset + len(sig)] == sig


def _u32le(b: bytes, offset: int) -> int:
    if len(b) < offset + 4:
        return -1
    return int.from_bytes(b[offset : offset + 4], "little")


# The DIB header sizes BMP actually uses. Without this, 'BM' matches any text beginning "BM".
_DIB_SIZES = {12, 40, 52, 56, 64, 108, 124}

# The types a signature proves outright, with no sibling format sharing it. Deliberately small: it
# is the domain over which a mismatch is a lie rather than a gap in the table. Left out on purpose
# because their signature proves a container and not the type above it: ISO-BMFF, EBML, Ogg, TIFF,
# sfnt fonts, MPEG audio and tar.
_CLOSED = {
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/bmp",
    "audio/wav",
    "video/x-msvideo",
    "application/pdf",
    "application/zip",
    "application/gzip",
    "application/x-7z-compressed",
    "application/x-rar-compressed",
    "application/x-bzip2",
}


def sniff(b: bytes) -> Optional[str]:
    """The type the leading bytes prove, or None when they prove nothing this is willing to name."""
    # A truncated PNG header is a common polyglot prefix, so require the IHDR chunk type at 12.
    if _magic(b, 0, b"\x89PNG\r\n\x1a\n"):
        return "image/png" if _magic(b, 12, b"IHDR") else None
    if _magic(b, 0, b"\xff\xd8\xff"):
        return "image/jpeg"
    if _magic(b, 0, b"GIF87a") or _magic(b, 0, b"GIF89a"):
        return "image/gif"
    # RIFF at 0 is webp, wav and avi alike; the form type at 8 separates them exactly.
    if _magic(b, 0, b"RIFF"):
        if _magic(b, 8, b"WEBP"):
            return "image/webp"
        if _magic(b, 8, b"WAVE"):
            return "audio/wav"
        if _magic(b, 8, b"AVI "):
            return "video/x-msvideo"
        return None
    if _magic(b, 0, b"BM") and _u32le(b, 14) in _DIB_SIZES:
        return "image/bmp"
    if _magic(b, 0, b"%PDF"):
        return "application/pdf"
    if _magic(b, 0, b"PK\x03\x04") or _magic(b, 0, b"PK\x05\x06") or _magic(b, 0, b"PK\x07\x08"):
        return "application/zip"
    if _magic(b, 0, b"\x1f\x8b"):
        return "application/gzip"
    if _magic(b, 0, b"7z\xbc\xaf\x27\x1c"):
        return "application/x-7z-compressed"
    if _magic(b, 0, b"Rar!\x1a\x07"):
        return "application/x-rar-compressed"
    # 'BZh' is three ASCII letters; the level digit and the block magic make it a signature.
    if _magic(b, 0, b"BZh") and len(b) > 3 and 0x31 <= b[3] <= 0x39 and _magic(b, 4, b"1AY&SY"):
        return "application/x-bzip2"
    return None


# What a wildcard means: a media family, not the subset whose bytes sniff() recognises. image/*
# never yields image/svg+xml: an SVG is script, so consenting to it has to be explicit.
_WILDCARD: Dict[str, Sequence[str]] = {
    "image": [
        "image/png",
        "image/jpeg",
        "image/gif",
        "image/webp",
        "image/bmp",
        "image/tiff",
        "image/avif",
        "image/heic",
        "image/heif",
        "image/x-icon",
    ],
    "video": [
        "video/mp4",
        "video/quicktime",
        "video/webm",
        "video/x-matroska",
        "video/x-msvideo",
        "video/mpeg",
        "video/ogg",
        "video/3gpp",
    ],
    "audio": [
        "audio/mpeg",
        "audio/wav",
        "audio/ogg",
        "audio/opus",
        "audio/flac",
        "audio/aac",
        "audio/mp4",
        "audio/webm",
    ],
}

# Spellings browsers and operating systems really send for a type that has one canonical name.
_ALIASES: Dict[str, str] = {
    "image/jpg": "image/jpeg",
    "image/pjpeg": "image/jpeg",
    "image/vnd.microsoft.icon": "image/x-icon",
    "audio/x-wav": "audio/wav",
    "audio/wave": "audio/wav",
    "audio/vnd.wave": "audio/wav",
    "audio/mp3": "audio/mpeg",
    "audio/x-flac": "audio/flac",
    "audio/x-aac": "audio/aac",
    "video/avi": "video/x-msvideo",
    "video/msvideo": "video/x-msvideo",
    "application/x-gzip": "application/gzip",
    "application/x-zip-compressed": "application/zip",
    "application/vnd.rar": "application/x-rar-compressed",
}


def _bare(content_type: str) -> str:
    return content_type.split(";", 1)[0].strip().lower()


def _canonical(content_type: str) -> str:
    bare = _bare(content_type)
    return _ALIASES.get(bare, bare)


_TYPE = re.compile(r"[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*")


def _bad(pattern: str, why: str) -> BlobError:
    return BlobError("invalid_content_type_pattern", f'content_types: {why} "{pattern}"')


def expand_content_types(allowed: Sequence[str]) -> List[str]:
    """
    Resolves the accept grammar to concrete types. Only image/*, video/* and audio/* exist; '*/*',
    any other wildcard and an empty list raise, because a list that matches everything is a limit
    that reads enforced and is not.
    """
    if isinstance(allowed, str):
        raise TypeError("content_types: expected a list of types, got a string")
    if len(allowed) == 0:
        raise BlobError("invalid_content_type_pattern", "content_types: empty list; omit the option to accept anything")
    out: List[str] = []
    for raw in allowed:
        pattern = raw.strip().lower()
        if pattern.endswith("/*"):
            family = _WILDCARD.get(pattern[:-2])
            if family is None:
                raise _bad(raw, "only image/*, video/* and audio/* are wildcards, got")
            candidates = [_canonical(t) for t in family]
        elif _TYPE.fullmatch(pattern):
            candidates = [_canonical(pattern)]
        else:
            raise _bad(raw, 'expected a "type/subtype", got')
        for t in candidates:
            if t not in out:
                out.append(t)
    return out


def check_content_type(declared: str, head: Optional[bytes], allowed: Sequence[str]) -> None:
    """
    The declared type against the allow list, then against the bytes when they are available. The
    byte half refuses on a proven conflict only: bytes that prove nothing pass, and so do bytes that
    prove something the declaration cannot be checked against (a .docx really is a zip).
    """
    declared_bare = _bare(declared)
    want = _ALIASES.get(declared_bare, declared_bare)
    if want not in [_canonical(t) for t in allowed]:
        raise BlobError(
            "content_type_not_allowed",
            f"{declared_bare} is not allowed",
            hint=f"allowed: {', '.join(allowed)}",
        )
    if head is None:
        return
    seen = sniff(head)
    if seen is None or seen == want or want not in _CLOSED:
        return
    raise BlobError(
        "content_type_not_allowed",
        f"the bytes are {seen}, which cannot be served as {declared_bare}",
        hint="declare the type the bytes actually are, or add it to content_types",
    )
