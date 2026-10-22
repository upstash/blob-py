import html
import re
from typing import Dict, List, Mapping, Optional
from urllib.parse import quote

from upstash_blob.errors import BlobError


def uri_encode(s: str, keep_slash: bool = False) -> str:
    """S3 wants every character outside the unreserved set percent-encoded."""
    return quote(s, safe="-_.~/" if keep_slash else "-_.~")


def encode_key(path: str) -> str:
    """
    A temporary credential authorises the whole bucket and URL resolution collapses ``..``, so a
    traversing key would touch a different object than named. Reject rather than normalise.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    segments = path.split("/")
    for s in segments:
        if s in (".", ".."):
            raise ValueError(f'path may not contain "." or ".." segments: {path}')
    return "/".join(uri_encode(s) for s in segments)


def nameable(path: str) -> bool:
    """Whether a url can name this key. One that storage listed may hold a "." segment."""
    try:
        encode_key(path)
    except ValueError:
        return False
    return True


_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


def presignable_key(path: str) -> None:
    """The agent signs no key with an empty segment ("dir/", "a//b") or a control character."""
    encode_key(path)
    if _CONTROL.search(path):
        raise BlobError("invalid_input", "a signed url cannot name a path with control characters")
    if "" in path.split("/"):
        raise BlobError(
            "invalid_input",
            "a signed url cannot name a path with an empty segment",
            hint='drop the leading, trailing or doubled "/"',
        )


def decode_entities(s: str) -> str:
    return html.unescape(s)


def escape_xml(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def tag(xml: str, name: str) -> Optional[str]:
    m = re.search(f"<{name}>(.*?)</{name}>", xml, re.DOTALL)
    return m.group(1) if m else None


def blocks(xml: str, name: str) -> List[str]:
    return re.findall(f"<{name}>.*?</{name}>", xml, re.DOTALL)


_HEADER_NAME = re.compile(r"[A-Za-z0-9!#$%&'*+.^_`|~-]+")
# ASCII, not Latin-1: R2 does not store a non-ASCII value verbatim. Measured 2026-08-25:
# metadata {"note": "café"} comes back as "=?utf-8?Q?caf=C3=A9?=".
_ASCII = re.compile(r"[\x20-\x7e]*")


def meta_headers(metadata: Optional[Mapping[str, str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (metadata or {}).items():
        if not isinstance(k, str) or not _HEADER_NAME.fullmatch(k):
            raise BlobError("invalid_input", f'metadata key "{k}" is not a valid header name')
        # Storage hands keys back lowercased, so "recordId" would be written and "recordid" read back:
        # refused here, where it is the caller's mistake, rather than as a missing key much later.
        if k != k.lower():
            raise BlobError(
                "invalid_input",
                f'metadata key "{k}" must be lowercase',
                hint=f'storage returns keys lowercased; write "{k.lower()}"',
            )
        if not isinstance(v, str):
            raise BlobError("invalid_input", f"metadata.{k} must be a string")
        # h11 refuses a value with leading or trailing whitespace, which fetch would trim silently.
        if not _ASCII.fullmatch(v) or v != v.strip():
            raise BlobError(
                "invalid_input",
                f"metadata.{k} has characters storage does not carry back unchanged",
                hint="metadata is printable ASCII; percent-encode anything else with urllib.parse.quote",
            )
        out[f"x-amz-meta-{k}"] = v
    return out
