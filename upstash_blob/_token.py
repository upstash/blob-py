import base64
import binascii
from dataclasses import dataclass

AGENT_URL = "https://blob.upstash.io"
DOMAIN_SUFFIX = "blob.upstash.io"

_TOKEN_VERSION = 0x02


@dataclass(frozen=True)
class DecodedToken:
    bucket_id: str
    password: str
    hash_for_domain: str
    """The bucket's public DNS label: ``<hash_for_domain>.blob.upstash.io`` serves its objects."""
    flags: int


def _from_base64url(s: str) -> bytes:
    b64 = s.replace("-", "+").replace("_", "/")
    return base64.b64decode(b64 + "=" * (-len(b64) % 4), validate=True)


def to_base64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def encode_token(bucket_id: str, password: str, hash_for_domain: str, flags: int = 0) -> str:
    id_, pw, h = bucket_id.encode(), password.encode(), hash_for_domain.encode()
    header = bytes([_TOKEN_VERSION, flags, len(id_), len(pw) >> 8, len(pw) & 0xFF, len(h)])
    return to_base64url(header + id_ + pw + h)


def decode_token(token: str) -> DecodedToken:
    """Trailing bytes mean tamper, so the length check is exact."""
    try:
        raw = _from_base64url(token.strip())
    except (binascii.Error, ValueError):
        raise ValueError("token is not base64url") from None
    if len(raw) < 6 or raw[0] != _TOKEN_VERSION:
        raise ValueError("token: unsupported format")
    id_len, pw_len, h_len = raw[2], (raw[3] << 8) | raw[4], raw[5]
    if id_len == 0 or pw_len == 0 or h_len == 0 or len(raw) != 6 + id_len + pw_len + h_len:
        raise ValueError("token: malformed")
    try:
        return DecodedToken(
            flags=raw[1],
            bucket_id=raw[6 : 6 + id_len].decode(),
            password=raw[6 + id_len : 6 + id_len + pw_len].decode(),
            hash_for_domain=raw[6 + id_len + pw_len :].decode(),
        )
    except UnicodeDecodeError:
        raise ValueError("token: malformed") from None
