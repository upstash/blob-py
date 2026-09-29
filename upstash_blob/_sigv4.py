"""AWS Signature Version 4, service 's3', UNSIGNED-PAYLOAD throughout, so bodies stream unhashed."""

import hashlib
import hmac
import re
from datetime import datetime, timezone
from typing import Dict, List, Mapping, Optional, Tuple

from upstash_blob._keys import uri_encode

UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
_SPACES = re.compile(r"\s+")


def _hmac(key: bytes, data: str) -> bytes:
    return hmac.new(key, data.encode(), hashlib.sha256).digest()


def canonical_query(query: Optional[Mapping[str, str]]) -> str:
    pairs: List[Tuple[str, str]] = sorted((uri_encode(k), uri_encode(v)) for k, v in (query or {}).items())
    return "&".join(f"{k}={v}" for k, v in pairs)


def sign_headers(
    *,
    access_key_id: str,
    secret_access_key: str,
    session_token: Optional[str],
    region: str,
    method: str,
    host: str,
    path: str,
    query: Optional[Mapping[str, str]] = None,
    headers: Optional[Mapping[str, str]] = None,
    now: Optional[datetime] = None,
    service: str = "s3",
) -> Dict[str, str]:
    """
    Header-authenticated request: returns the full header set to send, minus ``host``, which the
    HTTP client sets. ``path`` is already percent-encoded; S3 canonicalises it once, not twice.
    """
    when = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    amz = when.strftime("%Y%m%dT%H%M%SZ")
    day = amz[:8]

    out: Dict[str, str] = {k.lower(): v for k, v in (headers or {}).items()}
    out["host"] = host
    out["x-amz-date"] = amz
    out["x-amz-content-sha256"] = UNSIGNED_PAYLOAD
    if session_token:
        out["x-amz-security-token"] = session_token

    entries = sorted((k.strip(), _SPACES.sub(" ", v.strip())) for k, v in out.items())
    canonical_headers = "".join(f"{k}:{v}\n" for k, v in entries)
    signed = ";".join(k for k, _ in entries)
    canonical_request = "\n".join(
        [method.upper(), path or "/", canonical_query(query), canonical_headers, signed, UNSIGNED_PAYLOAD]
    )

    scope = f"{day}/{region}/{service}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(canonical_request.encode()).hexdigest()])
    key = _hmac(("AWS4" + secret_access_key).encode(), day)
    key = _hmac(key, region)
    key = _hmac(key, service)
    key = _hmac(key, "aws4_request")
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()

    out["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, SignedHeaders={signed}, Signature={signature}"
    )
    del out["host"]
    return out
