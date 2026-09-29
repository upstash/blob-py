"""What the sync and async buckets share: everything but the I/O."""

import asyncio
import math
import random
import re
import threading
import time
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote, urlsplit

import httpx

from upstash_blob._keys import blocks, decode_entities, encode_key, escape_xml, nameable, presignable_key, tag
from upstash_blob._sigv4 import canonical_query, sign_headers
from upstash_blob._telemetry import telemetry_headers
from upstash_blob._token import DOMAIN_SUFFIX, decode_token
from upstash_blob._units import CacheOption
from upstash_blob.errors import BlobError
from upstash_blob.types import BlobObject, MultipartUpload

TOKEN_ENV = "UPSTASH_BLOB_TOKEN"
"""The variable ``Bucket.from_env()`` reads."""

MAX_PRESIGN_SECONDS = 600
"""The agent refuses a longer url: it is also how long one outlives a suspension or rotation."""

DEFAULT_READ_SECONDS = 300

RETRIABLE_METHODS = {"GET", "HEAD", "PUT", "DELETE"}
RETRY_ATTEMPTS = 3

# R2 answers a temporary credential that has expired with a 403 naming it, which is
# indistinguishable from a signature error unless the body is read.
CREDENTIAL_REJECTED = re.compile(r"ExpiredToken|InvalidAccessKeyId|TokenRefreshRequired")

AGENT_TIMEOUT = 10.0

# Must stay under the agent's 60 s re-mint margin: at or above it the agent hands back the same
# credential the SDK already considers stale, and every operation refetches until the limiter 429s.
_REFRESH_MARGIN_S = 30.0
# A Retry-After longer than this is the agent asking for a pause no request can wait out.
_MAX_MINT_WAIT_S = 10.0

DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


# --------------------------------------------------------------- credentials


@dataclass(frozen=True)
class Credentials:
    access_key_id: str
    # Kept out of repr: an error reporter that captures frame locals would otherwise record them.
    secret_access_key: str = field(repr=False)
    session_token: str = field(repr=False)
    endpoint: str
    bucket: str
    region: str
    expires_at: float
    """Unix seconds."""
    visibility: Optional[str]
    """'private' means the bucket has no public host, so a BlobObject carries no url."""


def _r2_https(url: Any) -> bool:
    if not isinstance(url, str):
        return False
    # urlsplit reads "https://evil.example\\@acc.r2..." as an R2 host where a browser reads evil.example,
    # so anything that could carry userinfo is refused rather than parsed.
    if "\\" in url or "@" in url or any(ord(c) < 0x21 for c in url):
        return False
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    return parts.scheme == "https" and (parts.hostname or "").endswith(".r2.cloudflarestorage.com")


def normalized_host(url: str) -> str:
    """The Host header httpx sends for this url: lowercased, with the default port dropped."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    return host if parts.port in (None, 443) else f"{host}:{parts.port}"


def agent_headers(token: str, telemetry: bool) -> Dict[str, str]:
    return {"authorization": f"Bearer {token}", **(telemetry_headers() if telemetry else {})}


def mint_wait(res: httpx.Response) -> Optional[float]:
    """For a 429 or 503: how long the agent asked to wait. Raises when it is longer than a request can."""
    if res.status_code not in (429, 503):
        return None
    try:
        parsed = float(res.headers.get("retry-after", ""))
    except ValueError:
        parsed = 0.0
    # A missing Retry-After would otherwise retry immediately, three times.
    asked = parsed if parsed > 0 else 2.0
    if asked > _MAX_MINT_WAIT_S:
        raise BlobError("mint_backoff", f"the credential service asked for {asked:g}s of backoff", retry_after=asked)
    return asked


def credentials_from(res: httpx.Response) -> Credentials:
    status = res.status_code
    if status == 401:
        raise BlobError("unauthorized", "the bucket token was rejected")
    if status == 429:
        raise BlobError("rate_limited", "credential requests are rate limited")
    if status == 503:
        raise BlobError("not_ready")
    if not res.is_success:
        raise BlobError("request_failed", f"credentials request failed with {status}", status=502)
    try:
        body = res.json()
        creds = Credentials(
            access_key_id=str(body["accessKeyId"]),
            secret_access_key=str(body["secretAccessKey"]),
            session_token=str(body["sessionToken"]),
            endpoint=str(body["endpoint"]).rstrip("/"),
            bucket=str(body["bucket"]),
            region=str(body.get("region") or "auto"),
            expires_at=float(body["expiresAt"]),
            visibility=body.get("visibility") if body.get("visibility") in ("public", "private") else None,
        )
    except (ValueError, KeyError, TypeError):
        raise BlobError("request_failed", "credentials response was not understood", status=502) from None
    # The endpoint decides where every object request goes; refuse anything but R2 over https.
    if not _r2_https(creds.endpoint):
        raise BlobError("request_failed", "credentials response named an unexpected endpoint", status=502)
    return creds


class CredentialCache:
    """
    Keyed by token rather than held per bucket: ``Bucket.from_env()`` per request is the documented
    shape on serverless platforms, and an instance-held cache would mint a credential for each one.
    Shared by the sync and async buckets.
    """

    def __init__(self) -> None:
        self._current: Optional[Credentials] = None
        self._refresh_at = 0.0
        self._mint_lock = threading.Lock()
        # Guards _alocks only, never a mint: the event loop takes it too, so it must never be held
        # across network I/O.
        self._lock = threading.Lock()
        self._alocks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = weakref.WeakKeyDictionary()

    def fresh(self) -> Optional[Credentials]:
        if self._current is not None and time.time() < self._refresh_at:
            return self._current
        return None

    def peek(self) -> Optional[Credentials]:
        """The cached credential, without minting one. None before the first mint."""
        return self._current

    def invalidate(self) -> None:
        """
        After a 403 that named the credential itself: the next get() mints. The credential stays
        peekable, since its visibility is still true and a private bucket must not look public.
        """
        self._refresh_at = 0.0

    def _store(self, creds: Credentials) -> Credentials:
        self._refresh_at = max(time.time(), creds.expires_at - _REFRESH_MARGIN_S)
        self._current = creds
        return creds

    def get(self, mint: Callable[[], Credentials]) -> Credentials:
        creds = self.fresh()
        if creds is not None:
            return creds
        with self._mint_lock:
            creds = self.fresh()
            if creds is not None:
                return creds
            return self._store(mint())

    async def aget(self, mint: Callable[[], Awaitable[Credentials]]) -> Credentials:
        creds = self.fresh()
        if creds is not None:
            return creds
        loop = asyncio.get_running_loop()
        with self._lock:
            lock = self._alocks.get(loop)
            if lock is None:
                lock = self._alocks[loop] = asyncio.Lock()
        async with lock:
            creds = self.fresh()
            if creds is not None:
                return creds
            return self._store(await mint())


_caches: Dict[Tuple[bool, str], CredentialCache] = {}
_caches_lock = threading.Lock()


def credential_cache_for(token: str, telemetry: bool) -> CredentialCache:
    with _caches_lock:
        cache = _caches.get((telemetry, token))
        if cache is None:
            cache = _caches[(telemetry, token)] = CredentialCache()
        return cache


def reset_credential_caches() -> None:
    """Tests only."""
    with _caches_lock:
        _caches.clear()


# ------------------------------------------------------------------ presign


def presign_payload(
    method: str,
    path: str,
    expires_in: float,
    headers: Optional[Mapping[str, str]] = None,
    query: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    presignable_key(path)
    body: Dict[str, Any] = {
        "method": method,
        "key": path,
        "expiresIn": min(MAX_PRESIGN_SECONDS, max(1, int(expires_in))),
    }
    if headers:
        body["headers"] = dict(headers)
    if query:
        body["query"] = dict(query)
    return body


def presign_retriable(res: httpx.Response) -> bool:
    # Signing has no side effect, so every 429 and 5xx is safe to ask again.
    return res.status_code == 429 or res.status_code >= 500


def presigned_from(res: httpx.Response) -> Tuple[str, datetime]:
    try:
        body = res.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        body = {}
    reason = body.get("error") if isinstance(body.get("error"), str) else None
    status = res.status_code
    # 413 is a request body over the agent's cap, which only oversized metadata reaches.
    if status in (400, 413):
        fallback = "body too large" if status == 413 else "bad request"
        raise BlobError("invalid_input", f"the signing service refused the request: {reason or fallback}")
    # A 401 or 403 reason is the bucket owner's billing notice (suspended, read-only), so it rides on
    # `reason`, which neither the message nor to_dict() carries.
    if status == 401:
        raise BlobError("unauthorized", "the bucket token was rejected", reason=reason)
    if status == 403:
        raise BlobError("forbidden", "Upstash refused to sign a url for this bucket", reason=reason)
    if status == 429:
        raise BlobError("rate_limited", "presign requests are rate limited")
    if not res.is_success:
        raise BlobError(
            "request_failed", f"presign request failed with {status}{f': {reason}' if reason else ''}", status=502
        )
    # The url is handed to whoever the caller gives it to; refuse anything but R2 over https.
    url, expires_at = body.get("url"), body.get("expiresAt")
    unexpected = BlobError("request_failed", "the signing service answered with an unexpected url", status=502)
    if (
        not isinstance(url, str)
        or not _r2_https(url)
        or isinstance(expires_at, bool)
        or not isinstance(expires_at, (int, float))
        or not math.isfinite(expires_at)
    ):
        raise unexpected
    try:
        return url, datetime.fromtimestamp(expires_at, timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise unexpected from None


# ---------------------------------------------------------------- responses


def backoff(attempt: int, retry_after: Optional[str], base: float = 0.2) -> float:
    """Full jitter, doubling from ``base`` seconds per attempt and capped at 4 s; Retry-After wins."""
    if retry_after:
        try:
            secs = float(retry_after)
            if secs >= 0:
                return min(secs, 10.0)
        except ValueError:
            pass
    return random.random() * min(4.0, base * 2.0**attempt)


@dataclass
class Head:
    size: int
    etag: str
    content_type: str
    cache_control: Optional[str]
    metadata: Dict[str, str]
    uploaded_at: datetime


def _http_date(value: Optional[str]) -> datetime:
    if value:
        try:
            return parsedate_to_datetime(value).astimezone(timezone.utc)
        except (TypeError, ValueError):
            pass
    return datetime.now(timezone.utc)


_ISO = re.compile(r"(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(?:\.(\d+))?(Z|[+-]\d\d:?\d\d)?", re.ASCII)


def iso_date(value: Optional[str]) -> Optional[datetime]:
    """
    By hand rather than fromisoformat, which on 3.10 refuses any fraction that is not 3 or 6
    digits. None when it does not parse.
    """
    m = _ISO.fullmatch((value or "").strip())
    if not m:
        return None
    y, mo, d, h, mi, s = (int(g) for g in m.groups()[:6])
    micros = int((m.group(7) or "0")[:6].ljust(6, "0"))
    tz = timezone.utc
    zone = m.group(8)
    if zone and zone != "Z":
        sign = -1 if zone[0] == "-" else 1
        hh, mm = int(zone[1:3]), int(zone[-2:])
        tz = timezone(sign * timedelta(hours=hh, minutes=mm))
    try:
        return datetime(y, mo, d, h, mi, s, micros, tz).astimezone(timezone.utc)
    except ValueError:
        return None


def head_from_headers(h: httpx.Headers) -> Head:
    metadata = {k[11:]: v for k, v in h.items() if k.lower().startswith("x-amz-meta-")}
    try:
        size = int(h.get("content-length", "0"))
    except ValueError:
        size = 0
    return Head(
        size=size,
        etag=h.get("etag", ""),
        content_type=h.get("content-type", "application/octet-stream"),
        cache_control=h.get("cache-control"),
        metadata=metadata,
        uploaded_at=_http_date(h.get("last-modified")),
    )


def error_from_body(status: int, xml: str) -> BlobError:
    code = tag(xml, "Code")
    message = tag(xml, "Message")
    if status == 404 or code in ("NoSuchKey", "NoSuchUpload"):
        return BlobError("not_found")
    if status == 412 or code == "PreconditionFailed":
        return BlobError("conflict")
    if status == 401:
        return BlobError("unauthorized")
    if status == 403 and code and CREDENTIAL_REJECTED.search(code):
        return BlobError(
            "unauthorized",
            "storage refused the temporary credential",
            hint="it expired mid-request; the SDK re-mints and retries once",
        )
    if status == 403:
        return BlobError("signature_mismatch")
    if status == 429 or code in ("SlowDown", "TooManyRequests"):
        return BlobError("rate_limited", "R2 rate limited the request")
    if status == 503:
        return BlobError("not_ready")
    if status == 413 or code == "EntityTooLarge":
        return BlobError("too_large")
    detail = (f" {code}" if code else "") + (f": {message}" if message else "")
    return BlobError("request_failed", f"R2 responded {status}{detail}", status=502 if status >= 500 else status)


def is_conditional(headers: Optional[Mapping[str, str]]) -> bool:
    """A conditional write sent twice can refuse its own first attempt, so it is sent once."""
    return bool(headers) and any(k.lower() in ("if-match", "if-none-match") for k in headers or {})


def refused_to_send() -> BlobError:
    """httpx would not put the request on the wire: a header or a body length the SDK let through."""
    # Not str(e): h11 quotes the header value it refused.
    return BlobError("invalid_input", "the request could not be sent: a header value or the body length is invalid")


_ETAG = re.compile(r'(?:W/)?"[\x21\x23-\x7e]*"|[\x21-\x7e]+', re.ASCII)


def check_etag(etag: str) -> str:
    if not isinstance(etag, str) or not _ETAG.fullmatch(etag):
        raise BlobError("invalid_input", "if_unchanged: expected an etag")
    return etag


def transport_error(e: Exception, what: str = "storage") -> BlobError:
    return BlobError("request_failed", f"could not reach {what}: {type(e).__name__}", status=502)


# ---------------------------------------------------------------------- XML


def parse_list(
    xml: str, blob: Callable[[str, int, str, datetime], BlobObject]
) -> Tuple[List[BlobObject], Optional[str]]:
    blobs: List[BlobObject] = []
    for b in blocks(xml, "Contents"):
        key = tag(b, "Key")
        if key is None:
            continue
        etag = decode_entities(tag(b, "ETag") or "")
        modified = iso_date(tag(b, "LastModified")) or datetime.fromtimestamp(0, timezone.utc)
        blobs.append(blob(decode_entities(key), int(tag(b, "Size") or 0), etag, modified))
    nxt = tag(xml, "NextContinuationToken")
    cursor = decode_entities(nxt) if tag(xml, "IsTruncated") == "true" and nxt else None
    return blobs, cursor


def parse_uploads(xml: str) -> Tuple[List[MultipartUpload], Optional[str], Optional[str]]:
    """The uploads on this page, and the markers for the next one (None when this is the last)."""
    out: List[MultipartUpload] = []
    for b in blocks(xml, "Upload"):
        key, upload_id = tag(b, "Key"), tag(b, "UploadId")
        if key is None or upload_id is None:
            continue
        out.append(MultipartUpload(decode_entities(key), decode_entities(upload_id), iso_date(tag(b, "Initiated"))))
    if tag(xml, "IsTruncated") != "true":
        return out, None, None
    # The markers are sent back verbatim, so a key carrying &, < or > has to be decoded first or the
    # next page repeats this one forever.
    next_key, next_id = tag(xml, "NextKeyMarker"), tag(xml, "NextUploadIdMarker")
    return (
        out,
        decode_entities(next_key) if next_key else None,
        decode_entities(next_id) if next_id else None,
    )


def complete_body(parts: Sequence[Tuple[int, str]]) -> str:
    inner = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{escape_xml(etag)}</ETag></Part>" for n, etag in sorted(parts)
    )
    return f"<CompleteMultipartUpload>{inner}</CompleteMultipartUpload>"


def delete_body(paths: Sequence[str]) -> str:
    # Keys travel in XML, not a url, so a listed key with a "." segment is deletable here.
    inner = "".join(f"<Object><Key>{escape_xml(p)}</Key></Object>" for p in paths)
    return f"<Delete>{inner}</Delete>"


def delete_errors(xml: str) -> List[str]:
    keys = (tag(b, "Key") for b in blocks(xml, "Error"))
    return [decode_entities(k) for k in keys if k is not None]


# ------------------------------------------------------------------ headers


def attachment_disposition(name: str) -> str:
    """
    The name reaches storage as a query parameter and comes back as a header value, so a quote, a
    semicolon or a CRLF in it must not be able to add a parameter or a second header. RFC 6266 4.1
    carries the real name in ``filename*`` as an RFC 8187 ext-value; the ``filename`` fallback is cut
    back to characters that cannot end the quoted string.
    """
    raw = name or "download"
    ascii_name = re.sub(r"[^\w.\- ]+", "_", raw, flags=re.ASCII) or "download"
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(raw, safe='!')}"


_MEDIA_TYPE = re.compile(r'[\w.+-]+/[\w.+-]+(?:\s*;\s*[\w.+-]+=(?:[\w.+-]+|"[^";\\]*"))*', re.ASCII)


def safe_content_type(content_type: str) -> str:
    """This becomes a Content-Type header, so it has to be a media type and nothing else."""
    if not isinstance(content_type, str) or not _MEDIA_TYPE.fullmatch(content_type):
        raise BlobError("invalid_input", f'content_type: "{content_type}" is not a media type')
    return content_type


# --------------------------------------------------------------------- base


class BucketCore:
    """Token, credential cache and the pure half of every request, shared by both buckets."""

    def __init__(self, token: str, cache: Optional[CacheOption], enable_telemetry: bool) -> None:
        if not isinstance(token, str) or not token:
            raise TypeError("Bucket(token): token is required")
        # A token read from a secret file often ends in a newline, which no header can carry.
        token = token.strip()
        decoded = decode_token(token)
        self._token = token
        self._bucket_id = decoded.bucket_id
        self._hostname = f"{decoded.hash_for_domain}.{DOMAIN_SUFFIX}"
        self._default_cache = cache
        self._telemetry = enable_telemetry
        self._creds = credential_cache_for(token, enable_telemetry)

    def _cache(self, cache: Optional[CacheOption]) -> Optional[CacheOption]:
        return self._default_cache if cache is None else cache

    def _visibility(self) -> str:
        """What the credentials response says, else public. The token carries no visibility bit."""
        peeked = self._creds.peek()
        return (peeked.visibility if peeked else None) or "public"

    def _public_url(self, path: str) -> Optional[str]:
        if self._visibility() == "private":
            return None
        # A listed key is whatever storage holds. One with a "." segment has no url: a browser would
        # resolve it to a different object, and it must not break the page it is on either.
        if not nameable(path):
            return None
        return f"https://{self._hostname}/{encode_key(path)}"

    def _blob(self, path: str, size: int, etag: str, uploaded_at: datetime) -> BlobObject:
        url = self._public_url(path)
        versioned = None if url is None else f"{url}?v={quote(etag, safe='')}"
        return BlobObject(path, url, versioned, size, etag, uploaded_at)

    def _credential_expired(self) -> bool:
        peeked = self._creds.peek()
        return peeked is not None and peeked.expires_at <= time.time()

    def _agent_headers(self) -> Dict[str, str]:
        return agent_headers(self._token, self._telemetry)

    @staticmethod
    def _signed(
        creds: Credentials,
        method: str,
        path: Optional[str],
        query: Optional[Mapping[str, str]],
        headers: Optional[Mapping[str, str]],
    ) -> Tuple[str, Dict[str, str]]:
        """The url and the signed headers for one object request."""
        parts = urlsplit(creds.endpoint)
        base_path = parts.path.rstrip("/")
        url_path = f"{base_path}/{creds.bucket}" + ("" if path is None else "/" + encode_key(path))
        qs = canonical_query(query)
        signed = sign_headers(
            access_key_id=creds.access_key_id,
            secret_access_key=creds.secret_access_key,
            session_token=creds.session_token,
            region=creds.region,
            method=method,
            host=normalized_host(creds.endpoint),
            path=url_path,
            query=query,
            headers=headers,
        )
        return f"{parts.scheme}://{parts.netloc}{url_path}{'?' + qs if qs else ''}", signed
