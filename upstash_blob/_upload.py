"""
What the sync and async upload handlers share: everything but the I/O.

The handler speaks the wire protocol of ``@upstash/blob``'s browser client and React hooks, so a
React app pointed at a Python backend uploads straight to storage: phase 'begin' presigns, the
browser PUTs, phase 'end' records the object. Under the route's ``multipart`` threshold that is one
presigned PUT and the object exists from the moment the last byte lands, so a refusal at 'end'
deletes it. Over the threshold the file goes up in parts and the object does not exist until 'end'
completes it.
"""

import base64
import binascii
import dataclasses
import hashlib
import hmac
import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from upstash_blob._core import safe_content_type
from upstash_blob._keys import presignable_key
from upstash_blob._multipart import MultipartOption
from upstash_blob._sniff import SNIFF_BYTES, check_content_type, expand_content_types
from upstash_blob._token import to_base64url
from upstash_blob._units import CacheOption, Size, over_limit, parse_size
from upstash_blob.errors import BlobError
from upstash_blob.types import CompletedBlob

logger = logging.getLogger("upstash_blob")

UPLOAD_MARKER = "upstash-upload"
"""
Written onto every single-PUT object, signed into the url so only this SDK can set it, and read back
to answer the one question a multipart upload answers by construction: are the bytes at this path
the ones THIS upload put there? A presigned PUT is written by the browser, so the object at the path
may be anybody's: a stale token's, a concurrent upload's, or one that was there all along.
"""
UPLOAD_MARKER_HEADER = f"x-amz-meta-{UPLOAD_MARKER}"

UPLOAD_PROTOCOL = 1
"""
The version of the upload route protocol, which this handler speaks with ``@upstash/blob``'s browser
client and React hooks. It is not the package version, and it is the same number blob-js's
``uploadHandler`` speaks: the contract lives in blob-js ``src/shared/protocol.ts``.

Every upload route JSON body carries it as a top-level ``protocol`` key. The client sends it on every
POST; the server on the GET constraints document and on every 2xx answer to a POST. Error answers
carry none. Missing, or anything but an integer of at least 1, is 1. Every change is additive and
nothing is refused over the number, so at 1 the server does not read the client's.

1: phases begin, parts, end and cancel; the GET constraints document.
"""

PARTS_PER_BATCH = 16
TOKEN_TTL_MS = 7 * 86_400_000
LIMITS_MAX_AGE = 60

MAX_BODY = 1_000_000
"""The largest request body a route reads. The protocol's own bodies are a few kilobytes."""

ROUTE_NAME = re.compile(r"[A-Za-z_][\w-]*", re.ASCII)
"""Route names live in a url query, so they are limited to what reads back as one word."""


class _Unset:
    def __repr__(self) -> str:
        return "UNSET"

    def __bool__(self) -> bool:
        return False


UNSET: Any = _Unset()
"""A route option left out inherits the handler's. ``None`` clears it instead."""

_ABSENT: Any = object()


# ------------------------------------------------------------ public types


@dataclass(frozen=True)
class FileInfo:
    """What the browser declared about a file before a byte of it was sent."""

    name: str
    type: str
    size: int


@dataclass(frozen=True)
class BeforeUploadArgs:
    ctx: Any
    """Whatever ``context`` returned for this request."""
    route: str
    """The route this file was sent to. '' when the handler mounts no named routes."""
    request: Any
    """The framework's own request object."""
    file: FileInfo
    input: Any
    """What the browser sent as ``input``, validated by the route's ``input``."""


@dataclass
class BeforeUploadResult:
    path: str
    cache: Optional[CacheOption] = None
    """The ``Cache-Control`` this object is stored with, overriding the bucket default."""
    metadata: Optional[Mapping[str, str]] = None
    content_types: Optional[Sequence[str]] = None
    """May narrow the route's ``content_types`` for this upload, never widen them."""
    max_size: Optional[Size] = None
    """May lower the route's ``max_size`` for this upload, never raise it."""
    state: Any = None
    """
    Carried to ``on_upload_complete`` and ``on_error``. It rides in the completion token, which is
    signed but readable by the browser: JSON only, and never a secret.
    """


@dataclass(frozen=True)
class UploadCompleteArgs(CompletedBlob):
    """The stored object, flat, plus what this route knows about it."""

    ctx: Any
    route: str
    request: Any
    file: FileInfo
    """What the browser declared before a byte was sent: the name is the only place it survives."""
    upload_id: str
    """
    Identifies this upload, and is the same on every delivery: phase 'end' is at-least-once, so use
    it as an idempotency key.
    """
    multipart_upload_id: Optional[str]
    """Storage's own multipart id. None when the file went up as a single PUT."""
    metadata: Dict[str, str]
    state: Any


@dataclass(frozen=True)
class UploadErrorArgs:
    ctx: Any
    """None when ``context`` itself failed, or for an unknown route."""
    route: str
    request: Any
    error: BaseException
    file: Optional[FileInfo] = None
    path: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    state: Any = None


@dataclass(frozen=True)
class UploadResponse:
    """What a handler answers with, for an adapter to turn into the framework's own response."""

    status: int
    headers: Dict[str, str]
    body: bytes = field(repr=False)


Validator = Any
"""A pydantic model or ``TypeAdapter``, or any callable that returns the value or raises ValueError."""

Callback = Callable[[Any], Any]


@dataclass(frozen=True)
class UploadRoute:
    """
    One route of a handler. Every option left out inherits the handler's; ``content_types=None``
    and ``max_size=None`` clear a limit the handler set.
    """

    bucket: Any = None
    """A ``Bucket`` for ``UploadHandler``, an ``AsyncBucket`` for ``AsyncUploadHandler``."""
    content_types: Union[Sequence[str], None, _Unset] = UNSET
    max_size: Union[Size, None, _Unset] = UNSET
    multipart: Optional[MultipartOption] = None
    input: Optional[Validator] = None
    on_before_upload: Optional[Callback] = None
    on_upload_complete: Optional[Callback] = None
    on_error: Optional[Callback] = None


# ------------------------------------------------------------- constraints


@dataclass(frozen=True)
class Constraints:
    content_types: Optional[List[str]]
    max_size: Optional[int]


def resolve_constraints(content_types: Optional[Sequence[str]], max_size: Optional[Size]) -> Constraints:
    return Constraints(
        None if content_types is None else expand_content_types(content_types),
        None if max_size is None else parse_size(max_size, "max_size"),
    )


def enforce(c: Constraints, file: FileInfo, head: Optional[bytes]) -> None:
    if c.max_size is not None and file.size > c.max_size:
        raise BlobError("too_large", f"{file.name} is {over_limit(file.size, c.max_size)}")
    if c.content_types is not None:
        check_content_type(file.type, head, c.content_types)


def narrow(route: Constraints, decided: BeforeUploadResult, file: FileInfo, head: Optional[bytes]) -> None:
    """What on_before_upload returned may tighten the route's limits for one upload, never loosen them."""
    if decided.content_types is None and decided.max_size is None:
        return
    narrowed = resolve_constraints(decided.content_types, decided.max_size)
    if narrowed.max_size is not None and route.max_size is not None and narrowed.max_size > route.max_size:
        raise TypeError(f"on_before_upload widened max_size ({narrowed.max_size} > {route.max_size})")
    if narrowed.content_types is not None and route.content_types is not None:
        wider = [t for t in narrowed.content_types if t not in route.content_types]
        if wider:
            raise TypeError(f"on_before_upload widened content_types ({', '.join(wider)})")
    enforce(narrowed, file, head)


def constraints_response(c: Constraints) -> Tuple[bytes, str]:
    """
    What GET on a route answers: the limits it enforces, so a file picker is filled from the same
    list that does the refusing. Short and revalidated, since the limits change with a deploy.
    """
    served: Dict[str, Any] = {}
    if c.content_types is not None:
        served["contentTypes"] = c.content_types
    if c.max_size is not None:
        served["maxSize"] = c.max_size
    body = json.dumps({"protocol": UPLOAD_PROTOCOL, "constraints": served}, separators=(",", ":")).encode()
    return body, f'"{hashlib.sha256(body).hexdigest()[:16]}"'


def route_id(c: Constraints, has_input: bool, name: str) -> str:
    """Which route a completion token belongs to: a token minted by one is not spendable at another."""
    raw = json.dumps([name, c.content_types, c.max_size, has_input], separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ------------------------------------------------------------------- routes


@dataclass(frozen=True)
class Mounted:
    """One route with the handler's defaults applied, ready to serve."""

    name: str
    bucket: Any
    id: str
    constraints: Constraints
    multipart: Optional[MultipartOption]
    input: Optional[Validator]
    on_before_upload: Callback
    on_upload_complete: Optional[Callback]
    on_error: Optional[Callback]
    limits: bytes
    limits_etag: str


def _pick(base: Any, own: Any) -> Any:
    return (None if base is UNSET else base) if own is UNSET else own


def mount_routes(
    *,
    routes: Optional[Mapping[str, UploadRoute]],
    defaults: UploadRoute,
    endpoint: Optional[str],
    bucket_type: type,
    env_bucket: Callable[[str, bool], Any],
) -> Tuple[Dict[str, Mounted], Optional[Mounted]]:
    """The route table, and the one route when the handler names none."""
    named = routes is not None
    if named and not routes:
        raise TypeError("routes is empty: name at least one route, or omit routes to make the handler the route")
    table: Dict[str, Mounted] = {}
    for name, route in (routes or {"": UploadRoute()}).items():
        where = f"upload route {name!r}" if named else "the upload handler"
        if named and (not isinstance(name, str) or not ROUTE_NAME.fullmatch(name)):
            raise TypeError(f"upload route names must match {ROUTE_NAME.pattern}: {name!r} does not")
        if not isinstance(route, UploadRoute):
            raise TypeError(f"{where}: expected an UploadRoute(...), got {type(route).__name__}")
        bucket = route.bucket or defaults.bucket or env_bucket(name, named)
        if not isinstance(bucket, bucket_type):
            raise TypeError(f"{where}: bucket must be a {bucket_type.__name__}, got {type(bucket).__name__}")
        on_before_upload = route.on_before_upload or defaults.on_before_upload
        if on_before_upload is None:
            raise TypeError(f"{where} has no on_before_upload: write one on the route or on the handler")
        multipart = route.multipart if route.multipart is not None else defaults.multipart
        # Parsed here, not per request: an unparseable size is a startup error like every other
        # option, not a 500 raised after on_before_upload has already written the app's row.
        if multipart is not None and not isinstance(multipart, bool):
            parse_size(multipart, "multipart")
        validator = route.input if route.input is not None else defaults.input
        constraints = resolve_constraints(
            _pick(defaults.content_types, route.content_types), _pick(defaults.max_size, route.max_size)
        )
        limits, etag = constraints_response(constraints)
        table[name] = Mounted(
            name=name,
            bucket=bucket,
            # An endpoint separates two handlers sharing one bucket.
            id=route_id(constraints, validator is not None, f"{endpoint}?route={name}" if endpoint else name),
            constraints=constraints,
            multipart=multipart,
            input=validator,
            on_before_upload=on_before_upload,
            on_upload_complete=route.on_upload_complete or defaults.on_upload_complete,
            on_error=route.on_error or defaults.on_error,
            limits=limits,
            limits_etag=etag,
        )
    return table, None if named else table[""]


def pick_route(
    table: Dict[str, Mounted], sole: Optional[Mounted], route: Optional[str]
) -> Tuple[str, Optional[Mounted]]:
    name = route or ""
    # A name on a handler with no routes is a client bound to a different handler, and serving it
    # here would upload through a route it never asked for.
    if sole is not None:
        return name, None if name else sole
    return name, table.get(name) if name else None


# ------------------------------------------------------------ request body


@dataclass
class Details:
    """As much as the request had reached before it failed, for on_error."""

    file: Optional[FileInfo] = None
    path: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    state: Any = None


def header(headers: Optional[Mapping[str, str]], name: str) -> Optional[str]:
    """Every framework's header object is case-insensitive; a plain dict is not."""
    if not headers:
        return None
    value = headers.get(name)
    if value is None:
        value = next((v for k, v in headers.items() if k.lower() == name), None)
    return value


def check_request(body: bytes, content_type: Optional[str]) -> None:
    """Runs before ``context``: what it refuses never reaches a callback."""
    # The browser client always sends JSON. Refusing anything else means a cross-site form or a
    # no-cors fetch, which cannot set this type without a CORS preflight, never reaches a callback
    # with the visitor's cookies.
    if (content_type or "").split(";", 1)[0].strip().lower() != "application/json":
        raise BlobError("invalid_input", "expected a JSON body (content-type: application/json)", status=415)
    if len(body) > MAX_BODY:
        raise BlobError("too_large", "the request body is too large for an upload route")


def read_body(body: bytes) -> Dict[str, Any]:
    try:
        parsed = json.loads(body)
    except (ValueError, RecursionError):
        parsed = None
    if not isinstance(parsed, dict) or not isinstance(parsed.get("phase"), str):
        raise BlobError("invalid_input", "expected a JSON body with a phase")
    return parsed


def mapped_answer(mapped: Any, native: Optional[type]) -> Any:
    """
    What on_error returned, if it is an answer: a BlobError, an UploadResponse, or the framework's
    own response. Anything else (a logger's event id, say) leaves the default answer alone.
    """
    if isinstance(mapped, BlobError):
        return error_response(mapped)
    if isinstance(mapped, UploadResponse) or (native is not None and isinstance(mapped, native)):
        return mapped
    return None


def _int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


_MAX_SAFE_INTEGER = 2**53 - 1
"""The largest size the browser can declare exactly, and a float can hold for the size math."""


def read_file(raw: Any) -> FileInfo:
    size = _int(raw.get("size")) if isinstance(raw, dict) else None
    if (
        not isinstance(raw, dict)
        or not isinstance(raw.get("name"), str)
        or size is None
        or not 0 <= size <= _MAX_SAFE_INTEGER
    ):
        raise BlobError("invalid_input", "file must be { name, type, size }")
    declared = raw.get("type")
    bare = declared.lower().split(";", 1)[0].strip() if isinstance(declared, str) else ""
    # It becomes the object's Content-Type, so it has to be a media type and nothing else.
    return FileInfo(raw["name"], safe_content_type(bare) if bare else "application/octet-stream", size)


def read_head(raw: Any) -> Optional[bytes]:
    """The leading bytes a browser may send at 'begin', base64. Anything unreadable is simply no head."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        # Clamped before decoding, not after: raw is whatever the client chose to send.
        return base64.b64decode(raw[: math.ceil(SNIFF_BYTES / 3) * 4], validate=True)[:SNIFF_BYTES]
    except (binascii.Error, ValueError):
        return None


def read_from(raw: Any) -> int:
    n = _int(raw)
    return n if n is not None and n >= 1 else 1


def check_landed(raw: List[Any]) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for p in raw:
        n = _int(p.get("n")) if isinstance(p, dict) else None
        if n is None or not isinstance(p.get("etag"), str):
            raise BlobError("invalid_input", "parts must be [{ n, etag }]")
        out.append((n, p["etag"]))
    return out


def validate_input(validator: Optional[Validator], raw: Any) -> Any:
    """A route with no validator refuses input rather than dropping it silently."""
    if validator is None:
        if raw is not _ABSENT:
            raise BlobError("invalid_input", "this route takes no input")
        return None
    value = None if raw is _ABSENT else raw
    if isinstance(validator, type) and hasattr(validator, "model_validate"):
        run = validator.model_validate
    elif hasattr(validator, "validate_python"):
        run = validator.validate_python
    else:
        run = validator
    try:
        return run(value)
    except ValueError as e:
        # pydantic's ValidationError is a ValueError.
        errors = getattr(e, "errors", None)
        if callable(errors):
            issues = [
                (".".join(str(p) for p in i.get("loc", ())) + ": " if i.get("loc") else "") + str(i.get("msg"))
                for i in errors()
            ]
            raise BlobError("invalid_input", "; ".join(issues) or "invalid input") from None
        raise BlobError("invalid_input", str(e) or "invalid input") from None


def input_of(body: Mapping[str, Any]) -> Any:
    return body["input"] if "input" in body else _ABSENT


def check_decided(decided: Any) -> BeforeUploadResult:
    if not isinstance(decided, BeforeUploadResult) or not isinstance(decided.path, str):
        raise TypeError(f"on_before_upload must return BeforeUploadResult(path=...), got {type(decided).__name__}")
    try:
        presignable_key(decided.path)
    except ValueError as e:
        # unique_path keeps the browser's filename as given, so a "../" in the path can be the client's.
        raise BlobError("invalid_input", str(e)) from e
    return decided


def check_metadata(metadata: Mapping[str, str]) -> None:
    if any(isinstance(k, str) and k.lower() == UPLOAD_MARKER for k in metadata):
        raise BlobError("invalid_input", f"metadata.{UPLOAD_MARKER} is reserved by the SDK")


# ------------------------------------------------------- completion tokens


@dataclass(frozen=True)
class Grant:
    """What a completion token carries: everything 'parts', 'end' and 'cancel' need, signed."""

    bucket_id: str
    route_id: str
    id: str
    path: str
    name: str
    type: str
    size: int
    headers: Dict[str, str]
    """Pinned into the signature: content-type, cache-control, x-amz-meta-*."""
    state: Any
    exp: int
    """Unix ms."""
    upload_id: Optional[str]
    """Storage's multipart id. None for a single PUT: there is no upload to complete or abort."""
    part_size: int
    """The part size for a multipart, and the whole file for a single PUT, so one part covers it."""

    @property
    def file(self) -> FileInfo:
        return FileInfo(self.name, self.type, self.size)


def _mac(body: str, key: str) -> str:
    digest = hmac.new(f"upstash-blob-completion:{key}".encode(), body.encode(), hashlib.sha256).digest()
    return to_base64url(digest)


def sign_grant(g: Grant, key: str) -> str:
    """Signed, not encrypted: the payload is readable in devtools, so state carries an id, never a secret."""
    payload: Dict[str, Any] = {
        "v": 1,
        "b": g.bucket_id,
        "r": g.route_id,
        "id": g.id,
        "path": g.path,
        "n": g.name,
        "type": g.type,
        "size": g.size,
        "headers": g.headers,
        "ctx": g.state,
        "exp": g.exp,
        "partSize": g.part_size,
    }
    if g.upload_id is not None:
        payload["uploadId"] = g.upload_id
    try:
        raw = json.dumps(payload, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        raise TypeError("on_before_upload: state must be JSON-serializable") from None
    body = to_base64url(raw.encode())
    return f"{body}.{_mac(body, key)}"


def verify_grant(token: str, key: str) -> Optional[Grant]:
    body, dot, sig = token.partition(".")
    try:
        if not dot or not hmac.compare_digest(sig.encode(), _mac(body, key).encode()):
            return None
    except UnicodeEncodeError:
        # A lone surrogate is not a token this SDK minted.
        return None
    try:
        p = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        upload_id = p.get("uploadId")
        grant = Grant(
            bucket_id=p["b"],
            route_id=p["r"],
            id=p["id"],
            path=p["path"],
            name=p["n"],
            type=p["type"],
            size=p["size"],
            headers=p["headers"],
            state=p.get("ctx"),
            exp=p["exp"],
            upload_id=upload_id,
            part_size=p["partSize"],
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        return None
    ok = (
        p.get("v") == 1
        and all(isinstance(s, str) for s in (grant.bucket_id, grant.route_id, grant.id, grant.path, grant.name))
        and isinstance(grant.headers, dict)
        and all(_int(n) is not None for n in (grant.size, grant.exp, grant.part_size))
        and grant.part_size > 0
        and (upload_id is None or isinstance(upload_id, str))
    )
    return grant if ok else None


def check_grant(token: Any, key: str, bucket_id: str, route: str, details: Details) -> Grant:
    if not isinstance(token, str):
        raise BlobError("invalid_input", "completionToken is required")
    g = verify_grant(token, key)
    if g is None or g.bucket_id != bucket_id or g.route_id != route:
        raise BlobError("forbidden", "completionToken is not valid for this route")
    if g.exp < time.time() * 1000:
        raise BlobError("forbidden", "completionToken has expired")
    details.path = g.path
    details.state = g.state
    return g


def token_expiry() -> int:
    return int(time.time() * 1000) + TOKEN_TTL_MS


# --------------------------------------------------------------- responses


def answer(value: Dict[str, Any]) -> UploadResponse:
    """
    Every successful answer carries the protocol of the server that wrote it. On a deploy, instances
    of two releases answer one upload side by side, so the client reads each answer by its own number.
    """
    return json_response({**value, "protocol": UPLOAD_PROTOCOL})


def json_response(value: Any, status: int = 200) -> UploadResponse:
    body = json.dumps(value, separators=(",", ":"), default=_to_json).encode()
    return UploadResponse(status, {"content-type": "application/json"}, body)


def _to_json(value: Any) -> Any:
    """What on_upload_complete may return beyond plain JSON: a pydantic model, a dataclass, a datetime."""
    dump = getattr(value, "model_dump", None)
    if callable(dump) and not isinstance(value, type):
        return dump(mode="json")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"on_upload_complete returned a {type(value).__name__}, which is not JSON-serializable")


def limits_response(m: Mounted, if_none_match: Optional[str]) -> UploadResponse:
    headers = {
        "content-type": "application/json",
        "cache-control": f"public, max-age={LIMITS_MAX_AGE}",
        "etag": m.limits_etag,
    }
    if if_none_match == m.limits_etag:
        return UploadResponse(304, headers, b"")
    return UploadResponse(200, headers, m.limits)


def not_allowed() -> UploadResponse:
    return json_response(BlobError("invalid_input", "an upload route answers GET and POST", status=405).to_dict(), 405)


def iso(dt: datetime) -> str:
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def blob_json(b: CompletedBlob) -> Dict[str, Any]:
    out: Dict[str, Any] = {"path": b.path}
    if b.url is not None:
        out["url"] = b.url
        out["versionedUrl"] = b.versioned_url
    out.update(size=b.size, etag=b.etag, uploadedAt=iso(b.uploaded_at), contentType=b.content_type)
    return out


def end_json(b: CompletedBlob, data: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"blob": blob_json(b)}
    if data is not None:
        out["data"] = data
    return out


# Every refusal leaves a route as BlobError.to_dict(), so the browser rebuilds it with its code intact.
_CODE_FOR_STATUS: Dict[int, Any] = {
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    409: "conflict",
    411: "length_required",
    413: "too_large",
    429: "rate_limited",
}


def _status_of(e: BaseException) -> Optional[int]:
    # FastAPI and Starlette say status_code, Werkzeug says code, most others say status.
    for attr in ("status_code", "code", "status"):
        s = getattr(e, attr, None)
        if isinstance(s, int) and not isinstance(s, bool) and 400 <= s < 600:
            return s
    return None


def _message_of(e: BaseException) -> str:
    for attr in ("detail", "description", "message"):
        m = getattr(e, attr, None)
        if isinstance(m, str) and m:
            return m
    return "request failed"


def error_response(e: BlobError) -> UploadResponse:
    return json_response(e.to_dict(), e.status)


def default_answer(e: Exception) -> UploadResponse:
    """A BlobError, or an app error that named an HTTP status. Anything else is a bug and stays a raise."""
    if isinstance(e, BlobError):
        return error_response(e)
    status = _status_of(e)
    if status is None:
        raise e
    # An auth check that raised its own 401 reaches the browser as code 'unauthorized', so a caller
    # can tell a dead session from a rejected file without reading status numbers.
    code = _CODE_FOR_STATUS.get(status, "request_failed")
    return error_response(BlobError(code, _message_of(e), status=status))


def error_args(ctx: Any, route: str, request: Any, error: BaseException, d: Details) -> UploadErrorArgs:
    return UploadErrorArgs(ctx, route, request, error, d.file, d.path, d.metadata, d.state)


def complete_args(
    blob: CompletedBlob, ctx: Any, m: Mounted, request: Any, g: Grant, metadata: Dict[str, str]
) -> UploadCompleteArgs:
    return UploadCompleteArgs(
        **{f.name: getattr(blob, f.name) for f in dataclasses.fields(CompletedBlob)},
        ctx=ctx,
        route=m.name,
        request=request,
        file=g.file,
        upload_id=g.id,
        multipart_upload_id=g.upload_id,
        metadata=metadata,
        state=g.state,
    )


def plan_parts(g: Grant, start: int) -> List[Tuple[int, int]]:
    """``(part_number, length)`` for the next batch of part urls, from ``start``."""
    count = max(1, math.ceil(g.size / g.part_size))
    last = min(count, start + PARTS_PER_BATCH - 1)
    return [(n, g.size - g.part_size * (count - 1) if n == count else g.part_size) for n in range(start, last + 1)]


def part_count(g: Grant) -> int:
    return max(1, math.ceil(g.size / g.part_size))
