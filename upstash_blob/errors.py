import re
from typing import Any, Dict, List, Literal, Optional

BlobErrorCode = Literal[
    "not_found",
    "already_exists",
    "conflict",
    "content_type_not_allowed",
    "invalid_input",
    "too_large",
    "empty_body",
    "length_required",
    "signature_mismatch",
    "unauthorized",
    "forbidden",
    "rate_limited",
    "mint_backoff",
    "not_ready",
    "partial_delete",
    "move_left_a_copy",
    "invalid_content_type_pattern",
    "request_failed",
]

STATUS: Dict[str, int] = {
    "not_found": 404,
    "already_exists": 409,
    "conflict": 409,
    "content_type_not_allowed": 400,
    "invalid_input": 400,
    "too_large": 413,
    "empty_body": 400,
    "length_required": 411,
    "signature_mismatch": 403,
    "unauthorized": 401,
    "forbidden": 403,
    "rate_limited": 429,
    "mint_backoff": 429,
    "not_ready": 503,
    "partial_delete": 500,
    "move_left_a_copy": 500,
    "invalid_content_type_pattern": 500,
    "request_failed": 500,
}

_DEFAULT_MESSAGE: Dict[str, str] = {
    "not_found": "not found",
    "already_exists": "already exists",
    "conflict": "the object changed since it was read",
    "content_type_not_allowed": "content type not allowed",
    "invalid_input": "invalid input",
    "too_large": "too large",
    "empty_body": "empty body",
    "length_required": "length required",
    "signature_mismatch": "signature mismatch",
    "unauthorized": "unauthorized",
    "forbidden": "forbidden",
    "rate_limited": "rate limited",
    "mint_backoff": "the credential service asked for a backoff longer than a request can wait",
    "not_ready": "bucket is not ready",
    "partial_delete": "some paths were not deleted",
    "move_left_a_copy": "move left a copy at the source",
    "invalid_content_type_pattern": "invalid content type pattern",
    "request_failed": "request failed",
}

_DEFAULT_HINT: Dict[str, str] = {
    "signature_mismatch": "the upload did not match what was authorized, usually a different size or content type",
    "length_required": "pass size= or max_size= so the length is known before the first byte",
}

# A message that opens with an identifier (a MIME type, a file name) keeps its case:
# "Image/png is not allowed" names a type that does not exist.
_OPENS_WITH_IDENTIFIER = re.compile(r"[a-z][\w+*-]*[./][\w.+*/-]*", re.ASCII)


def _sentence_case(message: str) -> str:
    if _OPENS_WITH_IDENTIFIER.match(message):
        return message
    return message[:1].upper() + message[1:]


class BlobError(Exception):
    """
    Every failure the SDK raises. ``code`` is from a closed list, ``status`` is the HTTP status it
    maps to, and ``str(e)`` is a printable message that never carries a credential or a token.
    """

    code: BlobErrorCode
    status: int
    message: str
    hint: Optional[str]
    failed: Optional[List[str]]
    """partial_delete: the paths that survived."""
    etag: Optional[str]
    """already_exists: the etag of what is already there."""
    size: Optional[int]
    """already_exists: the size of what is already there."""
    retry_after: Optional[float]
    """mint_backoff and rate_limited: seconds the service asked the caller to wait."""
    reason: Optional[str]
    """The service's own words for a refusal, meant for the bucket owner (a suspension, a
    read-only bucket). Kept off ``message`` and ``to_dict()`` so it is never shown to end users."""

    def __init__(
        self,
        code: BlobErrorCode,
        message: Optional[str] = None,
        *,
        hint: Optional[str] = None,
        status: Optional[int] = None,
        failed: Optional[List[str]] = None,
        etag: Optional[str] = None,
        size: Optional[int] = None,
        retry_after: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> None:
        raw = message if message is not None else _DEFAULT_MESSAGE[code]
        hint = hint if hint is not None else _DEFAULT_HINT.get(code)
        base = _sentence_case(raw)
        text = f"{base} ({hint})" if hint and hint not in raw else base
        super().__init__(text)
        self.code = code
        self.message = text
        self.status = status if status is not None else STATUS[code]
        self.hint = hint
        self.failed = failed
        self.etag = etag
        self.size = size
        self.retry_after = retry_after
        self.reason = reason

    def __reduce__(self) -> Any:
        # Exception pickles as cls(*args), which would pass the message as the code.
        kwargs = {
            "hint": self.hint,
            "status": self.status,
            "failed": self.failed,
            "etag": self.etag,
            "size": self.size,
            "retry_after": self.retry_after,
            "reason": self.reason,
        }
        return (_restore, (self.code, self.message, kwargs))

    def __repr__(self) -> str:
        return f"BlobError(code={self.code!r}, status={self.status}, message={self.message!r})"

    def to_dict(self) -> Dict[str, Any]:
        """The JSON shape the TypeScript SDK's ``BlobError.fromJSON`` rebuilds in a browser."""
        out: Dict[str, Any] = {"code": self.code, "message": self.message, "status": self.status}
        if self.hint:
            out["hint"] = self.hint
        if self.failed:
            out["failed"] = self.failed
        if self.etag:
            out["etag"] = self.etag
        if self.size is not None:
            out["size"] = self.size
        if self.retry_after is not None:
            out["retryAfter"] = self.retry_after
        return out


def _restore(code: BlobErrorCode, message: str, kwargs: Dict[str, Any]) -> BlobError:
    return BlobError(code, message, **kwargs)
