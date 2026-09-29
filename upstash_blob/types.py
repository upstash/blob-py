import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional

import httpx


@dataclass(frozen=True)
class BlobObject:
    """The record of a stored blob: the fields a bucket listing carries. Never its bytes."""

    path: str
    url: Optional[str]
    """None on a private bucket: no public host serves it, so use ``signed_read_url()``."""
    versioned_url: Optional[str]
    """``f"{url}?v={etag}"``: the fix for a stable path that gets overwritten. None when url is."""
    size: int
    etag: str
    uploaded_at: datetime


@dataclass(frozen=True)
class CompletedBlob(BlobObject):
    """A completed write, whose stored content type is known."""

    content_type: str


@dataclass(frozen=True)
class BlobInfo(BlobObject):
    content_type: str
    metadata: Dict[str, str]


@dataclass(frozen=True)
class BlobDownload(BlobInfo):
    """What ``get()`` answers: the blob's facts and its bytes."""

    content: bytes = field(repr=False)

    def text(self, encoding: str = "utf-8") -> str:
        return self.content.decode(encoding)

    def json(self) -> Any:
        return json.loads(self.content)


@dataclass(frozen=True)
class BlobStream(BlobInfo):
    """What ``stream()`` yields: the blob's facts, with the bytes still on the wire."""

    _response: httpx.Response = field(repr=False, compare=False)

    def iter_bytes(self, chunk_size: Optional[int] = None) -> Iterator[bytes]:
        return self._response.iter_bytes(chunk_size)

    def read(self) -> bytes:
        return self._response.read()


@dataclass(frozen=True)
class AsyncBlobStream(BlobInfo):
    """What ``AsyncBucket.stream()`` yields: the blob's facts, with the bytes still on the wire."""

    _response: httpx.Response = field(repr=False, compare=False)

    def aiter_bytes(self, chunk_size: Optional[int] = None) -> AsyncIterator[bytes]:
        return self._response.aiter_bytes(chunk_size)

    async def read(self) -> bytes:
        return await self._response.aread()


@dataclass(frozen=True)
class ListPage:
    blobs: List[BlobObject]
    cursor: Optional[str]
    """Set only while more remains: pass it back as ``cursor=`` for the next page."""


@dataclass(frozen=True)
class SignedReadUrl:
    url: str
    expires_at: datetime
    """When the link stops working."""


@dataclass(frozen=True)
class SignedUploadUrl:
    url: str
    headers: Dict[str, str]
    """
    Sent verbatim with the PUT. They are part of the signature, so anything the caller changes,
    drops or adds to them is a 403 rather than a header the client got to choose.
    """
    expires_at: datetime
    """When the link stops working."""


@dataclass(frozen=True)
class MultipartUpload:
    """A multipart upload that was started and never completed or aborted."""

    path: str
    upload_id: str
    initiated_at: Optional[datetime]
    """None when storage sent a date that does not parse; such an upload is never treated as stale."""


@dataclass(frozen=True)
class S3Config:
    """
    Short-lived S3 credentials for the bucket. They expire at ``expires_at``; call ``s3()`` again
    for fresh ones. ``client_kwargs()`` is what ``boto3.client("s3", ...)`` takes.
    """

    endpoint_url: str
    region_name: str
    bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str = field(repr=False)
    aws_session_token: str = field(repr=False)
    expires_at: datetime

    def client_kwargs(self) -> Dict[str, str]:
        return {
            "endpoint_url": self.endpoint_url,
            "region_name": self.region_name,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "aws_session_token": self.aws_session_token,
        }
