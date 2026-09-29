import asyncio
import contextlib
import inspect
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, TypeVar, Union

import httpx

from upstash_blob import _body
from upstash_blob._core import (
    AGENT_TIMEOUT,
    CREDENTIAL_REJECTED,
    DEFAULT_READ_SECONDS,
    DEFAULT_TIMEOUT,
    MAX_PRESIGN_SECONDS,
    RETRIABLE_METHODS,
    RETRY_ATTEMPTS,
    TOKEN_ENV,
    BucketCore,
    Credentials,
    Head,
    attachment_disposition,
    backoff,
    check_etag,
    complete_body,
    credentials_from,
    delete_body,
    delete_errors,
    error_from_body,
    head_from_headers,
    is_conditional,
    mint_wait,
    parse_list,
    parse_parts,
    parse_uploads,
    presign_payload,
    presign_retriable,
    presigned_from,
    refused_to_send,
    safe_content_type,
    transport_error,
)
from upstash_blob._keys import decode_entities, encode_key, meta_headers, nameable, presignable_key, tag
from upstash_blob._multipart import MultipartOption, part_count, part_size_for, wants_multipart
from upstash_blob._sniff import check_content_type, expand_content_types
from upstash_blob._token import AGENT_URL
from upstash_blob._units import CacheOption, Duration, Size, cache_control, format_bytes, parse_duration, parse_size
from upstash_blob.errors import BlobError
from upstash_blob.types import (
    AsyncBlobStream,
    BlobDownload,
    BlobInfo,
    BlobObject,
    CompletedBlob,
    ListPage,
    MultipartUpload,
    S3Config,
    SignedReadUrl,
    SignedUploadUrl,
)

T = TypeVar("T")


class AsyncBucket:
    """
    Asynchronous client for one Upstash Blob bucket.

    >>> bucket = AsyncBucket.from_env()  # reads UPSTASH_BLOB_TOKEN
    >>> blob = await bucket.put("avatars/me.png", data, content_type="image/png")
    """

    def __init__(
        self,
        token: str,
        *,
        cache: Optional[CacheOption] = None,
        enable_telemetry: bool = True,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        """
        :param token: The bucket token from the Upstash console.
        :param cache: The ``Cache-Control`` written on every object this bucket stores. A per-call
            ``cache`` overrides it. See ``CacheOption``.
        :param enable_telemetry: Send the SDK version, runtime and platform on credential and signing
            requests. ``UPSTASH_DISABLE_TELEMETRY`` also turns it off.
        :param http_client: An ``httpx.AsyncClient`` to send requests with, for proxies or custom
            timeouts. The bucket does not close a client it was given.
        """
        self._core = BucketCore(token, cache, enable_telemetry)
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(timeout=DEFAULT_TIMEOUT)

    @classmethod
    def from_env(
        cls,
        name: str = TOKEN_ENV,
        *,
        cache: Optional[CacheOption] = None,
        enable_telemetry: bool = True,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> "AsyncBucket":
        """Reads the token from ``UPSTASH_BLOB_TOKEN``, or from the variable ``name``."""
        token = os.environ.get(name)
        if not token:
            raise TypeError(f"AsyncBucket.from_env: {name} is not set")
        return cls(token, cache=cache, enable_telemetry=enable_telemetry, http_client=http_client)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncBucket":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    # ------------------------------------------------------------- transport

    async def _mint(self) -> Credentials:
        attempt = 0
        while True:
            try:
                res = await self._client.post(
                    f"{AGENT_URL}/v1/credentials", headers=self._core._agent_headers(), timeout=AGENT_TIMEOUT
                )
            except httpx.TransportError as e:
                # A dropped connection is asked again; a hung agent is not. No cause: the request
                # being refused carries the bearer token.
                if isinstance(e, httpx.TimeoutException) or attempt >= 2:
                    raise transport_error(e, "the credential service") from None
                attempt += 1
                await asyncio.sleep(backoff(attempt, None))
                continue
            wait = mint_wait(res)
            if wait is not None and attempt < 3:
                attempt += 1
                await asyncio.sleep(wait)
                continue
            return credentials_from(res)

    async def _credentials(self) -> Credentials:
        return await self._core._creds.aget(self._mint)

    async def _fetch(
        self,
        method: str,
        path: Optional[str] = None,
        *,
        query: Optional[Mapping[str, str]] = None,
        headers: Optional[Mapping[str, str]] = None,
        content: Any = None,
        stream: bool = False,
    ) -> httpx.Response:
        """
        A 5xx, a 429 and a dropped connection are retried for the verbs that can be sent twice
        safely; multipart complete and the batch delete are POSTs and are not among them. A streamed
        body is never retried either: it has already been read once.
        """
        streaming = content is not None and not isinstance(content, (bytes, str))
        attempts = RETRY_ATTEMPTS if not streaming and method in RETRIABLE_METHODS else 1
        # A conditional write whose reply was lost may have landed, and sent again it would refuse
        # itself with a 412. Only answers that say it was not applied (429, 503) are asked again.
        conditional = is_conditional(headers)
        refreshed = False
        attempt = 0
        while True:
            attempt += 1
            creds = await self._credentials()
            url, signed = self._core._signed(creds, method, path, query, headers)
            request = self._client.build_request(method, url, headers=signed, content=content)
            try:
                res = await self._client.send(request, stream=stream)
            except httpx.LocalProtocolError:
                raise refused_to_send() from None
            except httpx.TransportError as e:
                # Nothing reached storage on a connect failure, so even a conditional write is safe to resend.
                unsent = isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout))
                if attempt >= attempts or (conditional and not unsent):
                    raise transport_error(e) from e
                await asyncio.sleep(backoff(attempt, None))
                continue
            if res.status_code == 403:
                # A HEAD carries no body to name the reason, so a credential that has visibly run out
                # counts as the same finding: re-mint once and ask again. A streamed body cannot be
                # sent again, but the next request still gets a fresh credential.
                await res.aread()
                if self._core._credential_expired() or CREDENTIAL_REJECTED.search(res.text):
                    self._core._creds.invalidate()
                    if not refreshed and not streaming:
                        refreshed = True
                        continue
                return res
            unapplied = (
                res.status_code in (429, 503) if conditional else res.status_code == 429 or res.status_code >= 500
            )
            if attempt < attempts and unapplied:
                await res.aclose()
                await asyncio.sleep(backoff(attempt, res.headers.get("retry-after")))
                continue
            return res

    async def _presign(
        self,
        method: str,
        path: str,
        expires_in: float,
        headers: Optional[Mapping[str, str]] = None,
        query: Optional[Mapping[str, str]] = None,
        part: Optional[Tuple[int, str]] = None,
    ) -> Tuple[str, datetime]:
        """
        A url for one operation on one object, signed by the agent. Never signed here: a url has to
        carry the temporary credential's session token, and R2 derives the credential's secret from
        it, so a url signed with it would hand its holder every object in the bucket.
        """
        payload = presign_payload(method, path, expires_in, headers, query, part)
        attempt = 0
        while True:
            attempt += 1
            try:
                res = await self._client.post(
                    f"{AGENT_URL}/v1/presign",
                    headers=self._core._agent_headers(),
                    json=payload,
                    timeout=AGENT_TIMEOUT,
                )
            except httpx.TransportError as e:
                # A hung agent is not asked again: three timeouts would hold a request for 30 s.
                if isinstance(e, httpx.TimeoutException) or attempt >= RETRY_ATTEMPTS:
                    raise BlobError("request_failed", "could not reach the signing service", status=502) from None
                await asyncio.sleep(backoff(attempt, None))
                continue
            if attempt < RETRY_ATTEMPTS and presign_retriable(res):
                await asyncio.sleep(backoff(attempt, res.headers.get("retry-after")))
                continue
            return presigned_from(res)

    async def _head(self, path: str) -> Optional[Head]:
        res = await self._fetch("HEAD", path)
        if res.status_code == 404:
            return None
        if not res.is_success:
            raise error_from_body(res.status_code, res.text)
        return head_from_headers(res.headers)

    async def _create_multipart(self, path: str, headers: Mapping[str, str]) -> str:
        res = await self._fetch("POST", path, query={"uploads": ""}, headers=headers)
        if not res.is_success:
            raise error_from_body(res.status_code, res.text)
        upload_id = tag(res.text, "UploadId")
        if not upload_id:
            raise BlobError("request_failed", "CreateMultipartUpload returned no upload id", status=502)
        return decode_entities(upload_id)

    async def _complete_multipart(self, path: str, upload_id: str, parts: Sequence[Tuple[int, str]]) -> str:
        res = await self._fetch(
            "POST",
            path,
            query={"uploadId": upload_id},
            headers={"content-type": "application/xml"},
            content=complete_body(parts),
        )
        if not res.is_success:
            raise error_from_body(res.status_code, res.text)
        # A 200 can still carry an error document.
        if tag(res.text, "Code"):
            raise error_from_body(500, res.text)
        return decode_entities(tag(res.text, "ETag") or "")

    async def _list_parts(self, path: str, upload_id: str) -> List[Tuple[int, str]]:
        out: List[Tuple[int, str]] = []
        marker: Optional[str] = None
        while True:
            query = {"uploadId": upload_id, "max-parts": "1000"}
            if marker:
                query["part-number-marker"] = marker
            res = await self._fetch("GET", path, query=query)
            if not res.is_success:
                raise error_from_body(res.status_code, res.text)
            parts, marker = parse_parts(res.text)
            out.extend(parts)
            if marker is None:
                return out

    async def _abort_multipart(self, path: str, upload_id: str) -> None:
        res = await self._fetch("DELETE", path, query={"uploadId": upload_id})
        if not res.is_success and res.status_code != 404:
            raise error_from_body(res.status_code, res.text)

    # ---------------------------------------------------------------- urls

    async def public_url(self, path: str) -> Optional[str]:
        """
        The public object URL. None on a private bucket, where ``signed_read_url()`` is the way in.
        Whether the bucket is private comes from the backend, not the token, so the first call on a
        fresh bucket fetches credentials; after that it is local.
        """
        encode_key(path)
        await self._credentials()
        return self._core._public_url(path)

    # ----------------------------------------------------------------- put

    async def put(
        self,
        path: str,
        body: _body.PutBody,
        *,
        content_type: Optional[str] = None,
        content_types: Optional[Sequence[str]] = None,
        max_size: Optional[Size] = None,
        cache: Optional[CacheOption] = None,
        metadata: Optional[Mapping[str, str]] = None,
        size: Optional[Size] = None,
        allow_overwrite: bool = True,
        if_unchanged: Optional[str] = None,
        multipart: Optional[MultipartOption] = None,
    ) -> CompletedBlob:
        """
        Store ``body`` at ``path``.

        :param body: bytes, str, a ``pathlib.Path``, a binary file object, or an iterable of bytes.
        :param content_type: Defaults to a guess from the file name, else ``application/octet-stream``.
        :param content_types: Allowed types (``image/*``, ``application/pdf``); the leading bytes
            are checked against the declared type too.
        :param max_size: Refuse a body over this size (``'20mb'``).
        :param size: Declared length for an iterable whose size is not otherwise known. Without it
            the body is buffered, up to ``max_size``, to learn its length.
        :param allow_overwrite: ``False`` refuses to replace an existing object: ``already_exists``.
        :param if_unchanged: An etag: the write fails with ``conflict`` if the object changed.
        :param multipart: Where the multipart path starts (16 MB by default). A size moves the line,
            ``True`` always parts, ``False`` never does. ``allow_overwrite=False`` and
            ``if_unchanged`` are single-PUT only.
        """
        encode_key(path)
        allowed = None if content_types is None else expand_content_types(content_types)
        limit = None if max_size is None else parse_size(max_size, "max_size")
        resolved = _body.aresolve(body)
        try:
            data: _body.AsyncData = resolved.data
            length = resolved.size
            ctype = safe_content_type(content_type or resolved.content_type or "application/octet-stream")
            if if_unchanged is not None:
                check_etag(if_unchanged)

            if length is None and size is not None:
                length = parse_size(size, "size")
            if limit is not None and length is not None and length > limit:
                raise BlobError(
                    "too_large", f"the body is {format_bytes(length)}, over the {format_bytes(limit)} limit"
                )
            if length is None:
                if limit is None:
                    raise BlobError("length_required")
                data = await _body.aread_all(data, limit)
                length = len(data)
            if allowed is not None:
                head, data = await _body.apeek(data)
                check_content_type(ctype, head, allowed)
            if limit is not None:
                data = _body.alimit(data, limit)

            # Resolved before the header is written: visibility decides public vs private on it.
            await self._credentials()
            object_headers = {
                "content-type": ctype,
                "cache-control": cache_control(self._core._cache(cache), self._core._visibility()),
                **meta_headers(metadata),
            }

            conditional = allow_overwrite is False or if_unchanged is not None
            if multipart is True and conditional:
                raise BlobError(
                    "invalid_input", "multipart: allow_overwrite=False and if_unchanged are single-PUT only"
                )
            if length > 0 and wants_multipart(multipart, length) and not conditional:
                return await self._put_multipart(path, data, length, object_headers, ctype)

            headers = {**object_headers, "content-length": str(length)}
            if allow_overwrite is False:
                headers["if-none-match"] = "*"
            if if_unchanged is not None:
                headers["if-match"] = if_unchanged

            res = await self._fetch(
                "PUT", path, headers=headers, content=_body.aexact(data, length) if length > 0 else b""
            )
            if res.status_code == 412:
                if allow_overwrite is False:
                    existing = await self._head(path)
                    raise BlobError(
                        "already_exists",
                        f"{path} already exists",
                        etag=existing.etag if existing else None,
                        size=existing.size if existing else None,
                    )
                raise BlobError("conflict")
            if not res.is_success:
                raise error_from_body(res.status_code, res.text)
            blob = self._core._blob(path, length, res.headers.get("etag", ""), datetime.now(timezone.utc))
            return CompletedBlob(**vars(blob), content_type=ctype)
        finally:
            if resolved.close:
                resolved.close()

    async def _put_multipart(
        self, path: str, data: _body.AsyncData, size: int, headers: Mapping[str, str], content_type: str
    ) -> CompletedBlob:
        """
        One part at a time: a part is buffered whole so it can be retried. An error anywhere aborts
        the upload rather than leaving parts behind that nothing can see.
        """
        part_size = part_size_for(size)
        expected = part_count(size, part_size)
        upload_id = await self._create_multipart(path, headers)
        try:
            parts: List[Tuple[int, str]] = []
            sent = 0
            async for chunk in _body.aparts_of(data, part_size):
                n = len(parts) + 1
                if n > expected:
                    raise BlobError("invalid_input", f"body is longer than the declared {size} bytes")
                res = await self._fetch(
                    "PUT",
                    path,
                    query={"partNumber": str(n), "uploadId": upload_id},
                    headers={"content-length": str(len(chunk))},
                    content=chunk,
                )
                if not res.is_success:
                    raise error_from_body(res.status_code, res.text)
                etag = res.headers.get("etag")
                if not etag:
                    raise BlobError("request_failed", f"part {n} landed without an etag", status=502)
                parts.append((n, etag))
                sent += len(chunk)
            if sent != size:
                raise BlobError("invalid_input", f"body was {sent} bytes, {size} were declared")
            etag = await self._complete_multipart(path, upload_id, parts)
        except BaseException:
            # Nothing lists an incomplete upload, so a failure that left one behind is invisible billing.
            with contextlib.suppress(Exception):
                await self._abort_multipart(path, upload_id)
            raise
        blob = self._core._blob(path, size, etag, datetime.now(timezone.utc))
        return CompletedBlob(**vars(blob), content_type=content_type)

    # ---------------------------------------------------------------- read

    async def get(self, path: str) -> BlobDownload:
        """The blob's facts and its bytes. Raises ``not_found`` rather than returning None."""
        res = await self._fetch("GET", path)
        if not res.is_success:
            raise error_from_body(res.status_code, res.text)
        h = head_from_headers(res.headers)
        blob = self._core._blob(path, h.size, h.etag, h.uploaded_at)
        return BlobDownload(**vars(blob), content_type=h.content_type, metadata=h.metadata, content=res.content)

    @contextlib.asynccontextmanager
    async def stream(self, path: str) -> AsyncIterator[AsyncBlobStream]:
        """
        Like ``get()``, with the bytes left on the wire until read:

        >>> async with bucket.stream("video.mp4") as blob:
        ...     async for chunk in blob.aiter_bytes():
        ...         out.write(chunk)
        """
        res = await self._fetch("GET", path, stream=True)
        try:
            if not res.is_success:
                await res.aread()
                raise error_from_body(res.status_code, res.text)
            h = head_from_headers(res.headers)
            blob = self._core._blob(path, h.size, h.etag, h.uploaded_at)
            yield AsyncBlobStream(**vars(blob), content_type=h.content_type, metadata=h.metadata, _response=res)
        finally:
            await res.aclose()

    async def info(self, path: str) -> BlobInfo:
        """The same record as ``get()``, without the bytes. Raises ``not_found``."""
        h = await self._head(path)
        if h is None:
            raise BlobError("not_found", f"{path} not found")
        blob = self._core._blob(path, h.size, h.etag, h.uploaded_at)
        return BlobInfo(**vars(blob), content_type=h.content_type, metadata=h.metadata)

    async def exists(self, path: str) -> bool:
        return await self._head(path) is not None

    async def list(
        self, *, prefix: Optional[str] = None, limit: Optional[int] = None, cursor: Optional[str] = None
    ) -> ListPage:
        """
        One page of keys, sizes, etags and urls, without metadata. ``cursor`` on the page is set only
        while more remains.
        """
        query = {"list-type": "2"}
        if prefix:
            query["prefix"] = prefix
        if limit is not None:
            query["max-keys"] = str(min(max(int(limit), 1), 1000))
        if cursor:
            query["continuation-token"] = cursor
        res = await self._fetch("GET", query=query)
        if not res.is_success:
            raise error_from_body(res.status_code, res.text)
        blobs, next_cursor = parse_list(res.text, self._core._blob)
        return ListPage(blobs, next_cursor)

    async def signed_read_url(
        self,
        path: str,
        *,
        expires_in: Optional[Duration] = None,
        download_as: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> SignedReadUrl:
        """
        A link that reads one object, for a private bucket.

        :param expires_in: How long the link should live, at most 10 minutes (default 5).
            ``expires_at`` says when the returned link actually expires.
        :param download_as: Save the response as this filename instead of displaying it inline.
        :param content_type: What storage answers with as ``Content-Type``.
        """
        seconds = DEFAULT_READ_SECONDS if expires_in is None else parse_duration(expires_in, "expires_in")
        query: Dict[str, str] = {}
        if download_as is not None:
            query["response-content-disposition"] = attachment_disposition(download_as)
        if content_type is not None:
            query["response-content-type"] = safe_content_type(content_type)
        url, expires_at = await self._presign("GET", path, seconds, query=query)
        return SignedReadUrl(url, expires_at)

    async def signed_upload_url(
        self,
        path: str,
        *,
        expires_in: Optional[Duration] = None,
        content_type: Optional[str] = None,
        cache: Optional[CacheOption] = None,
        metadata: Optional[Mapping[str, str]] = None,
        size: Optional[Size] = None,
        allow_overwrite: bool = True,
    ) -> SignedUploadUrl:
        """
        A url someone else can PUT one object to, with ``headers`` sent verbatim.

        :param expires_in: How long the link should live, at most 10 minutes (the default).
        :param content_type: The ``Content-Type`` the upload must send, and what it is stored as.
        :param size: Pin the body's exact length.
        :param allow_overwrite: ``False`` refuses the upload if something is already at the path.
        """
        presignable_key(path)
        seconds = MAX_PRESIGN_SECONDS if expires_in is None else parse_duration(expires_in, "expires_in")
        await self._credentials()
        headers = {
            "content-type": safe_content_type(content_type or "application/octet-stream"),
            "cache-control": cache_control(self._core._cache(cache), self._core._visibility()),
            **meta_headers(metadata),
        }
        if size is not None:
            headers["content-length"] = str(parse_size(size, "size"))
        if allow_overwrite is False:
            headers["if-none-match"] = "*"
        url, expires_at = await self._presign("PUT", path, seconds, headers=headers)
        return SignedUploadUrl(url, headers, expires_at)

    # --------------------------------------------------------------- write

    async def delete(
        self,
        paths: Union[str, Sequence[str], None] = None,
        *,
        prefix: Optional[str] = None,
        all: bool = False,
    ) -> None:
        """
        Delete one path, a list of paths, or everything under ``prefix``. A missing path is success.
        ``prefix=""`` means the whole bucket and needs ``all=True``. A list that is only partly
        deleted raises ``partial_delete`` with the survivors on ``failed``.
        """
        if (paths is None) == (prefix is None):
            raise BlobError("invalid_input", "delete: pass a path, a list of paths, or prefix=")
        if isinstance(paths, str):
            res = await self._fetch("DELETE", paths)
            if not res.is_success and res.status_code != 404:
                raise error_from_body(res.status_code, res.text)
            return
        if paths is not None:
            paths = list(paths)
            for p in paths:
                encode_key(p)
            failed: List[str] = []
            for i in range(0, len(paths), 1000):
                failed += await self._delete_batch(paths[i : i + 1000])
            if failed:
                raise BlobError(
                    "partial_delete", f"{len(failed)} of {len(paths)} paths were not deleted", failed=failed
                )
            return
        assert prefix is not None
        if prefix == "" and all is not True:
            raise BlobError(
                "invalid_input",
                'delete(prefix="") would delete every object in the bucket',
                hint='pass delete(prefix="", all=True) if that is what you mean',
            )
        failed = []
        cursor: Optional[str] = None
        while True:
            page = await self.list(prefix=prefix, limit=1000, cursor=cursor)
            if page.blobs:
                failed += await self._delete_batch([b.path for b in page.blobs])
            cursor = page.cursor
            if not cursor:
                break
        if failed:
            raise BlobError("partial_delete", f"{len(failed)} paths under {prefix} were not deleted", failed=failed)

    async def _delete_batch(self, paths: Sequence[str]) -> List[str]:
        if not paths:
            return []
        body = delete_body(paths)
        res = await self._fetch("POST", query={"delete": ""}, headers={"content-type": "application/xml"}, content=body)
        if not res.is_success:
            raise error_from_body(res.status_code, res.text)
        # S3 answers 200 with errors inside; a survivor is the truth, so ask again rather than trust the list.
        # A key no url can name (a "." segment) cannot be asked about, so it counts as a survivor.
        return [p for p in delete_errors(res.text) if not nameable(p) or await self.exists(p)]

    # ----------------------------------------------------------- multipart

    async def list_multipart_uploads(self, *, prefix: Optional[str] = None) -> List[MultipartUpload]:
        """
        Multipart uploads started and never completed or aborted. They are billed storage that
        ``list()`` cannot see, and a bucket cannot be deleted while one exists.
        """
        out: List[MultipartUpload] = []
        key_marker: Optional[str] = None
        id_marker: Optional[str] = None
        while True:
            query = {"uploads": "", "max-uploads": "1000"}
            if prefix:
                query["prefix"] = prefix
            if key_marker:
                query["key-marker"] = key_marker
            if id_marker:
                query["upload-id-marker"] = id_marker
            res = await self._fetch("GET", query=query)
            if not res.is_success:
                raise error_from_body(res.status_code, res.text)
            page, key_marker, id_marker = parse_uploads(res.text)
            out += page
            if not key_marker:
                return out

    async def abort_multipart_upload(self, upload: MultipartUpload) -> None:
        """
        Throws away an incomplete upload and every part that landed for it. Missing is success,
        which is why it takes the record ``list_multipart_uploads()`` returned rather than two
        strings: a swapped pair would abort nothing and report that it worked.
        """
        if not isinstance(upload, MultipartUpload):
            raise BlobError("invalid_input", "abort_multipart_upload: expected a MultipartUpload")
        if not upload.upload_id:
            raise BlobError("invalid_input", "abort_multipart_upload: upload_id is required")
        encode_key(upload.path)
        await self._abort_multipart(upload.path, upload.upload_id)

    async def abort_stale_multipart_uploads(
        self, *, older_than: Duration, prefix: Optional[str] = None
    ) -> List[MultipartUpload]:
        """List plus abort, for a cron. Returns what it aborted."""
        cutoff = time.time() - parse_duration(older_than, "older_than")
        stale = [
            u
            for u in await self.list_multipart_uploads(prefix=prefix)
            if u.initiated_at and u.initiated_at.timestamp() <= cutoff and nameable(u.path)
        ]
        for u in stale:
            await self._abort_multipart(u.path, u.upload_id)
        return stale

    # ----------------------------------------------------------- copy/move

    async def copy(
        self,
        src: str,
        dst: str,
        *,
        content_type: Optional[str] = None,
        cache: Optional[CacheOption] = None,
        metadata: Optional[Mapping[str, str]] = None,
    ) -> BlobObject:
        """
        Without options the destination carries the source's content type, cache-control and
        metadata. Storage rewrites the three together or not at all, so when any one is given the
        other two are read off the source first and sent back unchanged. ``metadata`` replaces the
        source's outright rather than merging into it.
        """
        creds = await self._credentials()
        headers = {"x-amz-copy-source": f"/{creds.bucket}/{encode_key(src)}"}
        if content_type is not None or cache is not None or metadata is not None:
            source = await self._head(src)
            if source is None:
                raise BlobError("not_found", f"{src} not found")
            visibility = self._core._visibility()
            headers["x-amz-metadata-directive"] = "REPLACE"
            headers["content-type"] = source.content_type if content_type is None else safe_content_type(content_type)
            if cache is not None:
                headers["cache-control"] = cache_control(cache, visibility)
            else:
                headers["cache-control"] = source.cache_control or cache_control(self._core._default_cache, visibility)
            headers.update(meta_headers(source.metadata if metadata is None else metadata))
        res = await self._fetch("PUT", dst, headers=headers)
        if not res.is_success:
            raise error_from_body(res.status_code, res.text)
        if tag(res.text, "Code"):
            raise error_from_body(500, res.text)
        h = await self._head(dst)
        if h is None:
            raise BlobError("not_found", f"{dst} was not found after copy")
        return self._core._blob(dst, h.size, h.etag, h.uploaded_at)

    async def move(
        self,
        src: str,
        dst: str,
        *,
        content_type: Optional[str] = None,
        cache: Optional[CacheOption] = None,
        metadata: Optional[Mapping[str, str]] = None,
    ) -> BlobObject:
        """
        A copy followed by a delete of the source: storage has no rename. A failed delete raises
        ``move_left_a_copy``, with the destination kept.
        """
        blob = await self.copy(src, dst, content_type=content_type, cache=cache, metadata=metadata)
        try:
            await self.delete(src)
        except Exception as e:
            raise BlobError("move_left_a_copy", f"moved {src} to {dst} but could not delete the source") from e
        return blob

    async def update_json(
        self,
        path: str,
        fn: Callable[[Any], Any],
        *,
        cache: Optional[CacheOption] = None,
        metadata: Optional[Mapping[str, str]] = None,
        max_attempts: int = 6,
    ) -> CompletedBlob:
        """
        Read-modify-write over a JSON document: ``fn(prev)`` gets the parsed document, or None when
        there is none, and returns the next one. Written with If-Match on the etag that was read, or
        If-None-Match: * when nothing was there; a write that lost the race is re-read and re-run
        after a short jittered pause, up to ``max_attempts`` times, then raises ``conflict``.
        Existing metadata is kept unless ``metadata`` is given.
        """
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
            raise BlobError("invalid_input", "max_attempts must be a positive integer")
        last: Optional[BlobError] = None
        for attempt in range(1, max_attempts + 1):
            prev: Any = None
            etag: Optional[str] = None
            existing: Optional[Dict[str, str]] = None
            try:
                got = await self.get(path)
                # An object that exists but is empty reads as None: there is no document either way.
                prev = json.loads(got.content) if got.content else None
                etag = got.etag
                existing = got.metadata
            except BlobError as e:
                if e.code != "not_found":
                    raise
            nxt = fn(prev)
            if inspect.isawaitable(nxt):
                nxt = await nxt
            try:
                return await self.put(
                    path,
                    json.dumps(nxt, separators=(",", ":")),
                    content_type="application/json",
                    cache=cache,
                    metadata=metadata if metadata is not None else existing,
                    allow_overwrite=etag is not None,
                    if_unchanged=etag,
                )
            except BlobError as e:
                if e.code not in ("conflict", "already_exists"):
                    raise
                last = e
                if attempt < max_attempts:
                    await asyncio.sleep(backoff(attempt, None, 0.025))
        plural = "" if max_attempts == 1 else "s"
        raise BlobError("conflict", f"{path} kept changing across {max_attempts} attempt{plural}") from last

    # -------------------------------------------------------- escape hatch

    async def s3(self) -> S3Config:
        """
        Short-lived S3 credentials for the bucket, for boto3 or any S3 client:

        >>> cfg = await bucket.s3()
        >>> s3 = boto3.client("s3", **cfg.client_kwargs())
        >>> s3.list_objects_v2(Bucket=cfg.bucket)

        They expire at ``cfg.expires_at``; call ``s3()`` again for fresh ones.
        """
        c = await self._credentials()
        return S3Config(
            endpoint_url=c.endpoint,
            region_name="auto",
            bucket=c.bucket,
            aws_access_key_id=c.access_key_id,
            aws_secret_access_key=c.secret_access_key,
            aws_session_token=c.session_token,
            expires_at=datetime.fromtimestamp(c.expires_at, timezone.utc),
        )
