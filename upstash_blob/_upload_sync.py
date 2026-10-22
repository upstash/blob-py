import inspect
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Mapping, Optional, Sequence

from upstash_blob._core import MAX_PRESIGN_SECONDS, TOKEN_ENV
from upstash_blob._keys import meta_headers
from upstash_blob._multipart import MultipartOption, part_size_for, wants_multipart
from upstash_blob._sync import Bucket
from upstash_blob._units import Size, cache_control
from upstash_blob._upload import (
    UPLOAD_MARKER,
    UPLOAD_MARKER_HEADER,
    BeforeUploadArgs,
    Callback,
    CorsProbe,
    Details,
    Grant,
    Mounted,
    UploadRoute,
    Validator,
    answer,
    check_decided,
    check_grant,
    check_landed,
    check_metadata,
    check_owner_option,
    check_request,
    complete_args,
    default_answer,
    end_json,
    enforce,
    error_args,
    error_response,
    header,
    input_of,
    limits_response,
    logger,
    mapped_answer,
    mount_routes,
    narrow,
    not_allowed,
    owner_string,
    part_count,
    pick_route,
    plan_parts,
    read_body,
    read_file,
    read_from,
    read_head,
    sign_grant,
    token_expiry,
    validate_input,
)
from upstash_blob.errors import BlobError
from upstash_blob.types import CompletedBlob


def _settle(value: Any) -> Any:
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value):
            value.close()
        raise TypeError("UploadHandler callbacks are synchronous; use AsyncUploadHandler for async callbacks")
    return value


class UploadHandler:
    """
    One upload endpoint for ``@upstash/blob``'s browser client and React hooks. The browser sends
    the bytes straight to storage; this route decides where they go and records them.

    With no ``routes`` it is the route, and the client calls ``useUpload()``. With ``routes`` it
    mounts several at one endpoint, named in the query: ``useUpload('avatar')``. Every option here
    is a default a route may replace.

    Mount it with ``upstash_blob.flask``, ``upstash_blob.django``, or call ``handle()`` directly.

    >>> uploads = UploadHandler(
    ...     content_types=["image/*"],
    ...     max_size="5mb",
    ...     context=require_user,
    ...     on_before_upload=lambda a: BeforeUploadResult(path=f"avatars/{a.ctx.id}"),
    ... )
    """

    def __init__(
        self,
        *,
        bucket: Optional[Bucket] = None,
        content_types: Optional[Sequence[str]] = None,
        max_size: Optional[Size] = None,
        multipart: Optional[MultipartOption] = None,
        input: Optional[Validator] = None,
        context: Optional[Callback] = None,
        owner: Optional[Callback] = None,
        on_before_upload: Optional[Callback] = None,
        on_upload_complete: Optional[Callback] = None,
        on_error: Optional[Callback] = None,
        routes: Optional[Mapping[str, UploadRoute]] = None,
        endpoint: Optional[str] = None,
    ) -> None:
        """
        :param bucket: Every route's bucket unless it names its own. Omitted, the handler builds one
            from ``UPSTASH_BLOB_TOKEN``, once.
        :param content_types: What the routes accept: ``["image/*", "application/pdf"]``. Checked
            against the declared type and the file's first bytes.
        :param max_size: The largest file the routes accept: ``"20MB"``. Decimal: ``"32MB"`` is
            32,000,000 bytes; for 32 MiB pass ``"32MiB"`` or ``32 * 1024 * 1024``.
        :param multipart: Where a file stops going up as one PUT and starts going up in parts (16 MB
            by default). Parts buy pause, resume and per-part retry.
        :param input: Validates what the browser sends as ``input``: a pydantic model, a
            ``TypeAdapter``, or a callable that raises ValueError.
        :param context: Runs once per POST, before the route. Its return is ``ctx`` in every
            callback. Raise to refuse: ``BlobError("unauthorized")``, or any exception with a
            ``status_code`` of 401. The place for the session check.
        :param owner: Who an upload belongs to, read from ``ctx``: ``owner=lambda user: user.id``.
            Signed into the upload at 'begin'; a later request whose ``ctx`` names someone else is a
            403 before storage or any callback is touched, so one signed-in user cannot finish,
            re-sign or cancel another's upload. None or "" is a 401. Needs ``context``.
        :param on_before_upload: Decides the path. Returns ``BeforeUploadResult``.
        :param on_upload_complete: Runs once the object is stored, and only after the route verified
            its signed completion token and that the stored object has the declared size: a forged
            or expired token, or one from another route or ``endpoint``, is a 403. What it returns
            reaches the browser as ``upload.blob.data``. Raising refuses: the object is deleted,
            unless it cannot be identified as this upload's (a retried multipart 'end', or a newer
            upload replaced it), in which case it is kept and logged. It may run more than once, or
            never if the browser stops before 'end': use ``upload_id`` as an idempotency key.
        :param on_error: Sees every refusal. Return a ``BlobError`` to answer with it, a framework
            response to answer with that, or None to leave the answer alone.
        :param routes: Named routes, each an ``UploadRoute``.
        :param endpoint: Where this handler is mounted. Only needed to separate two handlers on one
            bucket whose route names would otherwise collide.
        """
        check_owner_option(owner, context)
        self._context = context
        self._owner = owner
        self._on_error = on_error
        self._cors = CorsProbe()
        self._env_bucket: Optional[Bucket] = None
        defaults = UploadRoute(
            bucket=bucket,
            content_types=content_types,
            max_size=max_size,
            multipart=multipart,
            input=input,
            on_before_upload=on_before_upload,
            on_upload_complete=on_upload_complete,
            on_error=on_error,
        )
        self._table, self._sole = mount_routes(
            routes=routes, defaults=defaults, endpoint=endpoint, bucket_type=Bucket, env_bucket=self._from_env
        )

    def _from_env(self, name: str, named: bool) -> Bucket:
        # Built at most once: two routes falling back to the environment are one bucket.
        if self._env_bucket is None:
            if not os.environ.get(TOKEN_ENV):
                where = f"upload route {name!r} has no bucket" if named else "the upload handler has no bucket"
                raise TypeError(f"{where} and {TOKEN_ENV} is not set: set it, or pass bucket=")
            self._env_bucket = Bucket.from_env()
        return self._env_bucket

    def handle(
        self,
        method: str,
        *,
        request: Any = None,
        route: Optional[str] = None,
        body: bytes = b"",
        headers: Optional[Mapping[str, str]] = None,
        native: Optional[type] = None,
    ) -> Any:
        """
        Serves one request from any framework. Returns an ``UploadResponse``, or the response
        ``on_error`` returned in its place.

        :param method: GET serves the route's limits; POST runs the upload protocol.
        :param request: The framework's request, handed to every callback as ``request``.
        :param route: The ``route`` query parameter.
        :param body: The raw POST body.
        :param headers: The request headers; ``content-type`` and ``if-none-match`` are read.
        :param native: The framework's response class, which ``on_error`` may return to answer with.
        """
        verb = method.upper()
        if verb not in ("GET", "POST"):
            return not_allowed()
        name, m = pick_route(self._table, self._sole, route)
        if m is None:
            return self._not_found(name, request, native)
        if verb == "GET":
            return limits_response(m, header(headers, "if-none-match"))
        details = Details()
        try:
            check_request(body, header(headers, "content-type"))
        except Exception as e:
            return self._answer(e, m, None, request, details, native)
        ctx = None
        if self._context is not None:
            try:
                ctx = _settle(self._context(request))
            except Exception as e:
                return self._answer(e, m, None, request, details, native)
        try:
            data = read_body(body)
            phase = data["phase"]
            if phase == "begin":
                return answer(self._begin(m, ctx, request, data, details, header(headers, "origin")))
            if phase == "parts":
                return answer(self._parts(m, ctx, data, details))
            if phase == "end":
                return answer(self._end(m, ctx, request, data, details))
            if phase == "cancel":
                return answer(self._cancel(m, ctx, data, details))
            raise BlobError("invalid_input", f"unknown phase {phase}")
        except Exception as e:
            return self._answer(e, m, ctx, request, details, native)

    def _not_found(self, name: str, request: Any, native: Optional[type]) -> Any:
        # Never the names it does mount, and never a 500: an unknown route is a 404 like any other.
        error = BlobError("not_found", "unknown upload route")
        if self._on_error is not None:
            mapped = mapped_answer(_settle(self._on_error(error_args(None, name, request, error, Details()))), native)
            if mapped is not None:
                return mapped
        return error_response(error)

    def _answer(self, e: Exception, m: Mounted, ctx: Any, request: Any, d: Details, native: Optional[type]) -> Any:
        if m.on_error is not None:
            mapped = mapped_answer(_settle(m.on_error(error_args(ctx, m.name, request, e, d))), native)
            if mapped is not None:
                return mapped
        return default_answer(e)

    # ------------------------------------------------------------- phases

    def _owner_of(self, ctx: Any) -> Optional[str]:
        if self._owner is None:
            return None
        return owner_string(_settle(self._owner(ctx)))

    def _probe(self, bucket: Bucket, origin: Optional[str], url: str, headers: Any) -> None:
        claimed = self._cors.claim(origin)
        if claimed is None:
            return

        def run() -> None:
            try:
                res = bucket._client.request(**CorsProbe.request(claimed, url, headers))
                allowed = CorsProbe.verdict(claimed, res)
            except Exception:
                allowed = None
            self._cors.settle(claimed, allowed)

        threading.Thread(target=run, name="upstash-blob-cors-probe", daemon=True).start()

    def _begin(
        self, m: Mounted, ctx: Any, request: Any, body: Dict[str, Any], d: Details, origin: Optional[str]
    ) -> Dict[str, Any]:
        file = read_file(body.get("file"))
        d.file = file
        if file.size == 0:
            raise BlobError("empty_body")
        # The browser sends the file's first bytes so a mislabelled file is refused here, before a
        # multipart exists and before on_before_upload has written anything down. Ergonomics, not a
        # control: the part bodies never reach this server.
        head = read_head(body.get("head"))
        enforce(m.constraints, file, head)
        value = validate_input(m.input, input_of(body))
        # Before on_before_upload and storage: a request with no owner leaves no row and no multipart behind.
        owner = self._owner_of(ctx)

        decided = check_decided(_settle(m.on_before_upload(BeforeUploadArgs(ctx, m.name, request, file, value))))
        d.path, d.state = decided.path, decided.state
        narrow(m.constraints, decided, file, head)

        bucket: Bucket = m.bucket
        bucket._credentials()
        core = bucket._core
        metadata = dict(decided.metadata or {})
        d.metadata = metadata
        check_metadata(metadata)
        headers = {
            "content-type": file.type,
            "cache-control": cache_control(core._cache(decided.cache), core._visibility()),
            **meta_headers(metadata),
        }
        # The path is on_before_upload's to decide, so the multipart cannot be created before it
        # runs; a create that fails afterwards is told back to the app through on_error.
        multipart = wants_multipart(m.multipart, file.size)
        upload = str(uuid.uuid4())
        upload_id: Optional[str] = None
        if multipart:
            upload_id = bucket._create_multipart(decided.path, headers)
        else:
            headers[UPLOAD_MARKER_HEADER] = upload
        grant = Grant(
            bucket_id=core._bucket_id,
            route_id=m.id,
            id=upload,
            path=decided.path,
            name=file.name,
            type=file.type,
            size=file.size,
            headers=headers,
            state=decided.state,
            exp=token_expiry(),
            upload_id=upload_id,
            # One part covering the whole file is how a single PUT is expressed: the browser runs the
            # same loop over one url, and only this side knows the url is an object PUT.
            part_size=part_size_for(file.size) if multipart else file.size,
            owner=owner,
        )
        try:
            token = sign_grant(grant, core._signing_secret)
            parts = self._presign(bucket, grant, 1)
        except BaseException:
            # The browser never gets the token it would cancel with, so a multipart nobody can sign
            # parts for is aborted here rather than left billed until something sweeps it.
            if upload_id is not None:
                try:
                    bucket._abort_multipart(decided.path, upload_id)
                except Exception:
                    logger.exception("upload %r could not be signed and its multipart was not aborted", decided.path)
            raise
        if parts:
            self._probe(bucket, origin, parts[0]["url"], parts[0].get("headers"))
        return {
            "completionToken": token,
            "path": decided.path,
            "upload": {"partSize": grant.part_size, "multipart": multipart, "parts": parts},
        }

    def _parts(self, m: Mounted, ctx: Any, body: Dict[str, Any], d: Details) -> Dict[str, Any]:
        bucket: Bucket = m.bucket
        g = check_grant(
            body.get("completionToken"),
            bucket._core._signing_secret,
            bucket._core._bucket_id,
            m.id,
            d,
            self._owner_of(ctx),
        )
        # Nothing lands early on a single PUT: it is one object write, so a resumed upload runs it again.
        landed = bucket._list_parts(g.path, g.upload_id) if g.upload_id else []
        return {
            "partSize": g.part_size,
            "size": g.size,
            "multipart": g.upload_id is not None,
            "parts": self._presign(bucket, g, read_from(body.get("from"))),
            "landed": [{"n": n, "etag": etag} for n, etag in landed],
        }

    def _end(self, m: Mounted, ctx: Any, request: Any, body: Dict[str, Any], d: Details) -> Dict[str, Any]:
        bucket: Bucket = m.bucket
        g = check_grant(
            body.get("completionToken"),
            bucket._core._signing_secret,
            bucket._core._bucket_id,
            m.id,
            d,
            self._owner_of(ctx),
        )
        d.file = g.file

        expected = part_count(g)
        raw = body.get("parts")
        sent: List[Any] = raw if isinstance(raw, list) else []
        if len(sent) != expected and g.upload_id:
            landed = bucket._list_parts(g.path, g.upload_id)
        else:
            landed = check_landed(sent)
        if len(landed) != expected:
            raise BlobError("invalid_input", f"expected {expected} parts, {len(landed)} landed")
        # What discard() checks before deleting: that the object still holds this upload's bytes.
        # For a multipart it is the etag complete answered with; a retried 'end' has none, and then
        # a refusal leaves the object rather than delete one it cannot identify.
        completed: Optional[str] = None
        if g.upload_id:
            try:
                completed = bucket._complete_multipart(g.path, g.upload_id, landed)
            except BlobError as e:
                # NoSuchUpload after the object landed is a retried 'end': the earlier call completed it.
                if e.code != "not_found" or bucket._head(g.path) is None:
                    raise

        head = bucket._head(g.path)
        if head is None:
            raise BlobError("not_found", "the upload never landed")
        if not g.upload_id:
            # A single PUT stored the object itself, so a retried 'end' is idempotent for free, but
            # nothing says the object at this path came from this upload except the signed marker.
            if head.metadata.get(UPLOAD_MARKER) != g.id:
                raise BlobError("not_found", "the upload never landed")
            del head.metadata[UPLOAD_MARKER]
            completed = head.etag
        # On a public bucket the object is already served: a refusal from here on bounds the
        # exposure to a few round trips, it does not undo it. That is why the checks that can run
        # earlier run at 'begin'.
        if head.size != g.size:
            self._discard(bucket, g.path, completed)
            raise BlobError("signature_mismatch", f"stored {head.size} bytes, {g.size} were declared")

        base = bucket._core._blob(g.path, head.size, head.etag, head.uploaded_at)
        blob = CompletedBlob(
            base.path, base.url, base.versioned_url, base.size, base.etag, base.uploaded_at, head.content_type
        )
        d.metadata = head.metadata
        data = None
        if m.on_upload_complete is not None:
            try:
                data = _settle(m.on_upload_complete(complete_args(blob, ctx, m, request, g, head.metadata)))
            except BaseException:
                # The object exists only if on_upload_complete returned.
                self._discard(bucket, g.path, completed)
                raise
        return end_json(blob, data)

    def _cancel(self, m: Mounted, ctx: Any, body: Dict[str, Any], d: Details) -> Dict[str, Any]:
        bucket: Bucket = m.bucket
        g = check_grant(
            body.get("completionToken"),
            bucket._core._signing_secret,
            bucket._core._bucket_id,
            m.id,
            d,
            self._owner_of(ctx),
        )
        if g.upload_id:
            bucket._abort_multipart(g.path, g.upload_id)
            return {"ok": True}
        # A single PUT has nothing to abort. A cancel after the bytes landed is a whole object no
        # callback accepted; the marker tells the two apart and is the whole check, so a cancel
        # cannot be aimed at an object this upload did not write.
        head = bucket._head(g.path)
        if head is not None and head.metadata.get(UPLOAD_MARKER) == g.id:
            self._discard(bucket, g.path, head.etag)
        return {"ok": True}

    def _discard(self, bucket: Bucket, path: str, etag: Optional[str]) -> None:
        """
        Deletes the object a refused upload left behind, unless it is no longer that object. R2 has
        no conditional delete, so this re-reads the etag first: on a stable path a later upload may
        already have replaced these bytes.
        """
        try:
            # No etag is no permission: an orphan costs storage, a blind delete costs someone's file.
            if not etag:
                logger.error("refused upload %r could not be identified and was left stored", path)
                return
            current = bucket._head(path)
            if current is None:
                return
            if current.etag != etag:
                logger.warning("refused upload %r was replaced before it could be deleted; left alone", path)
                return
            res = bucket._fetch("DELETE", path)
            if not res.is_success and res.status_code != 404:
                raise BlobError("request_failed", f"storage responded {res.status_code}", status=502)
        except Exception:
            # The refusal is the answer; a failed delete must not replace it, nor pass silently.
            logger.exception("refused upload %r could not be deleted and is still stored", path)

    def _presign(self, bucket: Bucket, g: Grant, start: int) -> List[Dict[str, Any]]:
        # A single PUT writes the object itself, so what a multipart pins at create (content type,
        # cache-control, metadata, the marker) is signed into this url instead and sent with it.
        if not g.upload_id:
            if start > 1:
                return []
            url, _ = bucket._presign(
                "PUT", g.path, MAX_PRESIGN_SECONDS, headers={**g.headers, "content-length": str(g.size)}
            )
            return [{"n": 1, "url": url, "headers": dict(g.headers)}]
        upload_id = g.upload_id

        def one(n: int, length: int) -> Dict[str, Any]:
            url, _ = bucket._presign(
                "PUT", g.path, MAX_PRESIGN_SECONDS, headers={"content-length": str(length)}, part=(n, upload_id)
            )
            return {"n": n, "url": url}

        # One agent request per url, so a batch is signed concurrently rather than in sixteen round trips.
        batch = plan_parts(g, start)
        if not batch:
            return []
        with ThreadPoolExecutor(max_workers=len(batch)) as pool:
            return list(pool.map(lambda p: one(*p), batch))
