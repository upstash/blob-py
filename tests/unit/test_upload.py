import base64
import inspect
import json
import time
from typing import Any, Dict, Iterator, List, Optional, Union

import httpx
import pytest

from tests.fake import TOKEN, Fake, Obj, etag_of
from upstash_blob import (
    AsyncBucket,
    AsyncUploadHandler,
    BeforeUploadArgs,
    BeforeUploadResult,
    BlobError,
    Bucket,
    UploadCompleteArgs,
    UploadErrorArgs,
    UploadHandler,
    UploadResponse,
    UploadRoute,
)
from upstash_blob._upload import sign_grant, verify_grant

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
MIB = 1024 * 1024

Handler = Union[UploadHandler, AsyncUploadHandler]


class Client:
    """Drives either handler the way the browser client does, and plays the browser's PUTs."""

    def __init__(self, fake: Fake, kind: str) -> None:
        self.fake = fake
        self.kind = kind
        self.bucket: Any = (
            Bucket(TOKEN, http_client=httpx.Client(transport=httpx.MockTransport(fake.handler)))
            if kind == "sync"
            else AsyncBucket(TOKEN, http_client=httpx.AsyncClient(transport=httpx.MockTransport(fake.ahandler)))
        )

    def handler(self, **options: Any) -> Handler:
        options.setdefault("bucket", self.bucket)
        return UploadHandler(**options) if self.kind == "sync" else AsyncUploadHandler(**options)

    def route(self, **options: Any) -> UploadRoute:
        return UploadRoute(**options)

    async def call(
        self,
        h: Handler,
        method: str = "POST",
        body: Any = None,
        route: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        request: Any = "req",
        native: Optional[type] = None,
    ) -> Any:
        raw = body if isinstance(body, bytes) else json.dumps(body).encode() if body is not None else b""
        hdrs = {"content-type": "application/json", **(headers or {})}
        res = h.handle(method, request=request, route=route, body=raw, headers=hdrs, native=native)
        return await res if inspect.isawaitable(res) else res

    async def post(self, h: Handler, body: Any, route: Optional[str] = None, **kw: Any) -> Any:
        res = await self.call(h, "POST", body, route, **kw)
        assert isinstance(res, UploadResponse)
        return res.status, json.loads(res.body) if res.body else None

    async def begin(self, h: Handler, name: str = "a.png", size: int = len(PNG), **extra: Any) -> Any:
        file = {"name": name, "type": extra.pop("type", "image/png"), "size": size}
        kw = {k: extra.pop(k) for k in ("request", "headers") if k in extra}
        return await self.post(h, {"phase": "begin", "file": file, **extra}, extra.pop("route", None), **kw)

    def put(self, begun: Dict[str, Any], data: bytes) -> None:
        """The browser's PUT: the object with the headers signed into its url."""
        part = begun["upload"]["parts"][0]
        self.fake.objects[begun["path"]] = Obj(data, dict(part["headers"]), etag_of(data), time.time())

    def put_parts(self, path: str, data: bytes, part_size: int) -> List[Dict[str, Any]]:
        uid = next(u for u, (k, *_) in self.fake.uploads.items() if k == path)
        landed = []
        for i in range(0, len(data), part_size):
            chunk = data[i : i + part_size]
            self.fake.uploads[uid][2][i // part_size + 1] = chunk
            landed.append({"n": i // part_size + 1, "etag": etag_of(chunk)})
        return landed


@pytest.fixture(params=["sync", "async"])
def client(fake: Fake, request: pytest.FixtureRequest) -> Iterator[Client]:
    yield Client(fake, request.param)


def end_of(begun: Dict[str, Any]) -> Dict[str, Any]:
    """What the browser sends at 'end' after a single PUT: the one part it ran."""
    return {"phase": "end", "completionToken": begun["completionToken"], "parts": [{"n": 1, "etag": '"e"'}]}


def before(path: str = "up/a.png", **kw: Any) -> Any:
    return lambda a: BeforeUploadResult(path=path, **kw)


# ------------------------------------------------------------ single PUT


async def test_single_put_round_trip(client: Client, fake: Fake) -> None:
    seen: Dict[str, Any] = {}

    def complete(a: UploadCompleteArgs) -> Any:
        seen["args"] = a
        return {"id": 7}

    h = client.handler(
        content_types=["image/*"],
        context=lambda req: {"user": "u1", "req": req},
        on_before_upload=lambda a: BeforeUploadResult(
            path=f"{a.ctx['user']}/{a.file.name}", metadata={"owner": a.ctx["user"]}, state={"row": 3}
        ),
        on_upload_complete=complete,
    )
    status, begun = await client.begin(h, head=base64.b64encode(PNG[:32]).decode())
    assert status == 200, begun
    assert begun["path"] == "u1/a.png"
    plan = begun["upload"]
    assert plan["multipart"] is False and plan["partSize"] == len(PNG)
    [part] = plan["parts"]
    assert part["n"] == 1
    assert part["headers"]["content-type"] == "image/png"
    assert part["headers"]["x-amz-meta-owner"] == "u1"
    marker = part["headers"]["x-amz-meta-upstash-upload"]
    # The marker, metadata and exact length are signed into the url, not merely sent with it.
    signed = fake.presigns[-1]
    assert signed["method"] == "PUT" and signed["key"] == "u1/a.png"
    assert signed["headers"]["x-amz-meta-upstash-upload"] == marker
    assert signed["headers"]["content-length"] == str(len(PNG))
    assert "partNumber" not in signed

    client.put(begun, PNG)
    status, done = await client.post(h, end_of(begun))
    assert status == 200, done
    assert done["data"] == {"id": 7}
    blob = done["blob"]
    assert blob["path"] == "u1/a.png" and blob["size"] == len(PNG) and blob["contentType"] == "image/png"
    assert blob["url"] == "https://b0123456789a.blob.upstash.io/u1/a.png"
    assert blob["versionedUrl"].startswith(blob["url"] + "?v=")
    assert blob["uploadedAt"].endswith("Z")
    args = seen["args"]
    assert args.ctx["user"] == "u1" and args.ctx["req"] == "req" and args.request == "req"
    assert args.route == "" and args.state == {"row": 3} and args.upload_id == marker
    assert args.multipart_upload_id is None
    assert args.metadata == {"owner": "u1"}  # the marker is the SDK's, not the app's
    assert args.file.name == "a.png" and args.url == blob["url"]


async def test_end_refuses_an_object_this_upload_did_not_write(client: Client, fake: Fake) -> None:
    h = client.handler(on_before_upload=before())
    _, begun = await client.begin(h)
    # Something else is at the path: no marker, so 'end' must not record it.
    fake.objects["up/a.png"] = Obj(PNG, {"content-type": "image/png"}, etag_of(PNG), time.time())
    status, body = await client.post(h, end_of(begun))
    assert (status, body["code"]) == (404, "not_found")
    assert "up/a.png" in fake.objects


async def test_end_before_the_put_is_not_found(client: Client) -> None:
    h = client.handler(on_before_upload=before())
    _, begun = await client.begin(h)
    status, body = await client.post(h, end_of(begun))
    assert (status, body["code"]) == (404, "not_found")


async def test_size_mismatch_deletes_the_object(client: Client, fake: Fake) -> None:
    h = client.handler(on_before_upload=before())
    _, begun = await client.begin(h)
    client.put(begun, PNG + b"extra")
    status, body = await client.post(h, end_of(begun))
    assert (status, body["code"]) == (403, "signature_mismatch")
    assert "up/a.png" not in fake.objects


async def test_refusal_in_on_upload_complete_deletes_and_answers(client: Client, fake: Fake) -> None:
    def refuse(a: UploadCompleteArgs) -> None:
        raise BlobError("forbidden", "quota exceeded")

    h = client.handler(on_before_upload=before(), on_upload_complete=refuse)
    _, begun = await client.begin(h)
    client.put(begun, PNG)
    status, body = await client.post(h, end_of(begun))
    assert (status, body["code"], body["message"]) == (403, "forbidden", "Quota exceeded")
    assert "up/a.png" not in fake.objects


async def test_refusal_leaves_a_replaced_object_alone(client: Client, fake: Fake) -> None:
    def replace_then_refuse(a: UploadCompleteArgs) -> None:
        fake.objects["up/a.png"] = Obj(b"newer", {}, etag_of(b"newer"), time.time())
        raise BlobError("forbidden")

    h = client.handler(on_before_upload=before(), on_upload_complete=replace_then_refuse)
    _, begun = await client.begin(h)
    client.put(begun, PNG)
    status, _ = await client.post(h, end_of(begun))
    assert status == 403
    assert fake.objects["up/a.png"].data == b"newer"


async def test_an_app_bug_in_on_upload_complete_is_raised(client: Client, fake: Fake) -> None:
    def broken(a: UploadCompleteArgs) -> None:
        raise KeyError("oops")

    h = client.handler(on_before_upload=before(), on_upload_complete=broken)
    _, begun = await client.begin(h)
    client.put(begun, PNG)
    with pytest.raises(KeyError):
        await client.call(h, body=end_of(begun))
    assert "up/a.png" not in fake.objects


async def test_retried_end_is_idempotent(client: Client) -> None:
    h = client.handler(on_before_upload=before(), on_upload_complete=lambda a: a.upload_id)
    _, begun = await client.begin(h)
    client.put(begun, PNG)
    end = end_of(begun)
    s1, first = await client.post(h, end)
    s2, second = await client.post(h, end)
    assert s1 == s2 == 200 and first["data"] == second["data"]


async def test_cancel_deletes_only_this_uploads_object(client: Client, fake: Fake) -> None:
    h = client.handler(on_before_upload=before())
    _, begun = await client.begin(h)
    fake.objects["up/a.png"] = Obj(PNG, {}, etag_of(PNG), time.time())
    assert (await client.post(h, {"phase": "cancel", "completionToken": begun["completionToken"]}))[0] == 200
    assert "up/a.png" in fake.objects
    client.put(begun, PNG)
    assert (await client.post(h, {"phase": "cancel", "completionToken": begun["completionToken"]}))[0] == 200
    assert "up/a.png" not in fake.objects


async def test_private_bucket_answers_without_urls(client: Client, fake: Fake) -> None:
    fake.visibility = "private"
    h = client.handler(on_before_upload=before())
    _, begun = await client.begin(h)
    assert begun["upload"]["parts"][0]["headers"]["cache-control"].startswith("private")
    client.put(begun, PNG)
    _, done = await client.post(h, end_of(begun))
    assert "url" not in done["blob"] and "versionedUrl" not in done["blob"]


# ------------------------------------------------------------- multipart


async def test_multipart_round_trip_with_resume(client: Client, fake: Fake) -> None:
    size = 12 * MIB
    data = bytes(range(256)) * (size // 256)
    h = client.handler(on_before_upload=before("big/file.bin"), multipart=True)
    status, begun = await client.begin(h, name="file.bin", size=size, type="application/octet-stream")
    assert status == 200, begun
    plan = begun["upload"]
    assert plan["multipart"] is True and plan["partSize"] == 5 * MIB
    assert [p["n"] for p in plan["parts"]] == [1, 2, 3]
    assert all("headers" not in p for p in plan["parts"])
    parts = sorted((p for p in fake.presigns if "partNumber" in p), key=lambda p: int(p["partNumber"]))
    assert [(p["partNumber"], p["headers"]["content-length"]) for p in parts] == [
        (1, str(5 * MIB)),
        (2, str(5 * MIB)),
        (3, str(2 * MIB)),
    ]
    assert all(p["uploadId"] == "up&1" for p in parts)

    landed = client.put_parts("big/file.bin", data, 5 * MIB)
    # A resumed upload asks what landed.
    status, resumed = await client.post(h, {"phase": "parts", "completionToken": begun["completionToken"], "from": 2})
    assert status == 200 and resumed["multipart"] is True and resumed["size"] == size
    assert [p["n"] for p in resumed["parts"]] == [2, 3]
    assert resumed["landed"] == landed

    status, done = await client.post(h, {"phase": "end", "completionToken": begun["completionToken"], "parts": landed})
    assert status == 200, done
    assert fake.objects["big/file.bin"].data == data
    assert done["blob"]["size"] == size
    # A retried 'end' finds the upload already completed.
    status, again = await client.post(h, {"phase": "end", "completionToken": begun["completionToken"], "parts": landed})
    assert status == 200 and again["blob"]["etag"] == done["blob"]["etag"]


async def test_multipart_end_lists_parts_when_the_browser_sends_none(client: Client, fake: Fake) -> None:
    size = 6 * MIB
    h = client.handler(on_before_upload=before("big/b"), multipart=True)
    _, begun = await client.begin(h, name="b", size=size, type="")
    client.put_parts("big/b", b"x" * size, 5 * MIB)
    status, done = await client.post(h, end_of(begun))
    assert status == 200, done
    assert done["blob"]["contentType"] == "application/octet-stream"


async def test_multipart_cancel_aborts(client: Client, fake: Fake) -> None:
    h = client.handler(on_before_upload=before("big/c"), multipart=True)
    _, begun = await client.begin(h, name="c", size=6 * MIB, type="")
    assert fake.uploads
    assert (await client.post(h, {"phase": "cancel", "completionToken": begun["completionToken"]}))[0] == 200
    assert not fake.uploads


async def test_multipart_is_aborted_when_signing_fails(client: Client, fake: Fake) -> None:
    fake.presign_response = lambda body: httpx.Response(403, json={"error": "suspended"})
    h = client.handler(on_before_upload=before("big/d"), multipart=True)
    status, body = await client.begin(h, name="d", size=6 * MIB, type="")
    assert (status, body["code"]) == (403, "forbidden")
    assert "suspended" not in json.dumps(body)
    assert not fake.uploads


async def test_parts_batches_sixteen_urls(client: Client) -> None:
    size = 40 * 5 * MIB
    h = client.handler(on_before_upload=before("big/e"), multipart=True)
    _, begun = await client.begin(h, name="e", size=size, type="")
    assert [p["n"] for p in begun["upload"]["parts"]] == list(range(1, 17))
    _, more = await client.post(h, {"phase": "parts", "completionToken": begun["completionToken"], "from": 33})
    assert [p["n"] for p in more["parts"]] == list(range(33, 41))
    _, past = await client.post(h, {"phase": "parts", "completionToken": begun["completionToken"], "from": 99})
    assert past["parts"] == []


# ----------------------------------------------------------- constraints


async def test_begin_enforces_constraints(client: Client, fake: Fake) -> None:
    h = client.handler(content_types=["image/png"], max_size="1kb", on_before_upload=before())
    status, body = await client.begin(h, size=2000)
    assert (status, body["code"]) == (413, "too_large")
    status, body = await client.begin(h, type="text/html")
    assert (status, body["code"]) == (400, "content_type_not_allowed")
    # A PDF declared as a PNG is refused on its bytes.
    status, body = await client.begin(h, head=base64.b64encode(b"%PDF-1.7\n" + b"0" * 40).decode())
    assert (status, body["code"]) == (400, "content_type_not_allowed")
    status, body = await client.begin(h, size=0)
    assert (status, body["code"]) == (400, "empty_body")
    assert not fake.presigns


async def test_every_successful_answer_carries_the_protocol(client: Client) -> None:
    h = client.handler(on_before_upload=before("big/p"), multipart=True)
    answers = []
    _, begun = await client.begin(h, name="p", size=6 * MIB, type="", protocol=1)
    answers.append(begun)
    token = begun["completionToken"]
    answers.append((await client.post(h, {"phase": "parts", "completionToken": token, "protocol": 7}))[1])
    client.put_parts("big/p", b"x" * 6 * MIB, 5 * MIB)
    answers.append((await client.post(h, {"phase": "end", "completionToken": token}))[1])
    _, second = await client.begin(h, name="p", size=6 * MIB, type="")
    answers.append((await client.post(h, {"phase": "cancel", "completionToken": second["completionToken"]}))[1])
    assert [a["protocol"] for a in answers] == [1, 1, 1, 1]
    status, refused = await client.post(h, {"phase": "end", "completionToken": "x.y"})
    assert status == 403 and "protocol" not in refused


async def test_a_traversing_path_is_the_clients_bad_input(client: Client, fake: Fake) -> None:
    h = client.handler(on_before_upload=lambda a: BeforeUploadResult(path=f"u1/{a.file.name}"))
    for name in ("../x.png", "./x.png", ".."):
        status, body = await client.begin(h, name=name)
        assert (status, body["code"]) == (400, "invalid_input"), body
    assert not fake.presigns


async def test_an_impossible_size_is_refused_before_anything_is_created(client: Client, fake: Fake) -> None:
    for kw in ({}, {"max_size": "1MB"}):
        h = client.handler(on_before_upload=before(), **kw)
        status, body = await client.post(h, {"phase": "begin", "file": {"name": "a", "type": "", "size": 10**400}})
        assert (status, body["code"]) == (400, "invalid_input")
    assert not fake.uploads and not fake.presigns


async def test_a_token_that_is_not_text_is_forbidden(client: Client) -> None:
    h = client.handler(on_before_upload=before())
    for token in ("abc\ud800.def", "abc.d\ud800ef"):
        for phase in ("parts", "end", "cancel"):
            status, body = await client.post(h, {"phase": phase, "completionToken": token})
            assert (status, body["code"]) == (403, "forbidden")


async def test_binary_limits_read_as_exact_bytes(client: Client) -> None:
    h = client.handler(max_size="32MiB", on_before_upload=before())
    status, body = await client.begin(h, size=32 * MIB + 1)
    assert (status, body["code"]) == (413, "too_large")
    assert body["message"] == "a.png is 33,554,433 bytes, over the 33,554,432 byte limit"
    status, _ = await client.begin(h, size=32 * MIB)
    assert status == 200


async def test_before_upload_may_narrow_but_not_widen(client: Client) -> None:
    h = client.handler(max_size="1kb", on_before_upload=before(max_size=10))
    status, body = await client.begin(h)
    assert (status, body["code"]) == (413, "too_large")
    wide = client.handler(max_size="1kb", on_before_upload=before(max_size="1mb"))
    with pytest.raises(TypeError, match="widened max_size"):
        await client.begin(wide)
    wide_types = client.handler(content_types=["image/png"], on_before_upload=before(content_types=["image/*"]))
    with pytest.raises(TypeError, match="widened content_types"):
        await client.begin(wide_types)


async def test_get_serves_limits_with_an_etag(client: Client) -> None:
    h = client.handler(content_types=["image/png"], max_size="2mb", on_before_upload=before())
    res = await client.call(h, "GET")
    assert res.status == 200
    assert json.loads(res.body) == {"protocol": 1, "constraints": {"contentTypes": ["image/png"], "maxSize": 2_000_000}}
    assert res.headers["cache-control"] == "public, max-age=60"
    again = await client.call(h, "GET", headers={"if-none-match": res.headers["etag"]})
    assert again.status == 304 and again.body == b""


async def test_route_constraints_replace_and_clear_the_handlers(client: Client) -> None:
    h = client.handler(
        content_types=["image/png"],
        max_size="1kb",
        on_before_upload=before(),
        routes={"inherit": client.route(), "doc": client.route(content_types=["application/pdf"], max_size=None)},
    )
    res = await client.call(h, "GET", route="inherit")
    assert json.loads(res.body)["constraints"] == {"contentTypes": ["image/png"], "maxSize": 1000}
    res = await client.call(h, "GET", route="doc")
    assert json.loads(res.body)["constraints"] == {"contentTypes": ["application/pdf"]}


# ---------------------------------------------------------------- tokens


async def test_tokens_are_bound_to_their_route(client: Client) -> None:
    h = client.handler(
        on_before_upload=before(), routes={"a": client.route(), "b": client.route(content_types=["image/png"])}
    )
    _, begun = await client.begin(h, route="a")
    token = begun["completionToken"]
    status, body = await client.post(h, {"phase": "end", "completionToken": token}, route="b")
    assert (status, body["code"]) == (403, "forbidden")


async def test_tampered_and_expired_tokens_are_refused(client: Client) -> None:
    h = client.handler(on_before_upload=before())
    _, begun = await client.begin(h)
    token = begun["completionToken"]
    body, sig = token.split(".")
    payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    payload["path"] = "someone/else.png"
    forged = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=") + "." + sig
    status, res = await client.post(h, {"phase": "end", "completionToken": forged})
    assert (status, res["code"]) == (403, "forbidden")

    grant = verify_grant(token, "pw-secret")
    assert grant is not None
    expired = sign_grant(
        type(grant)(**{**grant.__dict__, "exp": int(time.time() * 1000) - 1}),
        "pw-secret",
    )
    status, res = await client.post(h, {"phase": "end", "completionToken": expired})
    assert (status, res["code"], res["message"]) == (403, "forbidden", "CompletionToken has expired")
    status, res = await client.post(h, {"phase": "end"})
    assert (status, res["code"]) == (400, "invalid_input")


def test_state_must_be_json() -> None:
    grant = verify_grant(sign_grant_for({"ok": 1}), "k")
    assert grant is not None and grant.state == {"ok": 1}
    with pytest.raises(TypeError, match="JSON-serializable"):
        sign_grant_for(object())


def sign_grant_for(state: Any) -> str:
    from upstash_blob._upload import Grant

    return sign_grant(Grant("b", "r", "id", "p", "n", "t", 1, {}, state, 0, None, 1), "k")


# ---------------------------------------------------------------- routes


async def test_named_routes_dispatch_on_the_query(client: Client) -> None:
    calls: List[str] = []
    h = client.handler(
        on_before_upload=lambda a: calls.append(a.route) or BeforeUploadResult(path=f"{a.route}/x"),
        routes={
            "avatar": client.route(),
            "doc": client.route(on_before_upload=lambda a: BeforeUploadResult(path="docs/x")),
        },
    )
    assert (await client.begin(h, route="avatar"))[1]["path"] == "avatar/x"
    assert (await client.begin(h, route="doc"))[1]["path"] == "docs/x"
    assert calls == ["avatar"]
    for name in (None, "nope", "__class__"):
        status, body = await client.begin(h, route=name)
        assert (status, body["code"]) == (404, "not_found")


async def test_a_sole_route_refuses_a_route_name(client: Client) -> None:
    h = client.handler(on_before_upload=before())
    status, body = await client.begin(h, route="avatar")
    assert (status, body["code"]) == (404, "not_found")


def test_bad_route_config_fails_at_construction(fake: Fake) -> None:
    c = Client(fake, "sync")
    with pytest.raises(TypeError, match="route names"):
        c.handler(on_before_upload=before(), routes={"a b": UploadRoute()})
    with pytest.raises(TypeError, match="no on_before_upload"):
        c.handler(routes={"a": UploadRoute()})
    with pytest.raises(TypeError, match="routes is empty"):
        c.handler(on_before_upload=before(), routes={})
    with pytest.raises(TypeError, match="must be a Bucket"):
        UploadHandler(bucket=Client(fake, "async").bucket, on_before_upload=before())
    with pytest.raises(ValueError, match="multipart"):
        c.handler(on_before_upload=before(), multipart="lots")
    with pytest.raises(TypeError, match="expected an UploadRoute"):
        c.handler(on_before_upload=before(), routes={"a": {}})


def test_bucket_falls_back_to_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UPSTASH_BLOB_TOKEN", raising=False)
    with pytest.raises(TypeError, match="UPSTASH_BLOB_TOKEN is not set"):
        UploadHandler(on_before_upload=before())
    monkeypatch.setenv("UPSTASH_BLOB_TOKEN", TOKEN)
    h = AsyncUploadHandler(on_before_upload=before(), routes={"a": UploadRoute(), "b": UploadRoute()})
    assert h._table["a"].bucket is h._table["b"].bucket


# ----------------------------------------------------------------- errors


async def test_context_refusals(client: Client) -> None:
    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str) -> None:
            self.status_code, self.detail = status_code, detail

    def deny(req: Any) -> None:
        raise HTTPException(401, "sign in first")

    h = client.handler(context=deny, on_before_upload=before())
    status, body = await client.begin(h)
    assert (status, body["code"], body["message"]) == (401, "unauthorized", "Sign in first")

    def boom(req: Any) -> None:
        raise RuntimeError("bug")

    with pytest.raises(RuntimeError):
        await client.begin(client.handler(context=boom, on_before_upload=before()))


class NativeResponse:
    pass


async def test_on_error_sees_refusals_and_may_replace_them(client: Client) -> None:
    seen: List[UploadErrorArgs] = []

    def on_error(a: UploadErrorArgs) -> Any:
        seen.append(a)
        if a.path == "swap":
            return BlobError("conflict", "taken")
        if a.path == "native":
            return NativeResponse()
        if a.path == "event-id":
            return "3f2a9c"  # what sentry_sdk.capture_exception returns: not an answer
        return None

    h = client.handler(
        content_types=["image/png"],
        on_before_upload=lambda a: BeforeUploadResult(path=a.input),
        on_error=on_error,
        input=str,
    )
    status, _ = await client.begin(h, type="text/plain", input="x")
    assert status == 400 and seen[-1].file is not None and seen[-1].path is None

    refuse_after = client.handler(
        on_before_upload=lambda a: BeforeUploadResult(path=a.input, metadata={"upstash-upload": "x"}),
        on_error=on_error,
        input=str,
    )
    status, body = await client.begin(refuse_after, input="swap")
    assert (status, body["code"]) == (409, "conflict")
    assert seen[-1].path == "swap"
    assert isinstance(
        await client.call(
            refuse_after,
            body={"phase": "begin", "file": {"name": "a", "type": "", "size": 1}, "input": "native"},
            native=NativeResponse,
        ),
        NativeResponse,
    )
    status, body = await client.begin(refuse_after, input="native")  # a handle() that named no native type
    assert (status, body["code"]) == (400, "invalid_input")
    status, body = await client.begin(refuse_after, input="event-id")
    assert (status, body["code"]) == (400, "invalid_input")
    status, body = await client.begin(client.handler(on_before_upload=before(), on_error=on_error), route="x")
    assert status == 404 and seen[-1].route == "x" and seen[-1].ctx is None


async def test_input_is_validated(client: Client) -> None:
    def thread_id(raw: Any) -> str:
        if not isinstance(raw, dict) or not isinstance(raw.get("thread"), str):
            raise ValueError("thread is required")
        return str(raw["thread"])

    h = client.handler(input=thread_id, on_before_upload=lambda a: BeforeUploadResult(path=f"t/{a.input}"))
    assert (await client.begin(h, input={"thread": "9"}))[1]["path"] == "t/9"
    status, body = await client.begin(h, input={})
    assert (status, body["code"], body["message"]) == (400, "invalid_input", "Thread is required")
    status, body = await client.begin(client.handler(on_before_upload=before()), input={"thread": "9"})
    assert (status, body["message"]) == (400, "This route takes no input")


async def test_pydantic_input(client: Client) -> None:
    pydantic = pytest.importorskip("pydantic")

    class Thread(pydantic.BaseModel):  # type: ignore[misc]
        thread: int

    h = client.handler(input=Thread, on_before_upload=lambda a: BeforeUploadResult(path=f"t/{a.input.thread}"))
    assert (await client.begin(h, input={"thread": 9}))[1]["path"] == "t/9"
    status, body = await client.begin(h, input={"thread": "x"})
    assert status == 400 and body["message"].lower().startswith("thread: input should be a valid integer")


async def test_request_shape_is_checked(client: Client) -> None:
    h = client.handler(on_before_upload=before())
    status, body = await client.post(h, b"not json")
    assert (status, body["code"]) == (400, "invalid_input")
    status, body = await client.post(h, {"phase": "nope"})
    assert (status, body["message"]) == (400, "Unknown phase nope")
    status, body = await client.post(h, {"phase": "begin", "file": {"name": "a", "size": True}})
    assert (status, body["code"]) == (400, "invalid_input")
    status, body = await client.post(h, {"phase": "begin"}, headers={"content-type": "text/plain"})
    assert status == 415
    status, body = await client.post(h, b"{" + b" " * 1_000_001 + b"}")
    assert (status, body["code"]) == (413, "too_large")
    status, body = await client.begin(h, type="text/html\r\nx-evil: 1")
    assert (status, body["code"]) == (400, "invalid_input")
    res = await client.call(h, "DELETE")
    assert res.status == 405


async def test_sync_handler_refuses_async_callbacks(fake: Fake) -> None:
    async def later(a: BeforeUploadArgs) -> BeforeUploadResult:
        return BeforeUploadResult(path="x")

    c = Client(fake, "sync")
    with pytest.raises(TypeError, match="AsyncUploadHandler"):
        await c.begin(c.handler(on_before_upload=later))


async def test_async_handler_awaits_async_callbacks(fake: Fake) -> None:
    async def ctx(req: Any) -> str:
        return "u"

    async def decide(a: BeforeUploadArgs) -> BeforeUploadResult:
        return BeforeUploadResult(path=f"{a.ctx}/x")

    c = Client(fake, "async")
    assert (await c.begin(c.handler(context=ctx, on_before_upload=decide)))[1]["path"] == "u/x"


async def test_context_never_runs_for_a_non_json_body(client: Client) -> None:
    ran: List[Any] = []
    h = client.handler(context=lambda req: ran.append(req), on_before_upload=before())
    status, _ = await client.post(h, {"phase": "begin"}, headers={"content-type": "text/plain"})
    assert status == 415 and ran == []


async def test_deeply_nested_json_is_a_refusal(client: Client) -> None:
    h = client.handler(on_before_upload=before())
    status, body = await client.post(h, b"[" * 200_000 + b"]" * 200_000)
    assert (status, body["code"]) == (400, "invalid_input")


# -------------------------------------------------------------------- owner


def owned(client: Client, **kw: Any) -> Handler:
    return client.handler(
        context=lambda req: req, owner=lambda req: req["user"], on_before_upload=before(), multipart="1mb", **kw
    )


async def test_owner_binds_every_later_request_to_who_began(client: Client, fake: Fake) -> None:
    h = owned(client)
    u1, u2 = {"user": "u1"}, {"user": "u2"}
    status, begun = await client.begin(h, size=3 * MIB, type="application/octet-stream", request=u1)
    assert status == 200, begun
    token = begun["completionToken"]
    assert verify_grant(token, "pw-secret").owner == "u1"  # type: ignore[union-attr]
    for phase in ("parts", "end", "cancel"):
        status, body = await client.post(h, {"phase": phase, "completionToken": token}, request=u2)
        assert (status, body["code"], body["message"]) == (403, "forbidden", "This upload belongs to another user")
        assert "path" not in body
    assert fake.uploads  # nothing was cancelled or completed by u2
    status, body = await client.post(h, {"phase": "parts", "completionToken": token, "from": 1}, request=u1)
    assert status == 200, body
    status, _ = await client.post(h, {"phase": "cancel", "completionToken": token}, request=u1)
    assert status == 200 and not fake.uploads


async def test_owner_takes_ints_and_refuses_nobody_before_anything_runs(client: Client, fake: Fake) -> None:
    ran: List[Any] = []
    h = client.handler(
        context=lambda req: req,
        owner=lambda req: req.get("id"),
        on_before_upload=lambda a: ran.append(a) or BeforeUploadResult(path="up/a.png"),
    )
    status, begun = await client.begin(h, request={"id": 7})
    assert status == 200 and verify_grant(begun["completionToken"], "pw-secret").owner == "7"  # type: ignore[union-attr]
    import uuid

    uid = uuid.UUID(int=1)
    status, begun = await client.begin(h, request={"id": uid})
    assert status == 200 and verify_grant(begun["completionToken"], "pw-secret").owner == str(uid)  # type: ignore[union-attr]
    ran.clear()
    presigns = len(fake.presigns)
    for nobody in ({}, {"id": ""}, {"id": None}):
        status, body = await client.begin(h, request=nobody)
        assert (status, body["code"]) == (401, "unauthorized")
    assert not ran and len(fake.presigns) == presigns


async def test_a_token_minted_without_an_owner_is_refused_by_a_handler_with_one(client: Client) -> None:
    plain = client.handler(on_before_upload=before())
    _, begun = await client.begin(plain)
    h = client.handler(context=lambda req: req, owner=lambda req: "u1", on_before_upload=before())
    status, body = await client.post(h, end_of(begun), request={})
    assert (status, body["code"]) == (403, "forbidden")


def test_owner_needs_a_context(fake: Fake) -> None:
    bucket = Bucket(TOKEN, http_client=httpx.Client(transport=httpx.MockTransport(fake.handler)))
    with pytest.raises(TypeError, match="needs a context"):
        UploadHandler(bucket=bucket, owner=lambda u: u, on_before_upload=before())


async def test_async_handler_awaits_an_async_owner(fake: Fake) -> None:
    async def who(ctx: Any) -> str:
        return "u1"

    bucket = AsyncBucket(TOKEN, http_client=httpx.AsyncClient(transport=httpx.MockTransport(fake.ahandler)))
    h = AsyncUploadHandler(bucket=bucket, context=lambda r: r, owner=who, on_before_upload=before())
    client = Client(fake, "async")
    status, begun = await client.begin(h)
    assert status == 200 and verify_grant(begun["completionToken"], "pw-secret").owner == "u1"  # type: ignore[union-attr]


# --------------------------------------------------------------- CORS probe


async def settle_probes(h: Handler) -> None:
    import asyncio
    import threading

    for t in threading.enumerate():
        if t.name == "upstash-blob-cors-probe":
            t.join(5)
    probes = getattr(h, "_probes", None)
    if probes:
        await asyncio.wait(list(probes), timeout=5)


async def test_a_refused_preflight_is_logged_once_per_origin(
    client: Client, fake: Fake, caplog: pytest.LogCaptureFixture
) -> None:
    seen: List[httpx.Request] = []

    def preflight(r: httpx.Request) -> Optional[httpx.Response]:
        if r.method != "OPTIONS":
            return None
        seen.append(r)
        if r.headers["origin"] == "https://ok.example":
            return httpx.Response(204, headers={"access-control-allow-origin": "https://ok.example"})
        return httpx.Response(403)

    fake.intercept = preflight
    h = client.handler(on_before_upload=before())
    for origin in ("https://app.example", "https://app.example", "https://ok.example", "null", "javascript:x"):
        status, _ = await client.begin(h, headers={"origin": origin})
        assert status == 200
        await settle_probes(h)
    assert [r.headers["origin"] for r in seen] == ["https://app.example", "https://ok.example"]
    first = seen[0]
    assert first.headers["access-control-request-method"] == "PUT"
    assert "x-amz-meta-upstash-upload" in first.headers["access-control-request-headers"].split(",")
    warnings = [r.getMessage() for r in caplog.records if "CORS preflight" in r.getMessage()]
    assert len(warnings) == 1 and "https://app.example" in warnings[0]


async def test_a_probe_cancelled_by_the_loop_is_asked_again(fake: Fake) -> None:
    import asyncio

    started = asyncio.Event()
    seen: List[httpx.Request] = []

    async def handler(r: httpx.Request) -> httpx.Response:
        if r.method == "OPTIONS":
            seen.append(r)
            started.set()
            await asyncio.Event().wait()  # the loop shuts down first
        return await fake.ahandler(r)

    bucket = AsyncBucket(TOKEN, http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    h = AsyncUploadHandler(bucket=bucket, on_before_upload=before())
    client = Client(fake, "async")
    await client.begin(h, headers={"origin": "https://app.example"})
    await asyncio.wait_for(started.wait(), 5)
    for t in list(h._probes):
        t.cancel()
    await asyncio.gather(*h._probes, return_exceptions=True)
    started.clear()
    await client.begin(h, headers={"origin": "https://app.example"})
    await asyncio.wait_for(started.wait(), 5)
    assert len(seen) == 2
    for t in list(h._probes):
        t.cancel()
    await asyncio.gather(*h._probes, return_exceptions=True)


async def test_a_preflight_storage_did_not_answer_is_asked_again(client: Client, fake: Fake) -> None:
    seen: List[httpx.Request] = []

    def flaky(r: httpx.Request) -> Optional[httpx.Response]:
        if r.method != "OPTIONS":
            return None
        seen.append(r)
        return httpx.Response(503)

    fake.intercept = flaky
    h = client.handler(on_before_upload=before())
    for _ in range(2):
        await client.begin(h, headers={"origin": "https://app.example"})
        await settle_probes(h)
    assert len(seen) == 2
