import json
import time
from typing import Any, List

import httpx
import pytest

from tests.fake import TOKEN, Fake, Obj, etag_of
from upstash_blob import (
    AsyncBucket,
    AsyncUploadHandler,
    BeforeUploadArgs,
    BeforeUploadResult,
    Bucket,
    UploadErrorArgs,
    UploadHandler,
)

BEGIN = {"phase": "begin", "file": {"name": "a.txt", "type": "text/plain", "size": 5}}


def sync_bucket(fake: Fake) -> Bucket:
    return Bucket(TOKEN, http_client=httpx.Client(transport=httpx.MockTransport(fake.handler)))


def async_bucket(fake: Fake) -> AsyncBucket:
    return AsyncBucket(TOKEN, http_client=httpx.AsyncClient(transport=httpx.MockTransport(fake.ahandler)))


def land(fake: Fake, begun: Any) -> None:
    part = begun["upload"]["parts"][0]
    fake.objects[begun["path"]] = Obj(b"hello", dict(part["headers"]), etag_of(b"hello"), time.time())


def end_of(begun: Any) -> Any:
    return {"phase": "end", "completionToken": begun["completionToken"], "parts": [{"n": 1, "etag": "e"}]}


def test_fastapi(fake: Fake) -> None:
    fastapi = pytest.importorskip("fastapi")
    from fastapi.responses import JSONResponse
    from fastapi.testclient import TestClient

    from upstash_blob.fastapi import upload_view

    seen: List[Any] = []

    def context(request: Any) -> str:
        if request.headers.get("authorization") != "Bearer ok":
            raise fastapi.HTTPException(401, "sign in")
        return "u1"

    def decide(a: BeforeUploadArgs) -> BeforeUploadResult:
        seen.append(a.request)
        return BeforeUploadResult(path=f"{a.ctx}/{a.file.name}")

    def on_error(a: UploadErrorArgs) -> Any:
        if a.route == "teapot":
            return JSONResponse({"custom": True}, status_code=418)
        return "event-id"

    uploads = AsyncUploadHandler(
        bucket=async_bucket(fake),
        context=context,
        on_before_upload=decide,
        on_upload_complete=lambda a: {"url": a.url},
        on_error=on_error,
        routes={"doc": _route(), "teapot": _route()},
    )
    app = fastapi.FastAPI()
    app.add_api_route("/api/upload", upload_view(uploads), methods=["GET", "POST"])
    client = TestClient(app)
    auth = {"authorization": "Bearer ok"}

    assert client.get("/api/upload?route=doc").json() == {"constraints": {}}
    res = client.post("/api/upload?route=doc", json=BEGIN)
    assert (res.status_code, res.json()["code"]) == (401, "unauthorized")
    res = client.post("/api/upload?route=doc", json=BEGIN, headers=auth)
    assert res.status_code == 200, res.text
    begun = res.json()
    assert begun["path"] == "u1/a.txt"
    assert seen[0].url.path == "/api/upload"
    land(fake, begun)
    res = client.post("/api/upload?route=doc", json=end_of(begun), headers=auth)
    assert res.status_code == 200 and res.json()["data"]["url"].endswith("/u1/a.txt")
    res = client.post("/api/upload?route=teapot", content=b"x", headers={**auth, "content-type": "text/plain"})
    assert (res.status_code, res.json()) == (418, {"custom": True})
    res = client.post(
        "/api/upload?route=doc",
        content=b"{" + b" " * 2_000_000 + b"}",
        headers={**auth, "content-type": "application/json"},
    )
    assert (res.status_code, res.json()["code"]) == (413, "too_large")

    with pytest.raises(TypeError, match="AsyncUploadHandler"):
        upload_view(UploadHandler(bucket=sync_bucket(fake), on_before_upload=decide))  # type: ignore[arg-type]


def test_flask(fake: Fake) -> None:
    flask = pytest.importorskip("flask")

    from upstash_blob.flask import upload_view

    uploads = UploadHandler(
        bucket=sync_bucket(fake),
        on_before_upload=lambda a: BeforeUploadResult(path=f"{a.request.args['route']}/{a.file.name}"),
        routes={"doc": _route()},
    )
    app = flask.Flask(__name__)

    # An extension that reads the body first must not leave the handler an empty one.
    @app.before_request
    def read_first() -> None:
        flask.request.get_json(silent=True)

    app.add_url_rule("/api/upload", view_func=upload_view(uploads), methods=["GET", "POST"])
    client = app.test_client()

    res = client.get("/api/upload?route=doc")
    etag = res.headers["etag"]
    assert client.get("/api/upload?route=doc", headers={"if-none-match": etag}).status_code == 304
    res = client.post("/api/upload?route=doc", json=BEGIN)
    assert res.status_code == 200, res.text
    begun = res.get_json()
    assert begun["path"] == "doc/a.txt"
    land(fake, begun)
    res = client.post("/api/upload?route=doc", json=end_of(begun))
    assert res.status_code == 200 and res.get_json()["blob"]["size"] == 5
    assert client.post("/api/upload?route=nope", json=BEGIN).status_code == 404
    big = b"{" + b" " * 2_000_000 + b"}"
    res = client.post("/api/upload?route=doc", data=big, content_type="application/json")
    assert res.status_code == 413


def test_django(fake: Fake) -> None:
    pytest.importorskip("django")
    import django
    from django.conf import settings

    if not settings.configured:
        settings.configure(
            SECRET_KEY="test",
            ALLOWED_HOSTS=["*"],
            ROOT_URLCONF=__name__,
            MIDDLEWARE=["django.middleware.csrf.CsrfViewMiddleware"],
        )
        django.setup()
    from django.test import Client
    from django.urls import path

    from upstash_blob.django import upload_view

    uploads = UploadHandler(
        bucket=sync_bucket(fake),
        on_before_upload=lambda a: BeforeUploadResult(path=f"dj/{a.file.name}"),
        on_upload_complete=lambda a: a.upload_id,
    )
    globals()["urlpatterns"] = [path("api/upload", upload_view(uploads))]
    client = Client(enforce_csrf_checks=True)

    res = client.post("/api/upload", BEGIN, content_type="application/json")
    assert res.status_code == 200, res.content
    begun = json.loads(res.content)
    land(fake, begun)
    res = client.post("/api/upload", end_of(begun), content_type="application/json")
    assert res.status_code == 200 and json.loads(res.content)["blob"]["path"] == "dj/a.txt"
    # A cross-site form post is not JSON, so it never reaches a callback despite the CSRF exemption.
    res = client.post("/api/upload", {"phase": "begin"})
    assert res.status_code == 415
    assert client.put("/api/upload").status_code == 405


async def test_django_async(fake: Fake) -> None:
    pytest.importorskip("django")
    from django.conf import settings

    if not settings.configured:
        pytest.skip("configured by test_django")
    from django.test import AsyncRequestFactory

    from upstash_blob.django import upload_view

    view = upload_view(
        AsyncUploadHandler(bucket=async_bucket(fake), on_before_upload=lambda a: BeforeUploadResult(path="dj/x"))
    )
    request = AsyncRequestFactory().post("/api/upload", BEGIN, content_type="application/json")
    res = await view(request)
    assert res.status_code == 200 and json.loads(res.content)["path"] == "dj/x"


def _route() -> Any:
    from upstash_blob import UploadRoute

    return UploadRoute()
