"""
Mounts an ``UploadHandler`` on Flask.

>>> from upstash_blob.flask import upload_view
>>> app.add_url_rule("/api/upload", view_func=upload_view(uploads), methods=["GET", "POST"])
"""

from typing import Any, Callable

from flask import Response, request
from werkzeug.wrappers import Response as WerkzeugResponse

from upstash_blob._upload import MAX_BODY, UploadResponse
from upstash_blob._upload_sync import UploadHandler


def _body(req: Any) -> bytes:
    # Cached, so a before_request hook that read the body first leaves it readable here.
    if req.content_length is not None and req.content_length <= MAX_BODY:
        return bytes(req.get_data(cache=True))
    # Over the cap, or of unknown length: one byte past the cap is all the handler needs to refuse it.
    body = req.stream.read(MAX_BODY + 1)
    if not body and req.content_length:
        return bytes(MAX_BODY + 1)
    return bytes(body)


def upload_view(uploads: UploadHandler, name: str = "upstash_blob_upload") -> Callable[[], Any]:
    """
    A view for GET and POST. ``request`` in every callback is Flask's request. Two handlers on one
    app need two ``name``s, since Flask names an endpoint after its view.
    """
    if not isinstance(uploads, UploadHandler):
        raise TypeError("upstash_blob.flask mounts an UploadHandler")

    def view() -> Any:
        req = request._get_current_object()  # type: ignore[attr-defined]
        res = uploads.handle(
            req.method,
            request=req,
            route=req.args.get("route"),
            body=_body(req) if req.method == "POST" else b"",
            headers=req.headers,
            native=WerkzeugResponse,
        )
        if isinstance(res, UploadResponse):
            return Response(res.body, status=res.status, headers=res.headers)
        return res

    view.__name__ = name
    return view
