"""
Mounts an ``UploadHandler`` on Flask.

>>> from upstash_blob.flask import upload_view
>>> app.add_url_rule("/api/upload", view_func=upload_view(uploads), methods=["GET", "POST"])
"""

from typing import Any, Callable

from flask import Response, request

from upstash_blob._upload import MAX_BODY, UploadResponse
from upstash_blob._upload_sync import UploadHandler


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
            # One byte past the cap, which the handler refuses: nothing longer is ever buffered.
            body=req.stream.read(MAX_BODY + 1) if req.method == "POST" else b"",
            headers=req.headers,
        )
        if isinstance(res, UploadResponse):
            return Response(res.body, status=res.status, headers=res.headers)
        return res

    view.__name__ = name
    return view
