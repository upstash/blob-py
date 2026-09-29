"""
Mounts an ``AsyncUploadHandler`` on FastAPI or Starlette.

>>> from upstash_blob.fastapi import upload_view
>>> app.add_api_route("/api/upload", upload_view(uploads), methods=["GET", "POST"])
"""

from typing import Any, Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response

from upstash_blob._upload import MAX_BODY, UploadResponse
from upstash_blob._upload_async import AsyncUploadHandler


async def _read(request: Request) -> bytes:
    # Stops one byte past the cap, which the handler refuses: nothing longer is ever buffered.
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > MAX_BODY:
            break
    return bytes(body)


def upload_view(uploads: AsyncUploadHandler) -> Callable[[Request], Awaitable[Any]]:
    """
    An endpoint for GET and POST. ``request`` in every callback is the Starlette ``Request``.
    What ``on_error`` returns in place of a ``BlobError`` is answered as it stands.
    """
    if not isinstance(uploads, AsyncUploadHandler):
        raise TypeError("upstash_blob.fastapi mounts an AsyncUploadHandler")

    async def upload(request: Request) -> Any:
        res = await uploads.handle(
            request.method,
            request=request,
            route=request.query_params.get("route"),
            body=await _read(request) if request.method == "POST" else b"",
            headers=request.headers,
        )
        if isinstance(res, UploadResponse):
            return Response(res.body, res.status, headers=res.headers)
        return res

    return upload
