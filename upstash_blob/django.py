"""
Mounts an upload handler on Django: ``UploadHandler`` as a sync view, ``AsyncUploadHandler`` as an
async one (under ASGI).

>>> from upstash_blob.django import upload_view
>>> urlpatterns = [path("api/upload", upload_view(uploads))]
"""

from typing import Any, Callable, Union

from django.http import HttpRequest, HttpResponse

from upstash_blob._upload import UploadResponse
from upstash_blob._upload_async import AsyncUploadHandler
from upstash_blob._upload_sync import UploadHandler


def _response(res: Any) -> Any:
    if isinstance(res, UploadResponse):
        return HttpResponse(res.body, status=res.status, headers=res.headers)
    return res


def _body(request: HttpRequest) -> bytes:
    # Django caps it at DATA_UPLOAD_MAX_MEMORY_SIZE; the handler's own cap is tighter.
    return request.body if request.method == "POST" else b""


def upload_view(uploads: Union[UploadHandler, AsyncUploadHandler]) -> Callable[..., Any]:
    """
    A view for GET and POST. ``request`` in every callback is the ``HttpRequest``.

    CSRF-exempt: the handler only reads ``application/json`` bodies, which a cross-site page cannot
    send without a CORS preflight, and the browser client sends no CSRF token. Authenticate in
    ``context``.
    """
    view: Any
    if isinstance(uploads, AsyncUploadHandler):
        handler = uploads

        async def async_view(request: HttpRequest) -> Any:
            return _response(
                await handler.handle(
                    request.method or "GET",
                    request=request,
                    route=request.GET.get("route"),
                    body=_body(request),
                    headers=request.headers,
                )
            )

        view = async_view
    elif isinstance(uploads, UploadHandler):
        sync_handler = uploads

        def sync_view(request: HttpRequest) -> Any:
            return _response(
                sync_handler.handle(
                    request.method or "GET",
                    request=request,
                    route=request.GET.get("route"),
                    body=_body(request),
                    headers=request.headers,
                )
            )

        view = sync_view
    else:
        raise TypeError("upstash_blob.django mounts an UploadHandler or an AsyncUploadHandler")
    view.csrf_exempt = True
    return view  # type: ignore[no-any-return]
