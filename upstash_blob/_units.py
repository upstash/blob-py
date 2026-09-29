import math
import re
from datetime import timedelta
from typing import Dict, Literal, Optional, Union

from upstash_blob.errors import BlobError

# Sizes are decimal ('2mb' = 2,000,000), matching how storage is billed. The only binary math in
# the SDK is multipart part sizing, because R2's part floor is 5 MiB.
Size = Union[int, str]
"""Bytes as an int, or a string like ``'2mb'``, ``'500kb'``, ``'5gb'``. Decimal units."""

Duration = Union[int, float, str, timedelta]
"""Seconds as a number, a ``timedelta``, or a string like ``'15m'``, ``'2h'``, ``'7d'``."""

CacheOption = Union[Duration, Literal["immutable", "revalidate", "no-store"]]
"""
The ``Cache-Control`` an object is stored with. Written once, at upload, and served on every read.

- ``'immutable'``: ``public, max-age=31536000, immutable``, for a versioned path that never changes
- ``'revalidate'``: ``public, max-age=0, must-revalidate``, for a stable path that gets overwritten
- ``'no-store'``: ``no-store``
- a duration (``'15m'``, ``3600``): ``public, max-age=<seconds>``
- unset: ``public, max-age=3600``
- anything containing ``=`` or ``,`` is a header, stored exactly as written

``private`` replaces ``public`` on a private bucket.
"""

_SIZE_UNITS: Dict[str, int] = {"b": 1, "kb": 10**3, "mb": 10**6, "gb": 10**9, "tb": 10**12}

_DURATION_UNITS: Dict[str, int] = {
    "ms": 1,
    "s": 1000,
    "sec": 1000,
    "second": 1000,
    "seconds": 1000,
    "m": 60_000,
    "min": 60_000,
    "minute": 60_000,
    "minutes": 60_000,
    "h": 3_600_000,
    "hr": 3_600_000,
    "hour": 3_600_000,
    "hours": 3_600_000,
    "d": 86_400_000,
    "day": 86_400_000,
    "days": 86_400_000,
}

_AMOUNT = re.compile(r"\s*(\d+(?:\.\d+)?)\s*([a-z]*)\s*", re.IGNORECASE | re.ASCII)


def parse_size(value: Size, what: str = "size") -> int:
    if isinstance(value, bool):
        raise TypeError(f"{what}: expected bytes or a string like '2mb', got {value!r}")
    if isinstance(value, (int, float)):
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{what}: expected a non-negative number of bytes, got {value}")
        return int(value)
    if not isinstance(value, str):
        raise TypeError(f"{what}: expected bytes or a string like '2mb', got {value!r}")
    m = _AMOUNT.fullmatch(value)
    if not m:
        raise ValueError(f"{what}: cannot parse {value!r} (try '2mb', '500kb', '5gb')")
    unit = (m.group(2) or "b").lower()
    mult = _SIZE_UNITS.get(unit)
    if mult is None:
        raise ValueError(f"{what}: unknown unit {m.group(2)!r} in {value!r} (b, kb, mb, gb, tb)")
    return int(float(m.group(1)) * mult)


def parse_duration(value: Duration, what: str = "duration") -> float:
    """Seconds. A bare number is seconds, the unit every TTL option on the web already uses."""
    if isinstance(value, bool):
        raise TypeError(f"{what}: expected seconds or a string like '15m', got {value!r}")
    if isinstance(value, timedelta):
        value = value.total_seconds()
    if isinstance(value, (int, float)):
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{what}: expected a non-negative number of seconds, got {value}")
        return float(value)
    if not isinstance(value, str):
        raise TypeError(f"{what}: expected seconds or a string like '15m', got {value!r}")
    m = _AMOUNT.fullmatch(value)
    if not m:
        raise ValueError(f"{what}: cannot parse {value!r} (try '15m', '2h', '7d')")
    unit = (m.group(2) or "s").lower()
    mult = _DURATION_UNITS.get(unit)
    if mult is None:
        raise ValueError(f"{what}: unknown unit {m.group(2)!r} in {value!r} (s, m, h, d)")
    return math.floor(float(m.group(1)) * mult) / 1000


def _round_half_up(n: float, digits: int) -> float:
    scale: float = 10.0**digits
    return math.floor(n * scale + 0.5) / scale


def format_bytes(size: float) -> str:
    """
    Decimal, the same way sizes are parsed, so a limit written ``'2mb'`` is refused as "2 MB". An
    exact multiple of the unit prints whole and anything else keeps a decimal; past 10 of a unit
    the decimal is dropped.
    """
    if not math.isfinite(size):
        return f"{size} B"
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(size)
    unit = 0
    # >= 999.95 and not 1000: at one decimal it would print as "1000 KB", which is a megabyte.
    while n >= 999.95 and unit < len(units) - 1:
        n /= 1000
        unit += 1
    if n.is_integer():
        return f"{int(n)} {units[unit]}"
    if n < 10:
        return f"{_round_half_up(n, 1):.1f} {units[unit]}"
    return f"{int(_round_half_up(n, 0))} {units[unit]}"


# A directive separator is what tells the two apart: a header always has one, a duration never does.
_RAW_HEADER = re.compile(r"[=,]")
_PRINTABLE = re.compile(r"[\x20-\x7e]*")


def cache_control(cache: Optional[CacheOption], visibility: str = "public") -> str:
    # `public` on an object only a signed request can read invites every shared cache between here
    # and the reader to keep a copy and hand it to the next one.
    scope = visibility
    if cache is None:
        return f"{scope}, max-age=3600"
    if cache == "immutable":
        return f"{scope}, max-age=31536000, immutable"
    # For a stable path that is overwritten: the copy is kept and checked with If-None-Match, so an
    # unchanged object costs a 304 instead of the whole object once every max-age.
    if cache == "revalidate":
        return f"{scope}, max-age=0, must-revalidate"
    if cache == "no-store":
        return "no-store"
    if isinstance(cache, str) and _RAW_HEADER.search(cache):
        raw = cache.strip()
        if not _PRINTABLE.fullmatch(raw):
            raise BlobError("invalid_input", "cache: a header value is printable ASCII")
        return raw
    return f"{scope}, max-age={math.floor(parse_duration(cache, 'cache'))}"
