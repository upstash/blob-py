import math
from typing import Optional, Union

from upstash_blob._units import Size, format_bytes, parse_size
from upstash_blob.errors import BlobError

MULTIPART_THRESHOLD = 16_000_000
"""Decimal, 16 MB. Under it a body goes up as one PUT, over it in parts."""

SINGLE_PUT_MAX = 5 * 1024 * 1024 * 1024
"""R2 refuses a single PUT larger than this, so past it there is no choice but parts."""

MultipartOption = Union[bool, Size]
"""``True`` always parts, ``False`` never does, a size is the threshold to use instead of 16 MB."""


def wants_multipart(option: Optional[MultipartOption], size: int) -> bool:
    """Whether a body of this size goes up in parts. Raises only for a ``False`` R2 cannot honour."""
    if size > SINGLE_PUT_MAX:
        if option is False:
            raise BlobError(
                "too_large",
                f"{format_bytes(size)} is over the {format_bytes(SINGLE_PUT_MAX)} a single PUT can carry",
                hint="multipart=False forbids the parts this body needs",
            )
        return True
    if isinstance(option, bool):
        return option
    return size > (MULTIPART_THRESHOLD if option is None else parse_size(option, "multipart"))


# Part math is binary: R2's floor for every part but the last is 5 MiB, checked at complete.
_MIB = 1024 * 1024
_MIN_PART = 5 * _MIB
_TARGET_PARTS = 250


def part_size_for(size: int) -> int:
    """max(5 MiB, ceil(size / 250)) rounded up to a MiB."""
    return max(_MIN_PART, math.ceil(math.ceil(size / _TARGET_PARTS) / _MIB) * _MIB)


def part_count(size: int, part_size: int) -> int:
    return max(1, math.ceil(size / part_size))


# Every part is buffered whole so a failed one can be sent again, and the next one is read only once a
# slot is free, so each part in flight costs its size in memory. Four overlap the round trips the way
# boto3's transfer manager does; past 16 MiB parts (bodies over about 4 GB) fewer go at once to stay
# within the budget, down to one past 32 MiB parts (about 8 GB).
PART_CONCURRENCY = 4
_PART_BUFFER_BUDGET = 64 * _MIB


def part_concurrency_for(part_size: int) -> int:
    """Parts sent at once for this part size: up to four, within 64 MiB; never under one."""
    return max(1, min(PART_CONCURRENCY, _PART_BUFFER_BUDGET // part_size))
