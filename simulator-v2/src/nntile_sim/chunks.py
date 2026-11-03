"""Fixed-size GPU chunks with potentially smaller final payloads."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Chunk:
    handle: str
    index: int
    size_bytes: int
    reserved_bytes: int

    @property
    def key(self) -> tuple[str, int]:
        return self.handle, self.index


def split_into_chunks(
    handle: str,
    size_bytes: int,
    chunk_size_bytes: int,
) -> tuple[Chunk, ...]:
    if size_bytes <= 0:
        raise ValueError("Handle size must be positive")
    if chunk_size_bytes <= 0:
        raise ValueError("Chunk size must be positive")

    result = []
    for index, offset in enumerate(range(0, size_bytes, chunk_size_bytes)):
        actual_size = min(chunk_size_bytes, size_bytes - offset)
        result.append(
            Chunk(
                handle=handle,
                index=index,
                size_bytes=actual_size,
                reserved_bytes=chunk_size_bytes,
            )
        )

    return tuple(result)