from collections.abc import Hashable, Mapping


type ChunkSize = str | int | tuple[int, ...] | None
type Chunks = Mapping[str | Hashable, ChunkSize] | None
