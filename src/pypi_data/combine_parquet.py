import hashlib
from collections import deque
from pathlib import Path
from typing import Deque

import httpx
import pyarrow
import pyarrow.parquet as pq
import structlog
from pyarrow import RecordBatch

from pypi_data.datasets import CodeRepository

log = structlog.get_logger()

TARGET_SIZE = 1024 * 1024 * 1024 * 1.8  # 1.8 GB
FILL_BUFFER_MEM_SIZE = 1024 * 1024 * 1024 * 5  # 5 GB
IO_BUFFER_SIZE = 1024 * 1024 * 50  # 50 MB


def append_buffer(
    fd: pyarrow.BufferOutputStream,
    writer: pq.ParquetWriter,
    batch: RecordBatch,
    roll_up_path: Path,
) -> bool:
    writer.write_batch(batch)
    fd.flush()
    size = roll_up_path.stat().st_size
    log.info(
        f"Wrote batch: {batch.num_rows=} "
        f"Input: {batch.nbytes / 1024 / 1024:.1f} MB "
        f"Output: {size / 1024 / 1024:.1f} MB"
    )
    return size >= TARGET_SIZE


def buffer_mem_size(buffer: Deque[tuple[tuple[int, str], RecordBatch]]) -> int:
    return sum(batch.nbytes for (_, _), batch in buffer)


def buffer_at_capacity(size: int) -> bool:
    return size >= FILL_BUFFER_MEM_SIZE


async def fill_buffer(
    buffer: Deque[tuple[tuple[int, str], RecordBatch]],
    client: httpx.AsyncClient,
    repositories: Deque[CodeRepository],
    path: Path,
) -> bool:
    while repositories:
        buffer_size = buffer_mem_size(buffer)
        log.info(f"Buffer size: {buffer_size / 1024 / 1024:.1f} MB")
        if buffer_at_capacity(buffer_size):
            log.info("Buffer filled")
            break

        repo = repositories.popleft()
        log.info(f"Downloading {repo.dataset_url}")
        if await repo.download_dataset(client, path) is False:
            log.info(f"Failed to download {repo.dataset_url}")
            continue
        log.info(f"Downloaded, reading {path}")
        table = pq.read_table(path, memory_map=True).combine_chunks()

        for idx, batch in enumerate(table.to_batches(max_chunksize=2_000_000)):
            batch: RecordBatch
            digest = hashlib.sha256()
            for item in batch.column("path").cast(pyarrow.large_binary()).to_pylist():
                digest.update(item)
            digest = digest.hexdigest()
            buffer.append(((repo.number, digest), batch))

    return bool(buffer)


def hash_parquet_keys(keys: list[tuple[int, str]]) -> str:
    combined = "-".join(f"{number}-{digest}" for (number, digest) in keys)
    return hashlib.sha256(combined.encode()).hexdigest()


async def combine_parquet(repositories: list[CodeRepository], directory: Path):
    directory.mkdir(exist_ok=True)
    repo_file = directory / "repo.parquet"

    roll_up_count = 0
    buffer: Deque[tuple[tuple[int, str], RecordBatch]] = deque()
    repositories: Deque[CodeRepository] = deque(repositories)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        while repositories:
            if await fill_buffer(buffer, client, repositories, repo_file) is False:
                continue

            roll_up_path = directory / f"merged-{roll_up_count}.parquet"

            keys = []
            first_key, first_buffer = buffer.popleft()
            keys.append(first_key)

            with pyarrow.output_stream(
                roll_up_path, compression=None, buffer_size=IO_BUFFER_SIZE
            ) as fd, pq.ParquetWriter(
                fd,
                compression="zstd",
                compression_level=7,
                write_statistics=True,
                schema=first_buffer.schema,
            ) as writer:
                log.info(f"Writing {repo_file}")
                append_buffer(fd, writer, first_buffer, roll_up_path)

                while buffer or repositories:
                    if not buffer:
                        res = await fill_buffer(buffer, client, repositories, repo_file)
                        if res is None:
                            continue

                    key, batch = buffer.popleft()
                    keys.append(key)
                    if append_buffer(fd, writer, batch, roll_up_path):
                        break

            hashed_keys = hash_parquet_keys(keys)
            final_path = roll_up_path.rename(
                directory / f"dataset-{hashed_keys[:8]}.parquet"
            )

            log.info(
                f"Finished batch {roll_up_count}, {len(keys)} batches, {final_path.name=} {final_path.stat().st_size / 1024 / 1024:.1f} MB"
            )

            roll_up_count += 1
