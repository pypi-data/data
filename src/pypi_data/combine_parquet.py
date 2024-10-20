import asyncio
import hashlib
import time
from collections import deque
from pathlib import Path
from typing import Deque

import httpx
import psutil
import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from pyarrow import RecordBatch
from pydantic import ByteSize

from pypi_data.datasets import CodeRepository

log = structlog.get_logger()

IO_BUFFER_SIZE = 1024 * 1024 * 50  # 50 MB


def append_buffer(
    fd: pyarrow.BufferOutputStream,
    writer: pq.ParquetWriter,
    batch: RecordBatch,
    roll_up_path: Path,
    target_size: int,
) -> bool:
    initial_size = roll_up_path.stat().st_size
    writer.write_batch(batch)
    fd.flush()
    end_size = roll_up_path.stat().st_size
    written_size = end_size - initial_size
    disk_usage = psutil.disk_usage(str(roll_up_path.absolute()))
    cpu_usage = psutil.cpu_percent(percpu=True)

    log.info(
        f"Wrote {batch.num_rows} rows "
        f"Batch Size: {ByteSize(batch.nbytes).human_readable(decimal=True)} "
        f"Initial Size: {ByteSize(initial_size).human_readable(decimal=True)} "
        f"End Size: {ByteSize(end_size).human_readable(decimal=True)} "
        f"Written: {ByteSize(written_size).human_readable(decimal=True)}"
    )
    log.info(
        f"System Stats: "
        f"cpu={cpu_usage}% "
        f"total={ByteSize(disk_usage.total).human_readable(decimal=True)} "
        f"used={ByteSize(disk_usage.used).human_readable(decimal=True)} "
        f"free={ByteSize(disk_usage.free).human_readable(decimal=True)} "
        f"percent={disk_usage.percent}%"
    )
    return end_size >= target_size


def buffer_mem_size(buffer: Deque[tuple[tuple[int, str], RecordBatch]]) -> int:
    return sum(batch.nbytes for (_, _), batch in buffer)


async def fill_buffer(
    buffer: Deque[tuple[tuple[int, str], RecordBatch]],
    max_buffer_size: int,
    client: httpx.AsyncClient,
    repositories: Deque[CodeRepository],
) -> bool:
    while repositories:
        time_hashing_ns = 0
        time_iterating_ns = 0
        time_loading_ns = 0

        start_time_ns = time.perf_counter_ns()

        buffer_size = buffer_mem_size(buffer)
        log.info(f"Buffer size: {ByteSize(buffer_size).human_readable(decimal=True)}")
        if buffer_size >= max_buffer_size:
            log.info(f"Buffer filled with {len(buffer)} entries")
            break

        repo = repositories.popleft()
        log.info(f"Downloading {repo.dataset_url}")
        start_downloading_ns = time.perf_counter_ns()
        if (dataset_bytes := await repo.download_dataset(client)) is False:
            log.info(f"Failed to download {repo.dataset_url}")
            continue

        time_downloading_ns = time.perf_counter_ns() - start_downloading_ns

        start_load_time = time.perf_counter_ns()

        table_batches = await asyncio.to_thread(
            lambda: pq.read_table(pa.py_buffer(memoryview(dataset_bytes)))
            .combine_chunks()
            .to_batches(max_chunksize=2_500_000)
        )
        del dataset_bytes
        time_loading_ns += time.perf_counter_ns() - start_load_time

        iterator = iter(enumerate(table_batches))

        while True:
            start_iterate_time = time.perf_counter_ns()
            try:
                idx, batch = next(iterator)
            except StopIteration:
                break

            time_iterating_ns += time.perf_counter_ns() - start_iterate_time

            batch: RecordBatch

            start_hash_time = time.perf_counter_ns()

            # Hash the path column with zero copies.
            data_buffer = batch.column("path").cast(pyarrow.large_binary()).buffers()[1]
            digest = hashlib.sha256(memoryview(data_buffer)).hexdigest()

            time_hashing_ns += time.perf_counter_ns() - start_hash_time

            buffer.append(((repo.number, digest), batch))

        runtime_ns = time.perf_counter_ns() - start_time_ns

        log.info(
            f"Perf: total={runtime_ns // 1_000_000} ms "
            f"download={time_downloading_ns // 1_000_000} ms "
            f"load={time_loading_ns // 1_000_000} ms "
            f"iter={time_iterating_ns // 1_000_000} ms "
            f"hash={time_hashing_ns // 1_000_000} ms"
        )

    return bool(buffer)


def hash_parquet_keys(keys: list[tuple[int, str]]) -> str:
    combined = "-".join(f"{number}-{digest}" for (number, digest) in keys)
    return hashlib.sha256(combined.encode()).hexdigest()


async def combine_parquet(
    repositories: list[CodeRepository],
    directory: Path,
    max_buffer_size: ByteSize,
    target_size: ByteSize,
):
    directory.mkdir(exist_ok=True)

    roll_up_count = 0
    buffer: Deque[tuple[tuple[int, str], RecordBatch]] = deque()
    repositories: Deque[CodeRepository] = deque(repositories)

    total_memory = psutil.virtual_memory().total
    max_buffer_size = min(
        int(total_memory * 0.75),  # 75% of total memory
        max_buffer_size,  # 10 GB
    )
    log.info(
        f"Total system memory: {ByteSize(total_memory).human_readable(decimal=True)}"
    )
    log.info(
        f"Configured buffer size: {ByteSize(max_buffer_size).human_readable(decimal=True)}"
    )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        while repositories:
            if (
                await fill_buffer(buffer, max_buffer_size, client, repositories)
                is False
            ):
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
                append_buffer(fd, writer, first_buffer, roll_up_path, target_size)

                while buffer or repositories:
                    if not buffer:
                        res = await fill_buffer(
                            buffer, max_buffer_size, client, repositories
                        )
                        if res is None:
                            continue

                    key, batch = buffer.popleft()
                    keys.append(key)
                    if append_buffer(fd, writer, batch, roll_up_path, target_size):
                        break

            hashed_keys = hash_parquet_keys(keys)
            final_path = roll_up_path.rename(
                directory / f"dataset-{hashed_keys[:8]}.parquet"
            )

            log.info(
                f"Finished batch {roll_up_count}, {len(keys)} batches, {final_path.name=} {ByteSize(final_path.stat().st_size).human_readable(decimal=True)}"
            )

            roll_up_count += 1
