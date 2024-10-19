import hashlib
from pathlib import Path

import httpx
import pyarrow
import pyarrow.parquet as pq
import structlog
from pyarrow import RecordBatch

from pypi_data.datasets import CodeRepository

log = structlog.get_logger()

TARGET_SIZE = 1024 * 1024 * 1024 * 1.8  # 1.8 GB


def append_buffer(writer: pq.ParquetWriter, batch: RecordBatch, roll_up_path: Path) -> bool:
    writer.write_batch(batch)
    writer.file_handle.flush()
    size = roll_up_path.stat().st_size
    log.info(f"Wrote batch: {batch.num_rows=} "
             f"Input: {batch.nbytes / 1024 / 1024:.1f} MB "
             f"Output: {size / 1024 / 1024:.1f} MB")
    return size >= TARGET_SIZE


async def fill_buffer(buffer: list[tuple[tuple[int, str], RecordBatch]], client: httpx.AsyncClient,
                      repo: CodeRepository,
                      path: Path):
    log.info(f"Downloading {repo.dataset_url}")
    await repo.download_dataset(client, path)
    log.info(f'Downloaded, reading {path}')
    table = pq.read_table(path, memory_map=True).combine_chunks()

    start = 0
    for idx, batch in enumerate(table.to_batches(max_chunksize=2_000_000)):
        batch: RecordBatch
        digest = hashlib.sha256()
        for item in batch.column("path").cast(pyarrow.large_binary()).to_pylist():
            digest.update(item)
        digest = digest.hexdigest()
        buffer.append(
            ((repo.number, digest), batch)
        )
        start += batch.num_rows


def hash_parquet_keys(keys: list[tuple[int, str]]) -> str:
    combined = "-".join(f"{number}-{digest}" for (number, digest) in keys)
    return hashlib.sha256(combined.encode()).hexdigest()


async def combine_parquet(repositories: list[CodeRepository], directory: Path):
    directory.mkdir(exist_ok=True)
    repo_file = directory / f"repo.parquet"

    roll_up_count = 0
    buffer: list[tuple[tuple[int, str], RecordBatch]] = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        while repositories:
            repo = repositories.pop(0)
            await fill_buffer(buffer, client, repo, repo_file)

            roll_up_path = directory / f"merged-{roll_up_count}.parquet"

            keys = []
            first_key, first_buffer = buffer.pop(0)
            keys.append(first_key)

            with pq.ParquetWriter(roll_up_path,
                                  compression="zstd",
                                  compression_level=7,
                                  write_statistics=True,
                                  schema=first_buffer.schema) as writer:
                log.info(f"Writing {repo_file}")
                append_buffer(writer, first_buffer, roll_up_path)

                while buffer or repositories:
                    if not buffer:
                        repo = repositories.pop(0)
                        await fill_buffer(buffer, client, repo, repo_file)

                    key, batch = buffer.pop(0)
                    keys.append(key)
                    if append_buffer(writer, batch, roll_up_path):
                        break

            hashed_keys = hash_parquet_keys(keys)
            final_path = roll_up_path.rename(
                directory / f"dataset-{hashed_keys[:8]}.parquet"
            )

            log.info(
                f"Finished batch {roll_up_count}, {len(keys)} batches, {final_path.name=} {final_path.stat().st_size / 1024 / 1024:.1f} MB"
            )

            roll_up_count += 1
