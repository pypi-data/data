from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from pyarrow import RecordBatch

from pypi_data.datasets import CodeRepository

log = structlog.get_logger()

TARGET_SIZE = 1024 * 1024 * 1024 * 1.7  # 1.5 GB


def append_buffer(writer: pq.ParquetWriter, batch: pa.RecordBatch, roll_up_path: Path) -> bool:
    log.info(f"Writing batch: {batch.num_rows=} {batch.nbytes / 1024 / 1024:.1f} MB")
    writer.write_batch(batch)
    writer.file_handle.flush()
    size = roll_up_path.stat().st_size
    log.info(f"Got size: {size / 1024 / 1024:.1f} MB")
    return size >= TARGET_SIZE


async def fill_buffer(buffer: list[RecordBatch], client: httpx.AsyncClient, repo: CodeRepository, path: Path):
    log.info(f"Downloading {repo.dataset_url}")
    await repo.download_dataset(client, path)
    log.info(f'Downloaded, reading {path}')
    table = pq.read_table(path, memory_map=True).combine_chunks()
    buffer.extend(table.to_batches(max_chunksize=2_000_000))


async def combine_parquet(repositories: list[CodeRepository], directory: Path):
    directory.mkdir(exist_ok=True)
    # combined_file = directory / "combined.parquet"
    # temp_combined = directory / "temporary.parquet"
    repo_file = directory / f"repo.parquet"

    roll_up_count = 0
    buffer: list[RecordBatch] = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        while repositories:
            repo = repositories.pop(0)
            await fill_buffer(buffer, client, repo, repo_file)

            roll_up_path = directory / f"merged-{roll_up_count}.parquet"

            first_buffer = buffer.pop(0)

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

                    batch = buffer.pop(0)
                    if append_buffer(writer, batch, roll_up_path):
                        break

            roll_up_count += 1

        # for repo in repositories:
        #     log.info("Downloading dataset", repo=repo.name)
        #     await repo.download_dataset(client, repo_file)
        #
        #     log.info("Merging dataset", repo=repo.name)
        #     await asyncio.to_thread(append_parquet_file, temp_combined, [combined_file, repo_file])
        #     log.info(f"Merged size: {temp_combined.stat().st_size / 1024 / 1024:.1f} MB")
        #
        #     if temp_combined.stat().st_size < TARGET_SIZE:
        #         temp_combined.rename(combined_file)
        #         repo_file.unlink()
        #     else:
        #         # Too big! Roll over
        #         roll_up_count = finish_batch(combined_file, roll_up_count, directory, repo_file, temp_combined)
        #
        # if repo_file.exists():
        #     finish_batch(combined_file, roll_up_count, directory, repo_file, temp_combined)


def append_parquet_file(output: Path, paths: list[Path]) -> Path:
    table = pa.concat_tables(
        (pq.read_table(str(p), memory_map=True) for p in paths if p.exists()),
        promote_options="none"
    )
    pq.write_table(table, str(output), compression="snappy")
    return output
