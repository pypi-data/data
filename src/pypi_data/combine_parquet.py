import asyncio
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from pypi_data.datasets import CodeRepository

log = structlog.get_logger()

TARGET_SIZE = 1024 * 1024 * 1024 * 1.5  # 1.5 GB


def finish_batch(combined_file: Path, roll_up_count: int, directory: Path, repo_file: Path, temp_combined: Path) -> int:
    merged = combined_file.rename(directory / f"merged-{roll_up_count}.parquet")
    log.info(f"Created merged file {roll_up_count} with size {merged.stat().st_size / 1024 / 1024:.1f} MB")
    repo_file.rename(combined_file)
    temp_combined.unlink()
    return roll_up_count + 1


async def combine_parquet(repositories: list[CodeRepository], directory: Path):
    directory.mkdir(exist_ok=True)
    combined_file = directory / "combined.parquet"
    temp_combined = directory / "temporary.parquet"
    repo_file = directory / f"repo.parquet"

    roll_up_count = 0

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for repo in repositories:
            log.info("Downloading dataset", repo=repo.name)
            await repo.download_dataset(client, repo_file)
            log.info("Merging dataset", repo=repo.name)
            await asyncio.to_thread(append_parquet_file, temp_combined, [combined_file, repo_file])
            log.info(f"Merged size: {temp_combined.stat().st_size / 1024 / 1024:.1f} MB")

            if temp_combined.stat().st_size < TARGET_SIZE:
                temp_combined.rename(combined_file)
                repo_file.unlink()
            else:
                # Too big! Roll over
                roll_up_count = finish_batch(combined_file, roll_up_count, directory, repo_file, temp_combined)

        if repo_file.exists():
            finish_batch(combined_file, roll_up_count, directory, repo_file, temp_combined)


def append_parquet_file(output: Path, paths: list[Path]) -> Path:
    table = pa.concat_tables(
        (pq.read_table(str(p), memory_map=True) for p in paths if p.exists()),
        promote_options="none"
    )
    pq.write_table(table, str(output), compression="zstd", compression_level=6)
    return output
