from pathlib import Path
from typing import BinaryIO

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from pypi_data.datasets import CodeRepository

log = structlog.get_logger()

TARGET_SIZE = 1024 * 1024 * 1024 * 1.5  # 1.5 GB


# def new_combined_file() -> Generator:
#     merged = combined_file.rename(directory / f"merged-{roll_up_count}.parquet")
#     log.info(f"Created merged file {roll_up_count} with size {merged.stat().st_size / 1024 / 1024:.1f} MB")
#     repo_file.rename(combined_file)
#     temp_combined.unlink()
#     return roll_up_count + 1


def append_pq(writer: pq.ParquetWriter, repo_file: Path, fd: BinaryIO, roll_up_path: Path) -> bool:
    log.info(f"Writing {repo_file}")
    writer.write_table(pq.read_table(repo_file, memory_map=True))
    fd.flush()
    size = roll_up_path.stat().st_size
    log.info(f"Got size: {size / 1024 / 1024:.1f} MB")
    return size >= TARGET_SIZE


async def combine_parquet(repositories: list[CodeRepository], directory: Path):
    directory.mkdir(exist_ok=True)
    # combined_file = directory / "combined.parquet"
    # temp_combined = directory / "temporary.parquet"
    repo_file = directory / f"repo.parquet"

    roll_up_count = 0

    async with httpx.AsyncClient(follow_redirects=True) as client:
        while repositories:
            repo = repositories.pop(0)
            await repo.download_dataset(client, repo_file)
            schema = pq.read_schema(repo_file)

            roll_up_path = directory / f"merged-{roll_up_count}.parquet"

            with roll_up_path.open("wb") as fd:
                with pq.ParquetWriter(fd,
                                      compression="zstd",
                                      compression_level=6,
                                      write_statistics=True,
                                      schema=schema) as writer:
                    log.info(f"Writing {repo_file}")
                    append_pq(writer, repo_file, fd, roll_up_path)

                    while repositories:
                        repo = repositories.pop(0)
                        await repo.download_dataset(client, repo_file)
                        if append_pq(writer, repo_file, fd, roll_up_path):
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
