import asyncio
import contextlib
import gzip
import io
from pathlib import Path
from typing import (
    Annotated,
    Iterable,
    Optional,
    BinaryIO,
    Generator,
    Literal,
)

import hishel
import httpx
import pydantic
import requests
import structlog
import tqdm
import typer
from github import Auth
from github import Github
from pydantic import RootModel, ByteSize

from pypi_data.combine_parquet import combine_parquet
from pypi_data.datasets import CodeRepository

app = typer.Typer()
session = requests.Session()
MB = 1024 * 1024

log = structlog.get_logger()

GithubToken = Annotated[str, typer.Option(envvar="GITHUB_TOKEN")]

Repos = RootModel[list[CodeRepository]]

cache_storage = hishel.AsyncSQLiteStorage()
cache_controller = hishel.Controller(
    cacheable_methods=["GET"],
    cacheable_status_codes=[200],
    allow_stale=True,
    always_revalidate=True,
)


def get_dataset_urls(github: Github) -> Iterable[tuple[str, str, str]]:
    for repo in github.get_organization("pypi-data").get_repos():
        if repo.name.startswith("pypi-mirror-"):
            yield (
                f"{repo.html_url}.git",
                repo.ssh_url,
                f"{repo.html_url}/releases/download/latest/dataset.parquet",
            )


def github_client(github_token) -> Github:
    auth = Auth.Token(github_token)
    g = Github(auth=auth)
    g.per_page = 100
    return g


@contextlib.contextmanager
def open_path(
    path: Path, mode: Literal["wb", "wt", "rb", "rt"]
) -> Generator[BinaryIO, None, None]:
    if path.suffix == ".gz":
        with gzip.open(path, mode) as gzip_fd:
            yield gzip_fd
    else:
        with path.open(mode, buffering=io.DEFAULT_BUFFER_SIZE) as fd:
            yield fd

    log.info(
        f"Finished open_path on {path} with {mode} - {path.stat().st_size / MB:.2f} MB"
    )


@app.command()
def load_repos(
    github_token: GithubToken,
    repos_file: Path,
    links_path: Path,
    limit: Annotated[Optional[int], typer.Option()] = None,
):
    g = github_client(github_token)
    repos = CodeRepository.fetch_all(g)
    if limit:
        repos = repos[:limit]

    repos = asyncio.run(load_indexes(repos))
    repos = sorted(repos, key=lambda r: r.number)

    log.info("Writing links")

    (links_path / "dataset.txt").write_text(
        "\n".join(str(repo.dataset_url) for repo in repos)
    )

    (links_path / "repositories.txt").write_text(
        "\n".join(str(repo.url) for repo in repos)
    )

    (links_path / "repositories_ssh.txt").write_text(
        "\n".join(str(repo.ssh_url) for repo in repos)
    )

    (links_path / "repositories.json").write_text(
        Repos([r.without_index() for r in repos]).model_dump_json(
            indent=2, exclude_none=True
        )
    )

    log.info("Serializing data")
    serialized = [
        repo.model_dump_json() + "\n"
        for repo in tqdm.tqdm(repos, mininterval=1, desc="Serializing")
    ]

    with open_path(repos_file, mode="wt") as fd:
        fd.writelines(tqdm.tqdm(serialized, mininterval=1, desc="Writing"))


async def load_indexes(
    repositories: list[CodeRepository], concurrency: int = 25
) -> list[CodeRepository]:
    semaphore = asyncio.Semaphore(concurrency)
    results = []
    async with hishel.AsyncCacheClient(
        controller=cache_controller, storage=cache_storage
    ) as client:
        with tqdm.tqdm(
            total=len(repositories), mininterval=1, desc="Loading indexes"
        ) as pbar:

            async def _run(r: CodeRepository) -> CodeRepository | None:
                async with semaphore:
                    result = await r.with_index(client)
                    pbar.update(1)
                    return result

            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(_run(repo)) for repo in repositories]

            for res in tasks:
                res = res.result()
                if res is not None:
                    results.append(res)

    log.info(f"Fetched {len(tasks)} repository indexes")
    return results


@app.command()
def merge_datasets(
    repo_path: Path,
    output: Path,
    max_buffer_size: Annotated[str, typer.Option()] = "10GB",
):
    with open_path(repo_path, mode="rb") as fd:
        repos = Repos.model_validate_json(fd.read()).root
    max_buffer_size = pydantic.RootModel[ByteSize].model_validate(max_buffer_size).root
    asyncio.run(combine_parquet(repos, output, max_buffer_size))


async def resolve_dataset_redirects(
    repositories: list[CodeRepository], concurrency: int = 10
) -> list[str]:
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        with tqdm.tqdm(
            total=len(repositories), mininterval=1, desc="Resolving redirects"
        ) as pbar:

            async def _run(r: CodeRepository) -> str | None:
                async with semaphore:
                    result = await r.get_temporary_dataset_url(client)
                    pbar.update(1)
                    return result

            async with asyncio.TaskGroup() as tg:
                results = [tg.create_task(_run(repo)) for repo in repositories]
            return [r.result() for r in results if r.result() is not None]


if __name__ == "__main__":
    app()
