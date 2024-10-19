import asyncio
import itertools
from pathlib import Path
from typing import Annotated, Iterable, Optional

import httpx
import requests
import structlog
import tqdm
import typer
import zstd
from github import Auth
from github import Github
from pydantic import RootModel

from pypi_data.combine_parquet import combine_parquet
from pypi_data.datasets import CodeRepository

app = typer.Typer()
session = requests.Session()
MB = 1024 * 1024

log = structlog.get_logger()

GithubToken = Annotated[str, typer.Option(envvar="GITHUB_TOKEN")]

Repos = RootModel[list[CodeRepository]]


def get_dataset_urls(github: Github) -> Iterable[tuple[str, str, str]]:
    for repo in github.get_organization("pypi-data").get_repos():
        if repo.name.startswith("pypi-mirror-"):
            yield f'{repo.html_url}.git', repo.ssh_url, f"{repo.html_url}/releases/download/latest/dataset.parquet"


def github_client(github_token) -> Github:
    auth = Auth.Token(github_token)
    g = Github(auth=auth)
    g.per_page = 100
    return g


def _read_path(path: Path) -> str:
    contents = path.read_bytes()
    if path.suffix == ".zstd":
        return zstd.decompress(contents).decode("utf-8")
    return contents.decode("utf-8")


def _write_path(path: Path, contents: str):
    if path.suffix == ".zstd":
        data = zstd.compress(contents.encode("utf-8"), 9)
    else:
        data = contents.encode("utf-8")
    log.info(f'Writing {len(data) // MB} MB to {path}')
    path.write_bytes(data)


@app.command()
def load_repos(github_token: GithubToken, output: Path, limit: Annotated[Optional[int], typer.Option()] = None):
    g = github_client(github_token)
    repos = CodeRepository.fetch_all(g)
    if limit:
        repos = repos[:limit]
    asyncio.run(load_indexes(repos))
    dumped = Repos(repos).model_dump_json()
    _write_path(output, dumped)


async def load_indexes(repositories: list[CodeRepository], concurrency: int = 10):
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        with tqdm.tqdm(total=len(repositories)) as pbar:
            async def _run(r: CodeRepository) -> CodeRepository:
                async with semaphore:
                    result = await r.load_index(client)
                    pbar.update(1)
                    return result

            async with asyncio.TaskGroup() as tg:
                results = [
                    tg.create_task(_run(repo))
                    for repo in repositories
                ]
            log.info(f'Fetched {len(results)} repository indexes')


@app.command()
def create_links(github_token: GithubToken, repo_path: Path):
    log.info(f'Reading repos from {repo_path}')
    contents = _read_path(repo_path)
    repos = Repos.model_validate_json(contents)
    log.info(f'Loaded: writing links')

    links_path = Path("links")

    sorted_repos = sorted(repos.root, key=lambda r: r.number)

    (links_path / "dataset.txt").write_text(
        "\n".join(str(repo.dataset_url) for repo in sorted_repos)
    )

    (links_path / "repositories.txt").write_text(
        "\n".join(str(repo.url) for repo in sorted_repos)
    )

    (links_path / "repositories_ssh.txt").write_text(
        "\n".join(str(repo.ssh_url) for repo in sorted_repos)
    )

    (links_path / "repositories.json").write_text(
        Repos([r.without_index() for r in sorted_repos]).model_dump_json(indent=2, exclude_none=True)
    )


@app.command()
def merge_datasets(repo_path: Path, output: Path):
    contents = _read_path(repo_path)
    repos = Repos.model_validate_json(contents)
    asyncio.run(combine_parquet(repos.root, output))


@app.command()
def output_sql(repo_path: Path, redirects: Annotated[bool, typer.Option()] = False,
               batches_of: Annotated[Optional[int], typer.Option()] = None,
               settings: Annotated[str, typer.Option()] = '',
               dialect: Annotated[str, typer.Option()] = 'duckdb',
               insert: Annotated[bool, typer.Option()] = False):
    # log.info(f'Reading repos from {repo_path}')
    contents = _read_path(repo_path)
    repos = Repos.model_validate_json(contents)

    if redirects:
        urls = asyncio.run(resolve_dataset_redirects(repos.root))
    else:
        urls = [r.dataset_url for r in repos.root]

    if dialect == 'duckdb':
        def make_url(url: str):
            return f"read_parquet('{url}')"
    elif dialect == 'clickhouse':
        def make_url(url: str):
            return f"url('{url}', 'Parquet')"
    else:
        raise ValueError(f"Unknown dialect: {dialect}")

    sql_queries = [
        f"SELECT * FROM {make_url(url)}"
        for url in urls
    ]

    if batches_of:
        batches = list(itertools.batched(sql_queries, batches_of))
    else:
        batches = [sql_queries]

    for batch in batches:
        query = 'INSERT INTO output\n' if insert else ''
        query += "\nUNION ALL ".join(batch)
        if settings:
            query += f'\nSETTINGS {settings}'

        print(query + ';\n')


async def resolve_dataset_redirects(repositories: list[CodeRepository], concurrency: int = 10) -> list[str]:
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        with tqdm.tqdm(total=len(repositories)) as pbar:
            async def _run(r: CodeRepository) -> str | None:
                async with semaphore:
                    result = await r.get_temporary_dataset_url(client)
                    pbar.update(1)
                    return result

            async with asyncio.TaskGroup() as tg:
                results = [
                    tg.create_task(_run(repo))
                    for repo in repositories
                ]
            return [r.result() for r in results if r.result() is not None]


if __name__ == "__main__":
    app()
