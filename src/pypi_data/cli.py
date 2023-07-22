import json
import sys
import tempfile
from pathlib import Path
from typing import Annotated, Iterable
from fsspec.implementations.http_sync import HTTPFileSystem
import typer
import shutil
import polars as pl
from github import Github
from github import Auth
import requests
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

app = typer.Typer()
session = requests.Session()
MB = 1024 * 1024
GB = MB * 1024

GithubToken = Annotated[str, typer.Option(envvar="GITHUB_TOKEN")]


def _get_index_urls(github: Github) -> Iterable[tuple[str]]:
    for repo in github.get_organization("pypi-data").get_repos():
        if repo.name.startswith("pypi-mirror-"):
            yield f"{repo.html_url}/releases/download/latest/dataset.parquet"


def github_client(github_token) -> Github:
    auth = Auth.Token(github_token)
    g = Github(auth=auth)
    g.per_page = 100
    return g


@app.command()
def print_index_urls(github_token: GithubToken):
    g = github_client(github_token)
    for url in _get_index_urls(g):
        print(url)


def group_by_size(github: Github, target_size: int) -> Iterable[list[str]]:
    fs = HTTPFileSystem()
    urls = _get_index_urls(github)
    with ThreadPoolExecutor() as pool:
        stat_results = pool.map(lambda url: fs.stat(url), urls)

        names = []
        total_size = 0
        for stat_result in stat_results:
            names.append(stat_result["name"])
            total_size += stat_result["size"]
            if total_size >= target_size:
                yield names
                names.clear()
                total_size = 0
        yield names


@app.command()
def group_index_urls(github_token: GithubToken,
                     output_path: Annotated[Path, typer.Argument(dir_okay=True, file_okay=False)],
                     target_size: int = GB * 1.3):
    g = github_client(github_token)
    outputs = []
    for idx, paths in enumerate(group_by_size(g, target_size=target_size)):
        name = str(idx)
        outputs.append(name)
        (output_path / name).write_text(
            json.dumps(paths, indent=2)
        )
        print(f"Group {idx} contains {len(paths)} paths", file=sys.stderr)
    print(json.dumps(outputs))


def _download_file(url: str, to: Path):
    with requests.get(url, stream=True) as r:
        with to.open("wb") as f:
            shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)


@app.command()
def compact_indexes(github_token: GithubToken):
    g = github_client(github_token)
    repo = g.get_repo("pypi-data/data")
    new_release = repo.create_git_release(
        "latest", "latest", draft=True, target_commitish="main", message="latest data"
    )
    for idx, paths in enumerate(group_by_size(g, target_size=int(GB * 1.3))):
        print(f"Downloading group {idx} containing {len(paths)} paths")
        with tempfile.TemporaryDirectory() as tmpdir, ThreadPoolExecutor() as pool:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            input_dir.mkdir()
            output_dir.mkdir()
            list(
                pool.map(
                    lambda r: _download_file(r[1], input_dir / f"{r[0]}.parquet"),
                    enumerate(paths),
                )
            )
            print(f"Downloaded group {idx}. Combining...")
            combined = pl.scan_parquet(f"{input_dir}/*.parquet", low_memory=False)
            combined.sink_parquet(
                output_dir / f"{idx}.parquet",
                compression="zstd",
                compression_level=14,
                statistics=True,
                maintain_order=False,
                row_group_size=64 * MB,
                data_pagesize_limit=1 * MB,
            )
            new_release.upload_asset(str(output_dir / f"{idx}.parquet"))
            print(f"Downloaded group {idx}")


if __name__ == "__main__":
    app()
