import json
from pathlib import Path
from typing import Annotated, Iterable
from fsspec.implementations.http_sync import HTTPFileSystem
import typer
import polars as pl
from github import Github
from github import Auth
import requests
from concurrent.futures import ThreadPoolExecutor

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
    group_dir = output_path / "groups"
    group_dir.mkdir()

    for idx, paths in enumerate(group_by_size(g, target_size=target_size)):
        name = str(idx)
        outputs.append(name)
        (group_dir / name).write_text(json.dumps(paths))
        # print(f"Group {idx} contains {len(paths)} paths", file=sys.stderr)
    (output_path / "groups.json").write_text(json.dumps(outputs))


if __name__ == "__main__":
    app()
