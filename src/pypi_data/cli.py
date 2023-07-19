from pathlib import Path
from typing import Annotated, Iterable
import json
import typer
import polars as pl
from github import Github
from github import Auth
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

app = typer.Typer()


@app.command()
def print_index_urls(github_token: Annotated[str, typer.Argument(envvar="GITHUB_TOKEN")]):
    auth = Auth.Token(github_token)
    g = Github(auth=auth)
    g.per_page = 100
    for repo in g.get_organization("pypi-data").get_repos():
        if repo.name.startswith('pypi-mirror-'):
            print(f'{repo.html_url}/releases/download/latest/dataset.parquet')
            # print(json.dumps({
            #     "url": f'{repo.html_url}/releases/download/latest/dataset.parquet',
            #     "repo": repo.name,
            # }))


def group_by_size(directory: Path, target_size: int) -> Iterable[list[Path]]:
    names = []
    total_size = 0
    for path in directory.glob("*.parquet"):
        names.append(path)
        total_size += path.stat().st_size
        if total_size >= target_size:
            yield names
            names.clear()
            total_size = 0
    yield names


@app.command()
def compact_indexes(
        directory: Annotated[Path, typer.Argument(dir_okay=True, file_okay=False, writable=True, resolve_path=True)],
        output_dir: Annotated[Path, typer.Argument(dir_okay=True, file_okay=False, writable=True, resolve_path=True)],
):
    frames = []
    for idx, paths in enumerate(group_by_size(directory, target_size=int(1024 * 1024 * 1024 * 1.3))):
        frames.append(pl.concat([
            pl.scan_parquet(path, low_memory=True)
            for path in paths
        ]))

    with ThreadPoolExecutor() as pool:
        result = pool.map(lambda f: f[1].sink_parquet(
            output_dir / f'{f[0]}.parquet',
            compression='zstd',
            compression_level=12,
            statistics=True,
            maintain_order=False,
        ), enumerate(frames))
        list(tqdm(result, total=len(frames)))


if __name__ == '__main__':
    app()
