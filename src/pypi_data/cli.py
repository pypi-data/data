import enum
import os
import tempfile
import threading
import time
import pandas as pd
import duckdb
import json
from pathlib import Path
from typing import Annotated, Iterable, List, Optional

import psutil
from fsspec.implementations.http_sync import HTTPFileSystem
import typer
import prql_python as prql
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
        if names:
            yield names


@app.command()
def group_index_urls(github_token: GithubToken,
                     output_path: Annotated[Path, typer.Argument(dir_okay=True, file_okay=False)],
                     target_size: int = 2.2 * GB):
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


def get_size(bytes):
    """
    Returns size of bytes in a nice format
    """
    for unit in ['', 'K', 'M', 'G', 'T', 'P']:
        if bytes < 1024:
            return f"{bytes:.2f}{unit}B"
        bytes /= 1024


class OutputFormat(enum.StrEnum):
    PARQUET = "parquet"
    JSON = "json"


@app.command()
def run_sql(
        prql_file: Annotated[Path, typer.Argument(dir_okay=False, file_okay=True, readable=True)],
        output_file: Annotated[Path, typer.Argument(dir_okay=False, file_okay=True, writable=True)],
        parameter: Annotated[Optional[List[str]], typer.Argument()] = None,
        output: Annotated[OutputFormat, typer.Option()] = OutputFormat.PARQUET,
        threads: Annotated[int, typer.Option()] = 2,
        no_limits: Annotated[bool, typer.Option()] = False,
        profile: Annotated[bool, typer.Option()] = False,
):
    options = prql.CompileOptions(
        format=True, signature_comment=True, target="sql.duckdb"
    )
    print(f'{parameter=}')
    if output == OutputFormat.JSON:
        fmt = "FORMAT JSON, ARRAY true"
    else:
        fmt = "FORMAT PARQUET, COMPRESSION zstd"
    if prql_file.name.endswith(".sql"):
        sql = prql_file.read_text()
        # Can't get it to work without doing this. So dumb.
        sql = sql.replace('$1', json.dumps(parameter))
        sql = f"{sql}; COPY temp_table TO '{output_file}' ({fmt});"
        conn = duckdb
    else:
        temp_db = Path(tempfile.mkdtemp(dir="data"))
        conn = duckdb.connect(str(temp_db / "duck.db"))

        compiled_sql = prql.compile(prql_file.read_text(), options=options)
        # sql = f"CREATE TABLE temp_table AS {compiled_sql}; COPY temp_table TO '{output_file}' ({fmt})"
        sql = compiled_sql.replace('$1', json.dumps(parameter))

        if profile:
            conn.execute("SET enable_profiling='QUERY_TREE_OPTIMIZER';")
            sql = f'EXPLAIN ANALYZE {sql}'

        limits = f"PRAGMA threads={threads}; PRAGMA memory_limit='6GB'" if not no_limits else ""
        sql = f"{limits}; {sql};"

    print(sql)

    print("\n\n\n")
    conn.execute("DROP TABLE IF EXISTS temp_table;")
    # x = duckdb.execute(sql, parameters=[parameter] if parameter else [])
    # import pprint
    # pprint.pprint(x.fetchall()
    # )
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")

    def print_thread():
        psutil.cpu_percent()
        net_old_value = psutil.net_io_counters(nowrap=True).bytes_sent + psutil.net_io_counters(nowrap=True).bytes_recv
        while True:
            time.sleep(1)
            memory = psutil.virtual_memory()
            cpu = psutil.cpu_percent()
            disk = psutil.disk_usage(os.getcwd())

            net_new_value = psutil.net_io_counters(nowrap=True).bytes_sent + psutil.net_io_counters(
                nowrap=True).bytes_recv
            net_usage = (net_new_value - net_old_value) / 1024.0 / 1024
            net_old_value = net_new_value

            print(f'\n{memory.percent=} {cpu=} {disk.percent=} {net_usage=} mb/s\n')

    t = threading.Thread(target=print_thread, daemon=True)
    t.start()

    sql = conn.sql(sql)

    if output == OutputFormat.PARQUET:
        sql.to_parquet(str(output_file), compression="zstd")
    else:
        df: pd.DataFrame = sql.to_df()
        df.set_index("name", inplace=True)
        df["stat"] = df["stat"].apply(lambda x: json.loads(x))
        df.to_json(output_file, orient="index", lines=False, indent=2)


if __name__ == "__main__":
    app()
