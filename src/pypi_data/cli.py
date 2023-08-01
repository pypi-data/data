import enum
import os
import sys
import tempfile
import threading
import time
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

from rich.syntax import Syntax
from rich.console import Console

app = typer.Typer()
session = requests.Session()
MB = 1024 * 1024
GB = MB * 1024

console = Console()

GithubToken = Annotated[str, typer.Option(envvar="GITHUB_TOKEN")]


def _get_urls(github: Github) -> Iterable[tuple[str, str]]:
    for repo in github.get_organization("pypi-data").get_repos():
        if repo.name.startswith("pypi-mirror-"):
            yield f'{repo.html_url}.git', f"{repo.html_url}/releases/download/latest/dataset.parquet"


def github_client(github_token) -> Github:
    auth = Auth.Token(github_token)
    g = Github(auth=auth)
    g.per_page = 100
    return g


@app.command()
def print_git_urls(github_token: GithubToken):
    g = github_client(github_token)
    for url, _ in _get_urls(g):
        print(url)


def group_by_size(github: Github, target_size: int) -> Iterable[list[tuple[int, str]]]:
    fs = HTTPFileSystem()
    urls = (u[1] for u in _get_urls(github))
    with ThreadPoolExecutor() as pool:
        stat_results = pool.map(lambda url: fs.stat(url), urls)

        names = []
        total_size = 0
        for stat_result in stat_results:
            name = stat_result["name"]
            index = int(name.removeprefix('https://github.com/pypi-data/pypi-mirror-').split('/')[0])
            names.append({
                "name": stat_result["name"],
                "id": index
            })
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
    TABLE = "table"


def _compile_sql(prql_file: Path) -> str:
    options = prql.CompileOptions(
        format=True, signature_comment=True, target="sql.duckdb"
    )

    extra_module = (prql_file.parent / f'_{prql_file.name}')
    if extra_module.exists():
        prefix = extra_module.read_text()
    else:
        prefix = ""

    prql_value = prql_file.read_text()
    compiled_sql = prql.compile(f'{prefix}\n{prql_value}', options=options)
    return compiled_sql


@app.command()
def compile_sql(
        prql_file: Annotated[Path, typer.Argument(dir_okay=False, file_okay=True, readable=True)],
):
    result = _compile_sql(prql_file)
    syntax = Syntax(result, "sql")
    console.print(syntax)


@app.command()
def run_sql(
        prql_file: Annotated[Path, typer.Argument(dir_okay=False, file_okay=True, readable=True)],
        output_file: Annotated[Path, typer.Argument(dir_okay=False, file_okay=True, writable=True)],
        parameter: Annotated[Optional[List[str]], typer.Argument()] = None,
        output: Annotated[OutputFormat, typer.Option()] = OutputFormat.PARQUET,
        threads: Annotated[int, typer.Option()] = 2,
        memory: Annotated[int, typer.Option()] = 6,
        no_limits: Annotated[bool, typer.Option()] = False,
        profile: Annotated[bool, typer.Option()] = False,
        db: Annotated[Optional[str], typer.Option()] = None,
        per_thread_output: Annotated[bool, typer.Option()] = False,
):
    """
    This whole method is a fucking mess.
    """

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
        data_dir = (Path.cwd() / "data")
        data_dir.mkdir(exist_ok=True)
        if not db:
            db = Path(tempfile.mkdtemp(dir="data"))
        else:
            db = data_dir / db
            db.mkdir(exist_ok=True)
        conn = duckdb.connect(str(db / "duck.db"))
        compiled_sql = _compile_sql(prql_file)
        # sql = f"CREATE TABLE temp_table AS {compiled_sql}; COPY temp_table TO '{output_file}' ({fmt})"
        sql = compiled_sql.replace('$1', json.dumps(parameter))

        if profile:
            conn.execute("PRAGMA enable_profiling='query_tree_optimizer';")
            conn.execute(f"PRAGMA profile_output='{db}/profile.json';")
            conn.execute(f"PRAGMA profiling_output='{db}/profile.json';")
            print(f"PRAGMA profile_output='{db}/profile.json';")
            # sql = f'EXPLAIN ANALYZE ({sql})'

        if not no_limits:
            limits = f"PRAGMA threads={threads}; PRAGMA memory_limit='{memory}GB';"
            conn.executemany(limits)

    print(sql)
    print("\n\n\n")
    sys.stdout.flush()

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

    sql_obj = conn.sql(sql)

    if output == OutputFormat.TABLE:
        try:
            conn.table("temp_table")
        except duckdb.CatalogException:
            sql_obj.to_table("temp_table")
        else:
            sql_obj.insert_into("temp_table")
    elif output == OutputFormat.PARQUET:
        if per_thread_output:
            output_sql = f'COPY ({sql}) TO \'{output_file}\' (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE)'
            print(f'\n\nper_thread_output {output_sql}\n\n\n')
            conn.execute(output_sql)
        else:
            sql_obj.to_parquet(str(output_file), compression="zstd")
    else:
        sql_obj.to_table("temp_table")
        conn.execute(f'COPY temp_table TO \'{output_file}\' (FORMAT JSON, array TRUE)')


@app.command()
def sort_json_stats(
        json_file: Annotated[Path, typer.Argument(dir_okay=False, file_okay=True, readable=True, writable=True)]
):
    json_contents = json.loads(json_file.read_text())
    for element in json_contents:
        if element["name"] == "stats_over_time":
            element["stat"] = sorted(element["stat"], key=lambda x: x["month"])

    json_file.write_text(json.dumps(json_contents, indent=2))


if __name__ == "__main__":
    app()
