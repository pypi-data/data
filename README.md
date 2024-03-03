# pypi-code datasets

This repository contains automatically updated data about code stored within PyPI. See the [website for instructions on how to use the datasets](https://py-code.org/datasets) inside this repository. 

## Quickstart - querying the contents of PyPI with DuckDB

Download all the files within the dataset:

```
curl -L --remote-name-all $(curl -L "https://github.com/pypi-data/data/raw/main/links/dataset.txt")
```

Follow the installation instructions for [DuckDB](https://duckdb.org/#quickinstall), then query the data!

The example below is finding the largest releases uploaded within the last 7 days by number of files 
contained within:

```shell
$ duckdb
v0.10.0 20b1486d11
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
D select project_name,
    project_release,
    count(*) as total_files,
    count(distinct hash) as unique_files,
    max(uploaded_on) as last_uploaded
  from 'index-*.parquet'
  where uploaded_on > (today() - INTERVAL 7 days)
  group by 1, 2 order by 3 desc limit 5;
┌───────────────┬─────────────────────────────────┬─────────────┬──────────────┬─────────────────────────┐
│ project_name  │         project_release         │ total_files │ unique_files │      last_uploaded      │
│    varchar    │             varchar             │    int64    │    int64     │        timestamp        │
├───────────────┼─────────────────────────────────┼─────────────┼──────────────┼─────────────────────────┤
│ ansible       │ ansible-9.3.0.tar.gz            │       40326 │        32368 │ 2024-02-27 18:17:53.53  │
│ homeassistant │ homeassistant-2024.3.0b3.tar.gz │       36353 │        35066 │ 2024-03-01 04:39:31.661 │
│ homeassistant │ homeassistant-2024.3.0b2.tar.gz │       36350 │        35063 │ 2024-02-29 20:31:07.727 │
│ homeassistant │ homeassistant-2024.3.0b1.tar.gz │       36344 │        35056 │ 2024-02-29 06:24:58.482 │
│ homeassistant │ homeassistant-2024.3.0b0.tar.gz │       36343 │        35055 │ 2024-02-28 21:53:51.729 │
└───────────────┴─────────────────────────────────┴─────────────┴──────────────┴─────────────────────────┘
D describe select * from 'index-*.parquet';
┌─────────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
│   column_name   │ column_type │  null   │   key   │ default │  extra  │
│     varchar     │   varchar   │ varchar │ varchar │ varchar │ varchar │
├─────────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
│ project_name    │ VARCHAR     │ YES     │         │         │         │
│ project_version │ VARCHAR     │ YES     │         │         │         │
│ project_release │ VARCHAR     │ YES     │         │         │         │
│ uploaded_on     │ TIMESTAMP   │ YES     │         │         │         │
│ path            │ VARCHAR     │ YES     │         │         │         │
│ archive_path    │ VARCHAR     │ YES     │         │         │         │
│ size            │ UBIGINT     │ YES     │         │         │         │
│ hash            │ BLOB        │ YES     │         │         │         │
│ skip_reason     │ VARCHAR     │ YES     │         │         │         │
│ lines           │ UBIGINT     │ YES     │         │         │         │
│ repository      │ UINTEGER    │ YES     │         │         │         │
├─────────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┤
│ 11 rows                                                     6 columns │
└───────────────────────────────────────────────────────────────────────┘
```
