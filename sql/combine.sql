PRAGMA memory_limit='5GB';
PRAGMA enable_progress_bar;
PRAGMA threads=4;
COPY
(
    select *
    from 'input/*.parquet'
)
TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);