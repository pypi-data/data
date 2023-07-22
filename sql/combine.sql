PRAGMA memory_limit='4GB';
COPY
(
    select *
    from 'input/*.parquet'
)
TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);