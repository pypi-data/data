PRAGMA memory_limit='5GB';
COPY
(
    select *
    from 'input/*.parquet'
)
TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);