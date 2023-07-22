PRAGMA memory_limit='7GB';
COPY
(
    select *
    from 'input/*.parquet'
)
TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);