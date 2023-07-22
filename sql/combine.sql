PRAGMA memory_limit='2GB';
-- PRAGMA enable_progress_bar;
PRAGMA threads=4;
COPY (select *
    from read_parquet('input/*.parquet', union_by_name=True)) TO temp_table;

COPY temp_table TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);