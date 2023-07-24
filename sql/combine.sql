PRAGMA memory_limit='2GB';
PRAGMA threads=4;
CREATE TABLE temp_table AS SELECT * FROM read_parquet('input/*.parquet', union_by_name=True);
COPY temp_table TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);