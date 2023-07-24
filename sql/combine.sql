PRAGMA memory_limit='6GB';
PRAGMA threads=2;
CREATE TABLE temp_table AS SELECT * FROM read_parquet('input/*.parquet', union_by_name=True);
COPY temp_table TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);