CREATE TABLE temp_table AS SELECT * FROM read_parquet($1);
COPY temp_table TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);