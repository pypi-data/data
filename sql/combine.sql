PRAGMA memory_limit='2GB';
PRAGMA threads=4;
COPY temp_table FROM 'input/*.parquet' (FORMAT PARQUET);
COPY temp_table TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);