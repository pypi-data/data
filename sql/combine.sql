PRAGMA memory_limit='2GB';
PRAGMA threads=4;
SET enable_progress_bar=true;
CREATE TABLE temp_table AS
select regexp_extract(filename, '(\d+)\.parquet', 1)::USMALLINT as repository, * exclude (filename)
FROM read_parquet('input/*.parquet', union_by_name = True, filename = true)
order by (uploaded_on, path);
COPY temp_table TO 'output.parquet' (FORMAT PARQUET, COMPRESSION zstd);
