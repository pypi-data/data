SET memory_limit='6GB';
SET threads=2;
COPY
(
select hash, any_value(path)
from read_parquet('data/*.parquet')
where path LIKE '%.py' and skip_reason = ''
group by 1
) TO 'unique-python-files.parquet' (FORMAT PARQUET, compression zstd);
