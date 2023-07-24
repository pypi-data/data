SET
memory_limit = '2GB';
SET
threads = 4;


COPY
(
select count(*) as "total_files",
       approx_count_distinct(hash)::bigint as "unique_files", sum(size)::bigint                   as "total_size", sum(lines)::bigint                  as "total_lines",
from 'data/*.parquet' ) TO 'stats/general_stats.json';

COPY
(
select regexp_extract(path, '\.[0-9a-z]+$') as extension,
       count()                              as total,
       sum(lines)::bigint                           as lines, sum(size) ::bigint as size,
from 'data/*.parquet'
group by extension
order by total DESC
    limit 10
    ) TO 'stats/top_extensions.json';

COPY
(
select regexp_extract(path, '\.[0-9a-z]+$') as extension,
       count()                              as total,
       sum(size) ::bigint as size,
from 'data/*.parquet'
where skip_reason = 'binary'
group by extension
order by total DESC
    limit 10
    ) TO 'stats/top_binary_extensions.json' (ARRAY TRUE);

COPY
(
select skip_reason,
       count(*) as total,
       sum(size) ::bigint as size
from 'data/*.parquet'
where skip_reason != ''
group by skip_reason
order by total DESC
    limit 10
    ) TO 'stats/skipped_files.json' (ARRAY TRUE);


COPY
(
select date_trunc('MONTH', uploaded_on) as month,
           count(distinct project_release)::bigint                 as total_uploads,
           count(distinct project_name)::bigint                    as project_releases,
           count(distinct (project_name, project_version))::bigint as total_project_versions,
           count(*)::bigint                                        as total_files,
           sum(size)::bigint                                       as total_size,
           sum(lines)::bigint                                      as total_lines,
            total_lines / date_diff('hours', min(uploaded_on), max(uploaded_on)) as lines_per_hour,
            total_size / date_diff('hours', min(uploaded_on), max(uploaded_on)) as bytes_per_hour,
            total_files / date_diff('hours', min(uploaded_on), max(uploaded_on)) as files_per_hour,
            total_uploads / date_diff('hours', min(uploaded_on), max(uploaded_on)) as releases_per_hour,
from 'data/*.parquet'
where date_part('month', uploaded_on) < date_part('month', current_date())
group by month
order by month desc
    ) TO 'stats/over_time.json' (ARRAY TRUE);


COPY
(
select date_trunc('MONTH', uploaded_on) as month,
           count(distinct project_release)::bigint                 as total_uploads,
           count(distinct project_name)::bigint                    as project_releases,
           count(distinct (project_name, project_version))::bigint as total_project_versions,
           count(*)::bigint                                        as files,
           sum(size)::bigint                                       as size,
           sum(lines)::bigint                                      as lines,
from 'data/*.parquet'
where date_part('month', uploaded_on) < date_part('month', current_date())
group by month
order by month
    ) TO 'stats/over_time.json' (ARRAY TRUE);