SELECT hash, repository, uploaded_on
FROM (SELECT hash,
             repository,
             uploaded_on,
             ROW_NUMBER() OVER (PARTITION BY encode(hash, 'hex') order by uploaded_on) as n
      FROM input_dataset
      where skip_reason = ''
        and archive_path ILIKE '%.py' and size > 0) as ordered;
