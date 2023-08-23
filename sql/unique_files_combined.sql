SELECT hash, repository, uploaded_on
FROM (SELECT hash,
             repository,
             uploaded_on,
             ROW_NUMBER() OVER (PARTITION BY encode(hash, 'hex') order by uploaded_on) as n
      FROM input_dataset
      ) as ordered
WHERE n = 1