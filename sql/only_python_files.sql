SELECT archive_path, hash, lines, project_name, project_version, size, uploaded_on
FROM input_dataset
where skip_reason = ''
  and archive_path ILIKE '%.py' and size != 0
order by project_name, project_version, archive_path;