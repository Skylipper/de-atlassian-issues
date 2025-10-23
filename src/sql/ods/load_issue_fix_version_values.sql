-- В одной задаче может быть от 0 до нескольких версий, они могут быть как добавлены, так и удалены.
-- Поэтому для обновленных задач удаляем записи и вставляем свежую версию
WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'ods.issue_fix_version_values'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)
DELETE
FROM ods.issue_fix_version_values
WHERE issue_id in
      (SELECT object_id::int as issue_id
       FROM stg.issues
       WHERE update_ts >= (SELECT last_loaded_ts FROM last_updated));


WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'ods.issue_fix_version_values'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT
INTO ods.issue_fix_version_values (issue_id, project_id, version_id, version_name, update_ts)
SELECT object_id::int                                               as issue_id,
       (object_value::jsonb -> 'fields' -> 'project' ->> 'id')::int as project_id,
       (jsonb_array_elements_text(object_value::jsonb -> 'fields' -> 'fixVersions')::jsonb ->>
        'id')::int                                                  as version_id,
       (jsonb_array_elements_text(object_value::jsonb -> 'fields' -> 'fixVersions')::jsonb ->>
        'name')::varchar(255)                                       as version_name,
       update_ts                                                    as update_ts
FROM stg.issues
WHERE update_ts >= (SELECT last_loaded_ts FROM last_updated)
ON CONFLICT (issue_id,version_id) DO UPDATE
    set version_name = EXCLUDED.version_name,
        update_ts    = EXCLUDED.update_ts