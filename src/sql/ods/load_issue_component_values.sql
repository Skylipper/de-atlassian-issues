-- В одной задаче может быть от 0 до нескольких компонентов, они могут быть как добавлены, так и удалены.
-- Поэтому для обновленных задач удаляем записи и вставляем свежую версию
WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'ods.issue_component_values'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)
DELETE
FROM ods.issue_component_values
WHERE issue_id in
      (SELECT object_id::int as issue_id
       FROM stg.issues
       WHERE update_ts >= (SELECT last_loaded_ts FROM last_updated));


WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'ods.issue_component_values'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT
INTO ods.issue_component_values (issue_id, project_id, component_id, component_name, update_ts)
SELECT object_id::int                                               as issue_id,
       (object_value::jsonb -> 'fields' -> 'project' ->> 'id')::int as project_id,
       (jsonb_array_elements_text(object_value::jsonb -> 'fields' -> 'components')::jsonb ->>
        'id')::int                                                  as component_id,
       (jsonb_array_elements_text(object_value::jsonb -> 'fields' -> 'components')::jsonb ->>
        'name')::varchar(255)                                       as component_name,
       update_ts                                                    as update_ts
FROM stg.issues
WHERE update_ts >= (SELECT last_loaded_ts FROM last_updated)