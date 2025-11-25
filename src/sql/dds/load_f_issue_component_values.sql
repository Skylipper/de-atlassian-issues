-- В одной задаче может быть от 0 до нескольких компонентов, они могут быть как добавлены, так и удалены.
-- Поэтому для обновленных задач удаляем записи и вставляем свежую версию
WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM dds.load_settings
                                      WHERE workflow_key = 'dds.f_issue_component_values'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

DELETE
FROM dds.f_issue_component_values
WHERE issue_id in
      (SELECT issue_id
       FROM ods.issue_component_values
       WHERE update_ts >= (SELECT last_loaded_ts FROM last_updated));


WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM dds.load_settings
                                      WHERE workflow_key = 'dds.f_issue_component_values'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT INTO dds.f_issue_component_values (issue_id, component_id, update_ts)
SELECT DISTINCT issue_id,
                component_id,
                now()
FROM ods.issue_component_values
WHERE update_ts >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY issue_id
ON CONFLICT (issue_id,component_id) DO NOTHING;