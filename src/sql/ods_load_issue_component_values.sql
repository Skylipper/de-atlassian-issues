WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'ods.components'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT
INTO ods.issue_component_values (issue_id, project_id, component_id, component_name)
SELECT object_id::int                                                                                                as issue_id,
       (object_value::jsonb -> 'fields' -> 'project' ->> 'id')::int                                         as project_id,
       (jsonb_array_elements_text(object_value::jsonb -> 'fields' -> 'components')::jsonb ->>
       'id')::int                                                                                          as component_id,
       (jsonb_array_elements_text(object_value::jsonb -> 'fields' -> 'components')::jsonb ->>
       'name')::varchar(255)                                                                                       as component_name
FROM stg.issues
WHERE update_ts >= (SELECT last_loaded_ts FROM last_updated);