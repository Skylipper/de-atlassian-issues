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
ON CONFLICT (issue_id,component_id) DO UPDATE
    set project_id     = EXCLUDED.project_id,
        component_name = EXCLUDED.component_name,
        update_ts      = EXCLUDED.update_ts;