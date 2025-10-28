WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'dds.d_components'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)
   , components as (SELECT DISTINCT icv.component_id
                                  , icv.project_id
                                  , icv.component_name
                    FROM ods.issue_component_values icv
                    WHERE icv.update_ts >= (SELECT last_loaded_ts FROM last_updated)
                    ORDER BY component_id)


INSERT
INTO dds.d_components (component_id, project_id, component_name, update_ts)
SELECT component_id, project_id, component_name,  now()
FROM components
ON CONFLICT (component_id) DO UPDATE
    SET component_name = EXCLUDED.component_name,
        update_ts    = now()
WHERE dds.d_components.component_name IS DISTINCT FROM EXCLUDED.component_name OR
      dds.d_components.project_id IS DISTINCT FROM EXCLUDED.project_id;
