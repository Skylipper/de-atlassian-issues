WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM dds.load_settings
                                      WHERE workflow_key = 'dds.d_projects'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT INTO dds.d_projects (project_id, project_key, update_ts)
SELECT DISTINCT project_id,
                project_key,
                now()
FROM ods.issues issues
WHERE issues.updated >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY project_id
ON CONFLICT (project_id) DO UPDATE
    SET project_key = EXCLUDED.project_key,
        update_ts = now()
WHERE d_projects.project_key IS DISTINCT FROM EXCLUDED.project_key



