WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM dds.load_settings
                                      WHERE workflow_key = 'dds.d_resolutions'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT INTO dds.d_resolutions (resolution_id, resolution_name, update_ts)
SELECT DISTINCT resolution_id,
                resolution_name,
                now()
FROM ods.issues issues
WHERE issues.resolution_id is not null
  AND issues.updated >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY resolution_id
ON CONFLICT (resolution_id) DO UPDATE
    SET resolution_name = EXCLUDED.resolution_name,
        update_ts = now()
WHERE d_resolutions.resolution_name IS DISTINCT FROM EXCLUDED.resolution_name;