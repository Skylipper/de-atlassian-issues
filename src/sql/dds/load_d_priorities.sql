WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'dds.d_priorities'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT INTO dds.d_priorities (priority_id, priority_name, update_ts)
SELECT DISTINCT priority_id,
                priority_name,
                now()
FROM ods.issues issues
WHERE issues.priority_id is not null
  AND issues.updated >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY priority_id
ON CONFLICT (priority_id) DO UPDATE
    SET priority_name = EXCLUDED.priority_name,
        update_ts = now()
WHERE d_priorities.priority_name IS DISTINCT FROM EXCLUDED.priority_name

