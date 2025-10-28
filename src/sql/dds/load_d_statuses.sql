WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'dds.d_statuses'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT
INTO dds.d_statuses (status_id, status_name, update_ts)
SELECT DISTINCT status_id,
                status_name,
                now()
FROM ods.issues issues
WHERE issues.status_id is not null
  AND issues.updated >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY status_id
ON CONFLICT (status_id) DO UPDATE
    SET status_name = EXCLUDED.status_name,
        update_ts       = now()
WHERE d_statuses.status_name IS DISTINCT FROM EXCLUDED.status_name;