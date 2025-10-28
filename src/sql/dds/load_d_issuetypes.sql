WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'dds.d_issuetypes'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT INTO dds.d_issuetypes (issuetype_id, issuetype_name, update_ts)
SELECT DISTINCT issuetype_id,
                issuetype_name,
                now()
FROM ods.issues issues
WHERE issues.updated >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY issuetype_id
ON CONFLICT (issuetype_id) DO UPDATE
    SET issuetype_name = EXCLUDED.issuetype_name,
        update_ts = now()
WHERE d_issuetypes.issuetype_name IS DISTINCT FROM EXCLUDED.issuetype_name



