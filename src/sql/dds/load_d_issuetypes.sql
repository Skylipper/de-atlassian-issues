WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM dds.load_settings
                                      WHERE workflow_key = 'dds.d_issuetypes'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

   , issuetypes as (SELECT DISTINCT issuetype_id, issuetype_name, update_ts
                    FROM (SELECT issuetype_id,
                                 issuetype_name,
                                 updated                                                             as update_ts,
                                 ROW_NUMBER() OVER (PARTITION BY issuetype_id ORDER BY updated DESC) as rn
                          FROM ods.issues issues
                          WHERE issues.updated >= (SELECT last_loaded_ts FROM last_updated)) a
                    WHERE rn = 1)

INSERT
INTO dds.d_issuetypes (issuetype_id, issuetype_name, update_ts)
SELECT DISTINCT issuetype_id,
                issuetype_name,
                update_ts
FROM issuetypes
ORDER BY issuetype_id
ON CONFLICT (issuetype_id) DO UPDATE
    SET issuetype_name = EXCLUDED.issuetype_name,
        update_ts      = now()
WHERE d_issuetypes.issuetype_name IS DISTINCT FROM EXCLUDED.issuetype_name



