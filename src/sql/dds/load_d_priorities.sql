WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM dds.load_settings
                                      WHERE workflow_key = 'dds.d_priorities'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)
   , priorities as (SELECT priority_id, priority_name, update_ts
                    FROM (SELECT DISTINCT priority_id,
                                          priority_name,
                                          updated                                                            as update_ts,
                                          ROW_NUMBER() OVER (PARTITION BY priority_id ORDER BY updated DESC) as rn
                          FROM ods.issues issues
                          WHERE issues.priority_id is not null
                            AND issues.updated >= (SELECT last_loaded_ts FROM last_updated)) a
                    WHERE rn = 1)

INSERT
INTO dds.d_priorities (priority_id, priority_name, update_ts)
SELECT DISTINCT priority_id,
                priority_name,
                update_ts
FROM priorities
ON CONFLICT (priority_id) DO UPDATE
    SET priority_name = EXCLUDED.priority_name,
        update_ts     = EXCLUDED.update_ts
WHERE d_priorities.priority_name IS DISTINCT FROM EXCLUDED.priority_name

