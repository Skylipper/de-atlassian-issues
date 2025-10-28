WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'dds.d_users'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT
INTO dds.d_users (user_key, user_name, update_ts)
SELECT DISTINCT user_key, user_name, now()
FROM (SELECT DISTINCT i.creator_key  as user_key
                    , i.creator_name as user_name
                    , i.updated      as updated
      FROM ods.issues i
      WHERE i.creator_key is not null

      UNION

      SELECT DISTINCT i.reporter_key  as user_key
                    , i.reporter_name as user_name
                    , i.updated       as updated
      FROM ods.issues i
      WHERE i.reporter_key is not null

      UNION

      SELECT DISTINCT i.assignee_key  as user_key
                    , i.assignee_name as user_name
                    , i.updated       as updated
      FROM ods.issues i
      WHERE i.assignee_key is not null) a
WHERE a.updated >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY a.user_key
ON CONFLICT (user_key) DO UPDATE
    SET user_name = EXCLUDED.user_name,
        update_ts = now()
WHERE d_users.user_name IS DISTINCT FROM EXCLUDED.user_name;
