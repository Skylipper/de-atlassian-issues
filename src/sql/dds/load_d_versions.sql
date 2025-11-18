WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM dds.load_settings
                                      WHERE workflow_key = 'dds.d_versions'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)
   , affect_versions as (SELECT DISTINCT version_id
                                       , project_id
                                       , iv.version_name
                                       , lv.version_name IS NOT NULL is_lts
                         FROM ods.issue_version_values iv
                                  LEFT JOIN ods.lts_versions lv ON iv.version_name = lv.version_name
                         WHERE iv.update_ts >= (SELECT last_loaded_ts FROM last_updated)
                         ORDER BY version_id)

   , fix_versions as (SELECT DISTINCT version_id
                                    , project_id
                                    , fiv.version_name
                                    , lv.version_name IS NOT NULL is_lts
                      FROM ods.issue_fix_version_values fiv
                               LEFT JOIN ods.lts_versions lv ON fiv.version_name = lv.version_name
                      WHERE fiv.update_ts >= (SELECT last_loaded_ts FROM last_updated)
                      ORDER BY version_id)
   , all_versions as (SELECT version_id, project_id, version_name, is_lts
                      FROM (SELECT version_id, project_id, version_name, is_lts
                            FROM affect_versions
                            UNION
                            SELECT version_id, project_id, version_name, is_lts
                            FROM fix_versions) a
                      ORDER BY version_id)


INSERT
INTO dds.d_versions (version_id, project_id, version_name, is_lts, update_ts)
SELECT version_id, project_id, version_name, is_lts, now()
FROM all_versions
ON CONFLICT (version_id) DO UPDATE
    SET version_name = EXCLUDED.version_name,
        is_lts       = EXCLUDED.is_lts,
        update_ts    = now()
WHERE dds.d_versions.version_name IS DISTINCT FROM EXCLUDED.version_name
   OR dds.d_versions.project_id IS DISTINCT FROM EXCLUDED.project_id
   OR dds.d_versions.is_lts IS DISTINCT FROM EXCLUDED.is_lts;

