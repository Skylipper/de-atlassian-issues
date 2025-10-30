WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'dds.d_versions'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)
   , lts_major_versions
    as (select CONCAT(major_version, '.') major_version_template
        from unnest(array ['10.3', '9.12','9.4','8.20','8.13','8.5','7.13','7.6','6.4','6.3','6.2','6.1','6.0']) x(major_version))

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

