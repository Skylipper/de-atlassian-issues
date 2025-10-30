INSERT INTO ods.lts_versions (version_name, update_ts)
SELECT DISTINCT object_id as version_name, now() as update_ts
FROM stg.lts_versions
ORDER BY object_id
ON CONFLICT (version_name) DO NOTHING