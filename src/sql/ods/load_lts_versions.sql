INSERT INTO ods.lts_versions (version_name)
SELECT DISTINCT object_id as version_name
FROM stg.lts_versions
ORDER BY object_id
ON CONFLICT (version_name) DO NOTHING