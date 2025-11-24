SELECT CASE COUNT(*)
           WHEN 0 THEN TRUE
           ELSE FALSE
           END
FROM ods.issue_version_values oivv
LEFT JOIN dds.f_issue_version_values dfivv ON oivv.issue_id = dfivv.issue_id AND oivv.version_id = dfivv.version_id
WHERE dfivv.issue_id is NULL AND oivv.update_ts >= CURRENT_DATE;