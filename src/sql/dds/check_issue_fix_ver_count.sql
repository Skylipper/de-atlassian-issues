SELECT CASE COUNT(*)
           WHEN 0 THEN TRUE
           ELSE FALSE
           END
FROM ods.issue_fix_version_values oifvv
LEFT JOIN dds.f_issue_fix_version_values dfifvv ON oifvv.issue_id = dfifvv.issue_id AND oifvv.version_id = dfifvv.fix_version_id
WHERE dfifvv.issue_id is NULL AND oifvv.update_ts >= CURRENT_DATE;