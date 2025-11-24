SELECT COUNT(DISTINCT issue_id)
FROM ods.issue_component_values
WHERE update_ts >= CURRENT_DATE