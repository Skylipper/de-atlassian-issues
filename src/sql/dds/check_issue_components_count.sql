SELECT CASE COUNT(*)
           WHEN 0 THEN TRUE
           ELSE FALSE
           END
FROM ods.issue_component_values oicv
LEFT JOIN dds.f_issue_component_values dficv ON oicv.issue_id = dficv.issue_id AND oicv.component_id = dficv.component_id
WHERE dficv.issue_id is NULL AND oicv.update_ts >= CURRENT_DATE;