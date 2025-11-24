SELECT CASE COUNT(*)
           WHEN 0 THEN TRUE
           ELSE FALSE
           END
FROM ods.issues oi
         LEFT JOIN dds.f_issues di ON oi.issue_id = di.issue_id
WHERE di.issue_id is null;