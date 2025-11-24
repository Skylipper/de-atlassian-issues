SELECT CASE COUNT(*)
           WHEN 0 THEN TRUE
           ELSE FALSE
           END
FROM stg.issues si
         LEFT JOIN ods.issues oi ON si.object_id = CAST(oi.issue_id as varchar)
WHERE oi.issue_id is null;