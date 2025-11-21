DELETE
FROM issues_info
WHERE issue_id in (SELECT (issue_id)
                   FROM issues_info_temp)