WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM dds.load_settings
                                      WHERE workflow_key = 'dds.f_issues'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT
INTO dds.f_issues (issue_id, issue_key, project_id, issuetype_id, priority_id, resolution_id, status_id, votes,
                   creator_id, reporter_id, assignee_id, created, updated, resolutiondate)
SELECT issue_id,
       issue_key,
       project_id,
       issuetype_id,
       priority_id,
       resolution_id,
       status_id,
       votes,
       c.user_id as creator_id,
       r.user_id as reporter_id,
       a.user_id as assignee_id,
       created,
       updated,
       resolutiondate
FROM ods.issues i
         LEFT JOIN dds.d_users c ON i.creator_key = c.user_key
         LEFT JOIN dds.d_users r ON i.reporter_key = r.user_key
         LEFT JOIN dds.d_users a ON i.assignee_key = a.user_key
WHERE i.updated >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY i.issue_id
ON CONFLICT (issue_id) DO UPDATE
    SET issue_key     = EXCLUDED.issue_key,
        project_id= EXCLUDED.project_id,
        issuetype_id= EXCLUDED.issuetype_id,
        priority_id= EXCLUDED.priority_id,
        resolution_id= EXCLUDED.resolution_id,
        status_id= EXCLUDED.status_id,
        votes= EXCLUDED.votes,
        creator_id    = EXCLUDED.creator_id,
        reporter_id   = EXCLUDED.reporter_id,
        assignee_id   = EXCLUDED.assignee_id,
        created= EXCLUDED.created,
        updated= EXCLUDED.updated,
        resolutiondate= EXCLUDED.resolutiondate


