WITH last_updated as (SELECT COALESCE(
                                     (SELECT (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                                      FROM ods.load_settings
                                      WHERE workflow_key = 'ods.issues'),
                                     '2010-01-01'::timestamp) as last_loaded_ts)

INSERT
INTO ods.issues (issue_id, issue_key, project_id, project_key, issuetype_id, issuetype_name, priority_id, priority_name,
                 resolution_id, resolution_name, status_id, status_name, votes, creator_key, creator_name, reporter_key,
                 reporter_name, assignee_key, assignee_name, created, updated,resolutiondate)
SELECT (object_value::jsonb ->> 'id')::int                                             as issue_id,
       (object_value::jsonb ->> 'key')::varchar(32)                                    as issue_key,
       (object_value::jsonb -> 'fields' -> 'project' ->> 'id')::int                    as project_id,
       (object_value::jsonb -> 'fields' -> 'project' ->> 'key')::varchar(32)           as project_key,
       (object_value::jsonb -> 'fields' -> 'issuetype' ->> 'id')::int                  as issuetype_id,
       (object_value::jsonb -> 'fields' -> 'issuetype' ->> 'name')::varchar(32)        as issuetype_name,
       (object_value::jsonb -> 'fields' -> 'priority' ->> 'id')::int                   as priority_id,
       (object_value::jsonb -> 'fields' -> 'priority' ->> 'name')::varchar(32)         as priority_name,
       (object_value::jsonb -> 'fields' -> 'resolution' ->> 'id')::int                 as resolution_id,
       (object_value::jsonb -> 'fields' -> 'resolution' ->> 'name')::varchar(32)       as resolution_name,
       (object_value::jsonb -> 'fields' -> 'status' ->> 'id')::int                     as status_id,
       (object_value::jsonb -> 'fields' -> 'status' ->> 'name')::varchar(32)           as status_name,
       (object_value::jsonb -> 'fields' -> 'votes' ->> 'votes')::int                   as votes,
       (object_value::jsonb -> 'fields' -> 'creator' ->> 'key')::varchar(32)           as creator_key,
       (object_value::jsonb -> 'fields' -> 'creator' ->> 'displayName')::varchar(128)  as creator_name,
       (object_value::jsonb -> 'fields' -> 'reporter' ->> 'key')::varchar(32)          as reporter_key,
       (object_value::jsonb -> 'fields' -> 'reporter' ->> 'displayName')::varchar(128) as reporter_name,
       (object_value::jsonb -> 'fields' -> 'assignee' ->> 'key')::varchar(32)          as assignee_key,
       (object_value::jsonb -> 'fields' -> 'assignee' ->> 'displayName')::varchar(128) as assignee_name,
       (object_value::jsonb -> 'fields' ->> 'created')::timestamp        as created,
       (object_value::jsonb -> 'fields' ->> 'updated')::timestamp        as updated,
       (object_value::jsonb -> 'fields' ->> 'resolutiondate')::timestamp as resolutiondate
FROM stg.issues
WHERE update_ts >= (SELECT last_loaded_ts FROM last_updated)
ORDER BY update_ts
LIMIT 10000
ON CONFLICT (issue_id) DO UPDATE
    set issue_key      = EXCLUDED.issue_key,
        project_id     = EXCLUDED.project_id,
        project_key    = EXCLUDED.project_key,
        issuetype_id   = EXCLUDED.issuetype_id,
        issuetype_name = EXCLUDED.issuetype_name,
        priority_id    = EXCLUDED.priority_id,
        priority_name  = EXCLUDED.priority_name,
        resolution_id  = EXCLUDED.resolution_id,
        resolution_name= EXCLUDED.resolution_name,
        status_id      = EXCLUDED.status_id,
        status_name    = EXCLUDED.status_name,
        votes          = EXCLUDED.votes,
        creator_key    = EXCLUDED.creator_key,
        creator_name   = EXCLUDED.creator_name,
        reporter_key   = EXCLUDED.reporter_key,
        reporter_name  = EXCLUDED.reporter_name,
        assignee_key   = EXCLUDED.assignee_key,
        assignee_name  = EXCLUDED.assignee_name
