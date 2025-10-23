CREATE TABLE IF NOT EXISTS ods.issues AS
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
       (object_value::jsonb -> 'fields' -> 'assignee' ->> 'displayName')::varchar(128) as assignee_name
FROM stg.issues
LIMIT 1
