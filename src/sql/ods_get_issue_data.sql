SELECT object_value::jsonb ->> 'id'                                    as issue_id,
       object_value::jsonb ->> 'key'                                   as issue_key,
       object_value::jsonb -> 'fields' -> 'project' ->> 'id'           as project_id,
       object_value::jsonb -> 'fields' -> 'project' ->> 'key'          as project_key,
       object_value::jsonb -> 'fields' -> 'issuetype' ->> 'id'         as issuetype_id,
       object_value::jsonb -> 'fields' -> 'issuetype' ->> 'name'       as issuetype_name,
       object_value::jsonb -> 'fields' -> 'priority' ->> 'id'          as priority_id,
       object_value::jsonb -> 'fields' -> 'priority' ->> 'name'        as priority_name,
       object_value::jsonb -> 'fields' -> 'resolution' ->> 'id'        as resolution_id,
       object_value::jsonb -> 'fields' -> 'resolution' ->> 'name'      as resolution_name,
       object_value::jsonb -> 'fields' -> 'status' ->> 'id'            as status_id,
       object_value::jsonb -> 'fields' -> 'status' ->> 'name'          as status_name,
       object_value::jsonb -> 'fields' -> 'votes' ->> 'votes'          as votes,
       object_value::jsonb -> 'fields' -> 'creator' ->> 'key'          as creator_key,
       object_value::jsonb -> 'fields' -> 'creator' ->> 'displayName'  as creator_name,
       object_value::jsonb -> 'fields' -> 'reporter' ->> 'key'         as reporter_key,
       object_value::jsonb -> 'fields' -> 'reporter' ->> 'displayName' as reporter_name,
       object_value::jsonb -> 'fields' -> 'assignee' ->> 'key'         as assignee_key,
       object_value::jsonb -> 'fields' -> 'assignee' ->> 'displayName' as assignee_name,
       object_value::jsonb -> 'fields' -> 'versions'                   as versions,
       object_value::jsonb -> 'fields' -> 'fixVersions'                as fixVersions,
       object_value::jsonb -> 'fields' -> 'components'                 as components

FROM stg.issues
WHERE object_id = '2104836'
LIMIT 500