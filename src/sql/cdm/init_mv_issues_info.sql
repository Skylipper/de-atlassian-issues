CREATE materialized view if not exists cdm.mv_issues_info AS
WITH issue_components AS (SELECT icv.issue_id, array_agg(c.component_name) as components
                          FROM dds.f_issue_component_values icv
                                   JOIN dds.d_components c ON icv.component_id = c.component_id
                          GROUP BY icv.issue_id)
   , issue_versions AS (SELECT ivv.issue_id, array_agg(version_name) as versions
                        FROM dds.f_issue_version_values ivv
                                 JOIN dds.d_versions v ON ivv.version_id = v.version_id
                        GROUP BY ivv.issue_id)
   , issue_fix_versions AS (SELECT ifvv.issue_id, array_agg(version_name) fix_versions
                            FROM dds.f_issue_fix_version_values ifvv
                                     JOIN dds.d_versions v ON ifvv.fix_version_id = v.version_id
                            GROUP BY ifvv.issue_id)
   , issue_lts AS (SELECT issue_id,
                          case when lts > 0 THEN true ELSE false END is_lts
                   FROM (SELECT i.issue_id, SUM(COALESCE(v.is_lts, false)::int) lts
                         FROM dds.f_issues i
                                  LEFT JOIN dds.f_issue_version_values ivv on i.issue_id = ivv.issue_id
                                  LEFT JOIN dds.d_versions v ON ivv.version_id = v.version_id
                         GROUP BY i.issue_id) a)

SELECT i.issue_id,
       i.issue_key,
       p.project_key,
       it.issuetype_name,
       pr.priority_name,
       s.status_name,
       res.resolution_name,
       ic.components,
       issue_lts.is_lts,
       iv.versions,
       ifv.fix_versions,
       a.user_name   as assignee_name,
       a.user_key    as assignee_key,
       rep.user_name as reporter_name,
       rep.user_key  as reporter_key,
       c.user_name   as creator_name,
       c.user_key    as creator_key,
       i.votes,
       i.created,
       i.updated,
       i.resolutiondate
FROM dds.f_issues i
         LEFT JOIN dds.d_issuetypes it on i.issuetype_id = it.issuetype_id
         LEFT JOIN dds.d_projects p on p.project_id = i.project_id
         LEFT JOIN dds.d_priorities pr on i.priority_id = pr.priority_id
         LEFT JOIN dds.d_statuses s on i.status_id = s.status_id
         LEFT JOIN dds.d_resolutions res ON i.resolution_id = res.resolution_id
         LEFT JOIN dds.d_users a ON i.assignee_id = a.user_id
         LEFT JOIN dds.d_users rep ON i.reporter_id = rep.user_id
         LEFT JOIN dds.d_users c ON i.creator_id = c.user_id
         LEFT JOIN issue_components ic ON i.issue_id = ic.issue_id
         LEFT JOIN issue_versions iv ON i.issue_id = iv.issue_id
         LEFT JOIN issue_fix_versions ifv ON i.issue_id = ifv.issue_id
         LEFT JOIN issue_lts ON i.issue_id = issue_lts.issue_id;
