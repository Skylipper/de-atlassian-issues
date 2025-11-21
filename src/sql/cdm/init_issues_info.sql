DROP table if exists atlassian.issues_info;

create table if not exists atlassian.issues_info
(
    issue_id        int,
    issue_key       varchar(20),
    project_key     varchar(10),
    issuetype_name  varchar(32),
    priority_name   varchar(32),
    status_name     varchar(32),
    resolution_name varchar(32),
     components      Array(String),
    is_lts          boolean,
    versions        Array(String),
    fix_versions    Array(String),
    assignee_name   varchar(32),
    assignee_key    varchar(32),
    reporter_name   varchar(32),
    reporter_key    varchar(32),
    creator_name    varchar(32),
    creator_key     varchar(32),
    votes           integer,
    created         DATETIME('Etc/UTC'),
    updated         DATETIME('Etc/UTC'),
    resolutiondate  DATETIME('Etc/UTC'),
    days_to_close   float,
    created_year    integer,
    created_month   integer,
    created_week    integer,
    created_day     integer,
    resolved_year   integer,
    resolved_month  integer,
    resolved_week   integer,
    resolved_day    integer,
    etl_time        timestamp
)
    engine = MergeTree
        PARTITION BY created_year
        ORDER BY issue_id
        SETTINGS index_granularity = 8192;

