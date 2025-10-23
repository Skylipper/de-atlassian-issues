CREATE TABLE IF NOT EXISTS ods.issues
(
    issue_id        integer unique     not null primary key,
    issue_key       varchar(32) unique not null,
    project_id      integer            not null,
    project_key     varchar(32)        not null,
    issuetype_id    integer            not null,
    issuetype_name  varchar(32)        not null,
    priority_id     integer,
    priority_name   varchar(32),
    resolution_id   integer,
    resolution_name varchar(32),
    status_id       integer,
    status_name     varchar(32),
    votes           integer,
    creator_key     varchar(32),
    creator_name    varchar(128),
    reporter_key    varchar(32),
    reporter_name   varchar(128),
    assignee_key    varchar(32),
    assignee_name   varchar(128)
);


