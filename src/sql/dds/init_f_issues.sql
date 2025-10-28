create table if not exists dds.f_issues
(
    issue_id       int primary key,
    issue_key      varchar(32) unique,
    project_id     int,
    issuetype_id   int,
    priority_id    int,
    resolution_id  int,
    status_id      int,
    votes          int,
    creator_id     int,
    reporter_id    int,
    assignee_id    int,
    created        timestamp,
    updated        timestamp,
    resolutiondate timestamp
);

alter table dds.f_issues
    add constraint f_issues_project_id_fk
        foreign key (project_id) references dds.d_projects (project_id);

alter table dds.f_issues
    add constraint f_issues_issuetype_id_fk
        foreign key (issuetype_id) references dds.d_issuetypes (issuetype_id);

alter table dds.f_issues
    add constraint f_issues_priority_id_fk
        foreign key (priority_id) references dds.d_priorities (priority_id);

alter table dds.f_issues
    add constraint f_issues_resolution_id_fk
        foreign key (resolution_id) references dds.d_resolutions (resolution_id);

alter table dds.f_issues
    add constraint f_issues_status_id_fk
        foreign key (status_id) references dds.d_statuses (status_id);

alter table dds.f_issues
    add constraint f_issues_creator_id_fk
        foreign key (creator_id) references dds.d_users (user_id);

alter table dds.f_issues
    add constraint f_issues_reporter_id_fk
        foreign key (reporter_id) references dds.d_users (user_id);

alter table dds.f_issues
    add constraint f_issues_assignee_id_fk
        foreign key (assignee_id) references dds.d_users (user_id);