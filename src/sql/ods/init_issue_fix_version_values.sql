create table if not exists ods.issue_fix_version_values
(
    id           serial primary key not null,
    issue_id     int                not null,
    project_id   int,
    version_id   int,
    version_name varchar(256),
    update_ts    timestamp
);

alter table ods.issue_fix_version_values
    add constraint issue_fixversion_id_unique
        unique (issue_id, version_id);