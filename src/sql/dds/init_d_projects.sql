create table if not exists dds.d_projects
(
    project_id  integer      not null primary key,
    project_key varchar(128) not null,
    update_ts   timestamp    not null
);