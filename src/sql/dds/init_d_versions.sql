create table dds.d_versions
(
    version_id   integer      not null primary key,
    project_id   integer      not null,
    version_name varchar(128) not null,
    is_lts       bool         not null,
    update_ts    timestamp    not null
);

alter table dds.d_versions
    add constraint versions_project_id_fk
        foreign key (project_id) references dds.d_projects (project_id);

