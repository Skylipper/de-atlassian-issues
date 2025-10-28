create table dds.d_components
(
    component_id   integer      not null primary key,
    project_id   integer      not null,
    component_name varchar(128) not null,
    update_ts    timestamp    not null
);

alter table dds.d_components
    add constraint components_project_id_fk
        foreign key (project_id) references dds.projects (project_id);

