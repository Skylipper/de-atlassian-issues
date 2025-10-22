create table if not exists ods.issue_component_values
(
    id             serial primary key not null,
    issue_id       numeric(8)         not null,
    project_id     numeric(8),
    component_id   numeric(8),
    component_name varchar(256)
);