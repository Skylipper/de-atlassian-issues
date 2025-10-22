create table if not exists ods.issue_component_values
(
    id             serial primary key not null,
    issue_id       int         not null,
    project_id     int,
    component_id   int,
    component_name varchar(256)
);