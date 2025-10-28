CREATE table if not exists dds.f_issue_component_values
(
    id           serial primary key,
    issue_id     int,
    component_id int,
    update_ts    timestamp
);

alter table dds.f_issue_component_values
    add constraint f_issue_component_values_issue_id_fk
        foreign key (issue_id) references dds.f_issues (issue_id);

alter table dds.f_issue_component_values
    add constraint f_issue_component_values_component_id_fk
        foreign key (component_id) references dds.d_components (component_id);

ALTER table dds.f_issue_component_values
    ADD CONSTRAINT all_unique_constraint UNIQUE (issue_id, component_id);

