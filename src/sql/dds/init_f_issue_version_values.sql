CREATE table if not exists dds.f_issue_version_values
(
    id           serial primary key,
    issue_id     int,
    version_id int,
    update_ts    timestamp
);

alter table dds.f_issue_version_values
    add constraint f_issue_version_values_issue_id_fk
        foreign key (issue_id) references dds.f_issues (issue_id);

alter table dds.f_issue_version_values
    add constraint f_issue_version_values_version_id_fk
        foreign key (version_id) references dds.d_versions (version_id);

ALTER table dds.f_issue_version_values
    ADD CONSTRAINT all_unique_version_constraint UNIQUE (issue_id, version_id);

