create table ods.lts_versions
(
    id           serial primary key,
    version_name varchar(255) not null
        constraint lts_versions_version_name_unique
            unique
);