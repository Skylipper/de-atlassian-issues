create table stg.lts_versions
(
    id           serial primary key,
    object_id    varchar(255) not null
        constraint lts_versions_object_id_unique
            unique,
    object_value text         not null,
    update_ts    timestamp    not null
);