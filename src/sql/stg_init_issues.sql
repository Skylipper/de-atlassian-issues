CREATE schema if not exists stg;

DROP table if exists stg.issues;

CREATE table if not exists stg.issues
(
    id           serial primary key not null,
    object_id    varchar(255)       not null unique,
    object_value text               not null,
    update_ts    timestamp          not null
);

alter table stg.issues
    add constraint issues_object_id_unique
        unique (object_id);