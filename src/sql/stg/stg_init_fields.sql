CREATE schema if not exists stg;

-- DROP table if exists stg.fields;

CREATE table if not exists stg.fields
(
    id           serial primary key not null,
    object_id    varchar(255)       not null,
    object_value text               not null,
    update_ts    timestamp          not null
);

alter table stg.fields
    add constraint fields_object_id_unique
        unique (object_id);