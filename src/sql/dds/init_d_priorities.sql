create table if not exists dds.d_priorities
(
    priority_id   integer     not null primary key,
    priority_name varchar(32) not null,
    update_ts     timestamp   not null
);