create table if not exists dds.d_statuses
(
    status_id   integer     not null primary key,
    status_name varchar(32) not null,
    update_ts   timestamp   not null
);