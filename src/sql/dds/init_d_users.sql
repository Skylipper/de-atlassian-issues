create table if not exists dds.d_users
(
    user_id   serial       not null primary key,
    user_key  varchar(32)  not null unique,
    user_name varchar(128) not null,
    update_ts timestamp    not null
);