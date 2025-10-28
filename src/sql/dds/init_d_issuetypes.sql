create table if not exists dds.d_issuetypes
(
    issuetype_id   integer      not null primary key,
    issuetype_name varchar(128) not null,
    update_ts      timestamp    not null
);