CREATE TABLE messages
(
    id         serial constraint messages_pk primary key,
    content    jsonb,
    status     varchar(10)              default 'pending'::character varying,
    created_at timestamp with time zone default CURRENT_TIMESTAMP not null,
    updated_at timestamp with time zone default CURRENT_TIMESTAMP not null,
    filter_applied int,
    message  jsonb

);


