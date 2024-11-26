create table filters
(
    id         serial     constraint filters_pk primary key,
    name  varchar(255) NOT NULL,
    action VARCHAR(255) NOT NULL,
    message jsonb,
    created_at timestamp with time zone default CURRENT_TIMESTAMP not null,
    updated_at timestamp with time zone default CURRENT_TIMESTAMP not null
);

insert into filters (name, action)
values  ('Example Filter', 'process'),
        ('Example Filter 2', 'process');