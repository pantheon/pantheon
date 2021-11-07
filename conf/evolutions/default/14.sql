# --- !Ups

drop table if exists principal_permissions;
drop table if exists role_actions;
drop table if exists roles;

create table if not exists roles(
    role_id uuid primary key,
    resource_type varchar not null,
    name varchar not null,
    unique(resource_type, name)
);

create table if not exists role_actions(
    role_id uuid not null references roles(role_id),
    action_type varchar not null,
    primary key (role_id, action_type)
);

create table principal_permissions(
    permission_id uuid primary key,
    principal_id uuid not null,
    role_id uuid not null references roles(role_id),
    resource_id uuid not null,
    unique(principal_id, role_id, resource_id)
);

with r as (
insert into roles values
    (uuid_generate_v4(), 'Catalog', 'Owner'),
    (uuid_generate_v4(), 'Schema', 'Owner'),
    (uuid_generate_v4(), 'DataSource', 'Owner'),
    (uuid_generate_v4(), 'SavedQuery', 'Owner'),
    (uuid_generate_v4(), 'JdbcProduct', 'Owner')
returning role_id
)
insert into role_actions
  select *, 'Read' from r union
  select *, 'Edit' from r union
  select *, 'Delete' from r union
  select *, 'Grant' from r;

with r as (
insert into roles values
    (uuid_generate_v4(), 'Catalog', 'Editor'),
    (uuid_generate_v4(), 'Schema', 'Editor'),
    (uuid_generate_v4(), 'DataSource', 'Editor'),
    (uuid_generate_v4(), 'SavedQuery', 'Editor'),
    (uuid_generate_v4(), 'JdbcProduct', 'Editor')
returning role_id
)
insert into role_actions
  select *, 'Read' from r union
  select *, 'Edit' from r union
  select *, 'Delete' from r;

with r as (
insert into roles values
    (uuid_generate_v4(), 'Catalog', 'Reader'),
    (uuid_generate_v4(), 'Schema', 'Reader'),
    (uuid_generate_v4(), 'DataSource', 'Reader'),
    (uuid_generate_v4(), 'SavedQuery', 'Reader'),
    (uuid_generate_v4(), 'JdbcProduct', 'Reader')
returning role_id
)
insert into role_actions
  select *, 'Read' from r;

# --- !Downs

drop table if exists principal_permissions;
drop table if exists role_actions;
drop table if exists roles;
