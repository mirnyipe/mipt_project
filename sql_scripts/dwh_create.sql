create schema if not exists dwh;

create table if not exists dwh.dwh_dim_clients (
    client_id varchar(128),
    last_name varchar(128),
    first_name varchar(128),
    patronymic varchar(128),
    date_of_birth date,
    passport_num varchar(128),
    passport_valid_to date,
    phone varchar(128),
    effective_from timestamp default now(),
    effective_to timestamp default timestamp '5999-12-31 23:59:59',
    deleted_flg char(1) default 'N'
);

create table if not exists dwh.dwh_dim_accounts (
    account varchar(128),
    valid_to date,
    client varchar(128),
    effective_from timestamp default now(),
    effective_to timestamp default timestamp '5999-12-31 23:59:59',
    deleted_flg char(1) default 'N'
);

create table if not exists dwh.dwh_dim_cards (
    card_num varchar(128),
    account varchar(128),
    effective_from timestamp default now(),
    effective_to timestamp default timestamp '5999-12-31 23:59:59',
    deleted_flg char(1) default 'N'
);



