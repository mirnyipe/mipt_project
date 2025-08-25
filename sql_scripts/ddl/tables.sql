-- STAGING
create table if not exists stg.stg_transactions(
    trans_id    varchar(128),
    trans_date  timestamp,
    card_num    varchar(64),
    oper_type   varchar(64),
    amt         numeric(18,2),
    oper_result varchar(32),
    terminal    varchar(64),
    file_dt     date,
    filename    varchar(256)
);

create table if not exists stg.stg_terminals(
    terminal_id      varchar(64),
    terminal_type    varchar(64),
    terminal_city    varchar(128),
    terminal_address varchar(256),
    file_dt          date,
    filename         varchar(256)
);

create table if not exists stg.stg_passport_blacklist(
    passport_num varchar(32),
    entry_dt     date,
    file_dt      date,
    filename     varchar(256)
);

-- DWH: SCD1
create table if not exists dwh.dwh_dim_cards(
    card_num    varchar(64) primary key,
    account_num varchar(128) not null,
    create_dt   timestamp default current_timestamp,
    update_dt   timestamp
);

create table if not exists dwh.dwh_dim_accounts(
    account_num varchar(128) primary key,
    valid_to    date,
    client      varchar(128),
    create_dt   timestamp default current_timestamp,
    update_dt   timestamp
);

create table if not exists dwh.dwh_dim_clients(
    client_id         varchar(128) primary key,
    last_name         varchar(128),
    first_name        varchar(128),
    patronymic        varchar(128),
    date_of_birth     date,
    passport_num      varchar(32),
    passport_valid_to date,
    phone             varchar(64),
    create_dt         timestamp default current_timestamp,
    update_dt         timestamp
);

-- DWH: SCD2 (терминалы)
create table if not exists dwh.dwh_dim_terminals_hist(
    terminal_id      varchar(64),
    terminal_type    varchar(64),
    terminal_city    varchar(128),
    terminal_address varchar(256),
    effective_from   timestamp not null,
    effective_to     timestamp not null default timestamp '5999-12-31 23:59:59',
    deleted_flg      integer not null default 0,
    primary key (terminal_id, effective_from)
);

-- DWH: FACTS
create table if not exists dwh.dwh_fact_transactions(
    trans_id    varchar(128) primary key,
    trans_date  timestamp,
    card_num    varchar(64),
    oper_type   varchar(64),
    amt         numeric(18,2),
    oper_result varchar(32),
    terminal    varchar(64)
);

create table if not exists dwh.dwh_fact_passport_blacklist(
    passport_num varchar(32),
    entry_dt     date,
    primary key (passport_num, entry_dt)
);

-- META
create table if not exists meta.meta_load_files(
    source_name  varchar(64),
    file_dt      date,
    filename     varchar(256),
    processed_at timestamp default current_timestamp,
    status       varchar(32) default 'DONE',
    primary key (source_name, filename)
);

create table if not exists meta.meta_last_dates(
    source_name  varchar(64) primary key,
    last_dt      date
);

-- REPORT
create table if not exists rep.rep_fraud(
    event_dt   timestamp,
    passport   varchar(32),
    fio        varchar(384),
    phone      varchar(64),
    event_type varchar(256),
    report_dt  timestamp
);
