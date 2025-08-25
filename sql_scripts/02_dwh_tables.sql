-- =========================
-- STAGING
-- =========================
create table if not exists stg.STG_TRANSACTIONS(
    trans_id      varchar(128),
    trans_date    timestamp,
    card_num      varchar(64),
    oper_type     varchar(64),
    amt           numeric(18,2),
    oper_result   varchar(32),
    terminal      varchar(64),
    file_dt       date,
    filename      varchar(256)
);

create table if not exists stg.STG_TERMINALS(
    terminal_id       varchar(64),
    terminal_type     varchar(64),
    terminal_city     varchar(128),
    terminal_address  varchar(256),
    file_dt           date,
    filename          varchar(256)
);

create table if not exists stg.STG_PASSPORT_BLACKLIST(
    passport_num  varchar(32),
    entry_dt      date,
    file_dt       date,
    filename      varchar(256)
);

-- =========================
-- DWH: DIM SCD1
-- =========================
create table if not exists dwh.DWH_DIM_CARDS(
    card_num     varchar(64) primary key,
    account_num  varchar(32) not null,
    create_dt    timestamp default current_timestamp,
    update_dt    timestamp
);

create table if not exists dwh.DWH_DIM_ACCOUNTS(
    account_num  varchar(32) primary key,
    valid_to     date,
    client       varchar(64),
    create_dt    timestamp default current_timestamp,
    update_dt    timestamp
);

create table if not exists dwh.DWH_DIM_CLIENTS(
    client_id          varchar(64) primary key,
    last_name          varchar(128),
    first_name         varchar(128),
    patronymic         varchar(128),
    date_of_birth      date,
    passport_num       varchar(32),
    passport_valid_to  date,
    phone              varchar(64),
    create_dt          timestamp default current_timestamp,
    update_dt          timestamp
);

-- =========================
-- DWH: DIM SCD2 (TERMINALS)
-- =========================
create table if not exists dwh.DWH_DIM_TERMINALS_HIST(
    terminal_id       varchar(64),
    terminal_type     varchar(64),
    terminal_city     varchar(128),
    terminal_address  varchar(256),
    effective_from    timestamp not null,
    effective_to      timestamp not null default timestamp '5999-12-31 23:59:59',
    deleted_flg       integer not null default 0,
    primary key (terminal_id, effective_from)
);

-- =========================
-- DWH: FACTS
-- =========================
create table if not exists dwh.DWH_FACT_TRANSACTIONS(
    trans_id      varchar(128) primary key,
    trans_date    timestamp,
    card_num      varchar(64),
    oper_type     varchar(64),
    amt           numeric(18,2),
    oper_result   varchar(32),
    terminal      varchar(64)
);

create table if not exists dwh.DWH_FACT_PASSPORT_BLACKLIST(
    passport_num  varchar(32),
    entry_dt      date,
    primary key (passport_num, entry_dt)
);

-- =========================
-- REPORT
-- =========================
create table if not exists rep.REP_FRAUD(
    event_dt   timestamp,
    passport   varchar(32),
    fio        varchar(384),
    phone      varchar(64),
    event_type varchar(256),
    report_dt  timestamp
);
