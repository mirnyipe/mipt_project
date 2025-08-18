create table if not exists stg.transactions (
    transaction_id bigint,
    transaction_date timestamp,
    amount numeric(15,2),
    card_num varchar(19),
    oper_type varchar(20),
    oper_result varchar(20),
    terminal varchar(20)
);

create table if not exists stg.terminals (
    terminal_id varchar(10),
    terminal_type varchar(10),
    terminal_city varchar(50),
    terminal_address varchar(512)
);

create table if not exists stg.passport_blacklist (
    date date,
    passport varchar(20)
);

