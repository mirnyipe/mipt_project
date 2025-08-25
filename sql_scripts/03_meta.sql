create table if not exists meta.META_LOAD_FILES(
    source_name  varchar(64),       -- 'transactions' | 'terminals' | 'passport_blacklist'
    file_dt      date,
    filename     varchar(256),
    processed_at timestamp default current_timestamp,
    status       varchar(32) default 'DONE',
    primary key (source_name, filename)
);

-- для удобства — хранить последнюю обработанную дату по типу источника
create table if not exists meta.META_LAST_DATES(
    source_name  varchar(64) primary key,
    last_dt      date
);
