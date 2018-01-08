-- schema for polo script

-- drop tables first if exists
drop table if exists polo_order_book;
drop table if exists polo_trades;

create table polo_order_book (
    server_datetime TEXT,
    product_id TEXT,
    bids_1 TEXT,
    bids_2 TEXT,
    bids_3 TEXT,
    bids_4 TEXT,
    bids_5 TEXT,
    bids_6 TEXT,
    bids_7 TEXT,
    bids_8 TEXT,
    bids_9 TEXT,
    bids_10 TEXT,
    bids_11 TEXT,
    bids_12 TEXT,
    bids_13 TEXT,
    bids_14 TEXT,
    bids_15 TEXT,
    asks_1 TEXT,
    asks_2 TEXT,
    asks_3 TEXT,
    asks_4 TEXT,
    asks_5 TEXT,
    asks_6 TEXT,
    asks_7 TEXT,
    asks_8 TEXT,
    asks_9 TEXT,
    asks_10 TEXT,
    asks_11 TEXT,
    asks_12 TEXT,
    asks_13 TEXT,
    asks_14 TEXT,
    asks_15 TEXT
);

create table polo_trades (
    server_datetime TEXT,
    exchange_datetime TEXT,
    sequence TEXT,
    trade_id TEXT,
    product_id TEXT,
    price TEXT,
    volume TEXT,
    side TEXT,
    backfilled TEXT
);