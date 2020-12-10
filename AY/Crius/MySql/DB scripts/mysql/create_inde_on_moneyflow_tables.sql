create unique index unique_moneyflow_1990_1994_tscode_date on moneyflow_1990_1994 (ts_code,trade_date);
create index ind_moneyflow_1990_1994_tscode on moneyflow_1990_1994(ts_code);
create index ind_moneyflow_1990_1994_trade_date on moneyflow_1990_1994(trade_date);

create unique index unique_moneyflow_1995_1999_tscode_date on moneyflow_1995_1999 (ts_code,trade_date);
create index ind_moneyflow_1995_1999_tscode on moneyflow_1995_1999(ts_code);
create index ind_moneyflow_1995_1999_trade_date on moneyflow_1995_1999(trade_date);

create unique index unique_moneyflow_2000_2004_tscode_date on moneyflow_2000_2004 (ts_code,trade_date);
create index ind_moneyflow_2000_2004_tscode on moneyflow_2000_2004(ts_code);
create index ind_moneyflow_2000_2004_trade_date on moneyflow_2000_2004(trade_date);

create unique index unique_moneyflow_2005_2009_tscode_date on moneyflow_2005_2009 (ts_code,trade_date);
create index ind_moneyflow_2005_2009_tscode on moneyflow_2005_2009(ts_code);
create index ind_moneyflow_2005_2009_trade_date on moneyflow_2005_2009(trade_date);

create unique index unique_moneyflow_2010_2014_tscode_date on moneyflow_2010_2014 (ts_code,trade_date);
create index ind_moneyflow_2010_2014_tscode on moneyflow_2010_2014(ts_code);
create index ind_moneyflow_2010_2014_trade_date on moneyflow_2010_2014(trade_date);

create unique index unique_moneyflow_2015_2019_tscode_date on moneyflow_2015_2019 (ts_code,trade_date);
create index ind_moneyflow_2015_2019_tscode on moneyflow_2015_2019(ts_code);
create index ind_moneyflow_2015_2019_trade_date on moneyflow_2015_2019(trade_date);

create unique index unique_moneyflow_2020_2024_tscode_date on moneyflow_2020_2024 (ts_code,trade_date);
create index ind_moneyflow_2020_2024_tscode on moneyflow_2020_2024(ts_code);
create index ind_moneyflow_2020_2024_trade_date on moneyflow_2020_2024(trade_date);

alter table moneyflow_1990_1994 add column trade_date_int int;
update moneyflow_1990_1994 set trade_date_int = cast(trade_date as unsigned);
create index ind_moneyflow_1990_1994_trade_date_int on moneyflow_1990_1994(trade_date_int);

alter table moneyflow_1995_1999 add column trade_date_int int;
update moneyflow_1995_1999 set trade_date_int = cast(trade_date as unsigned);
create index ind_moneyflow_1995_1999_trade_date_int on moneyflow_1995_1999(trade_date_int);

alter table moneyflow_2000_2004 add column trade_date_int int;
update moneyflow_2000_2004 set trade_date_int = cast(trade_date as unsigned);
create index ind_moneyflow_2000_2004_trade_date_int on moneyflow_2000_2004(trade_date_int);

alter table moneyflow_2005_2009 add column trade_date_int int;
update moneyflow_2005_2009 set trade_date_int = cast(trade_date as unsigned);
create index ind_moneyflow_2005_2009_trade_date_int on moneyflow_2005_2009(trade_date_int);

alter table moneyflow_2010_2014 add column trade_date_int int;
update moneyflow_2010_2014 set trade_date_int = cast(trade_date as unsigned);
create index ind_moneyflow_2010_2014_trade_date_int on moneyflow_2010_2014(trade_date_int);

alter table moneyflow_2015_2019 add column trade_date_int int;
update moneyflow_2015_2019 set trade_date_int = cast(trade_date as unsigned);
create index ind_moneyflow_2015_2019_trade_date_int on moneyflow_2015_2019(trade_date_int);

alter table moneyflow_2020_2024 add column trade_date_int int;
update moneyflow_2020_2024 set trade_date_int = cast(trade_date as unsigned);
create index ind_moneyflow_2020_2024_trade_date_int on moneyflow_2020_2024(trade_date_int);