---Copy stocks.csv over to /user/cloudera/stocks
---Create a table

-- create table stocks (
-- 	stock_name string,
-- 	trade_date string,
-- 	hi double,
-- 	lo double,
-- 	open double,
-- 	close double,
-- 	traded_volume bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ;


---Load Data
--LOAD DATA  INPATH '/user/cloudera/stocks/stocks.csv' OVERWRITE INTO TABLE stocks;

--------some analytics
--select stock_name, sum(traded_volume) from stocks group by stock_name

------------Create a Partition table
-- create table stocks_p (
-- trade_date string, hi double, lo double, open double, close double, traded_volume bigint)
-- PARTITIONED BY (stock_name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
-- STORED AS ORCFILE;

----------Load data into partitioned table
--INSERT INTO TABLE stocks_p PARTITION (stock_name='MSFT')  select trade_date, hi, lo, open, close, traded_volume from stocks where stock_name = 'MSFT';
--INSERT INTO TABLE stocks_p PARTITION (stock_name='APPL')  select trade_date, hi, lo, open, close, traded_volume from stocks where stock_name = 'APPL';
--INSERT INTO TABLE stocks_p PARTITION (stock_name='YHOO')  select trade_date, hi, lo, open, close, traded_volume from stocks where stock_name = 'YHOO';
--------some analytics
--select stock_name, sum(traded_volume) from stocks_p group by stock_name

