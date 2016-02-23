REGISTER udftest-1.jar;

DEFINE WDFM  com.ixat.pigudf.udftest.WeekDayFromMillis;

clickstreamData = load 'clicks.csv' USING PigStorage(',') as (userid:int, timestamp:chararray,url:chararray);
clickstreamData_g = group clickstreamData by WDFM(timestamp);
clickstreamData_gc = foreach clickstreamData_g generate group as Day, COUNT(clickstreamData) as NUMVIEWS;
dump clickstreamData_gc;
